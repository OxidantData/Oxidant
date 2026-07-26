//! `sts:AssumeRole` credential provider for S3 access — the Hadoop-AWS `fs.s3a.assumed.role.arn`
//! equivalent (Spark's `AssumedRoleCredentialProvider`), resolved once per bucket at first S3
//! access and kept fresh for the cluster's lifetime.
//!
//! `object_store`'s own `AmazonS3Builder::from_env()` already resolves the pod's own IRSA
//! identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`) internally, but it has no notion of
//! assuming a SECOND, different role on top of that base identity — `fs.s3a.assumed.role.arn`
//! names exactly that second role. `object_store::aws::AmazonS3Builder::with_credentials(...)` is
//! the public extension point this plugs into (see [`crate::catalog_bridge::ensure_remote_store`]).
//!
//! STS sessions default to a 1-hour expiry, so a long-running cluster needs the credential
//! refreshed transparently rather than assumed once at startup and left to expire mid-query —
//! [`AssumeRoleCredentialProvider::get_credential`] re-assumes the role whenever the cached
//! credential is within [`REFRESH_MARGIN`] of expiring.

use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use object_store::aws::AwsCredential;
use object_store::{CredentialProvider, Error as StoreError, Result as StoreResult};
use tokio::sync::Mutex as AsyncMutex;

/// Re-assume the role once the cached credential is within this margin of its actual STS
/// expiration, so an in-flight request never races a credential that expires mid-request.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

/// Config-key names this module recognizes in a table's `storage_options` (see
/// `weft_catalog::TableMetadata::storage_options`) — deliberately the real Hadoop-AWS key names,
/// not a weft-invented equivalent, so a config ported from Spark/Hadoop-AWS just works.
pub const ASSUMED_ROLE_ARN_KEY: &str = "fs.s3a.assumed.role.arn";
pub const ASSUMED_ROLE_SESSION_NAME_KEY: &str = "fs.s3a.assumed.role.session.name";

/// STS `AssumeRole`-backed [`CredentialProvider`], caching the resulting temporary credential and
/// transparently refreshing it before it expires. Uses the process's own default AWS credential
/// chain (in a cluster pod, that's IRSA — the same identity `AmazonS3Builder::from_env()` would
/// otherwise use directly) as the caller identity for the `AssumeRole` call itself.
#[derive(Debug)]
pub struct AssumeRoleCredentialProvider {
    role_arn: String,
    session_name: String,
    region: String,
    cached: RwLock<Option<(Arc<AwsCredential>, SystemTime)>>,
    /// Serializes the actual `sts:AssumeRole` call so N concurrent cache-miss callers produce ONE
    /// STS request, not N — held across the `.await` (hence `tokio::sync::Mutex`, not
    /// `std::sync::Mutex`, which must never be held across an await point). Without this, a burst
    /// of concurrent queries hitting the cache miss/refresh-margin window at once each fire their
    /// own AssumeRole call, risking STS per-caller-identity throttling under load.
    refresh_lock: AsyncMutex<()>,
}

impl AssumeRoleCredentialProvider {
    pub fn new(role_arn: String, session_name: Option<String>, region: String) -> Self {
        Self {
            role_arn,
            session_name: session_name.unwrap_or_else(|| "weft-cluster".to_string()),
            region,
            cached: RwLock::new(None),
            refresh_lock: AsyncMutex::new(()),
        }
    }

    /// A cached credential, if one exists and isn't within `REFRESH_MARGIN` of expiring. Never
    /// holds the lock across an `.await` — read it, decide, drop it, all synchronously.
    fn fresh_cached(&self) -> Option<Arc<AwsCredential>> {
        let guard = self
            .cached
            .read()
            .expect("assume-role credential cache poisoned");
        let (cred, expiry) = guard.as_ref()?;
        (SystemTime::now() + REFRESH_MARGIN < *expiry).then(|| cred.clone())
    }

    async fn refresh(
        &self,
    ) -> Result<Arc<AwsCredential>, Box<dyn std::error::Error + Send + Sync>> {
        // Serialize the actual STS call: only the caller that acquires this lock first proceeds
        // to AssumeRole; everyone else queues here, then finds a fresh cache entry (written by
        // the lock-holder below) and returns immediately without ever calling STS themselves.
        let _guard = self.refresh_lock.lock().await;
        if let Some(cred) = self.fresh_cached() {
            return Ok(cred);
        }

        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_sts::config::Region::new(self.region.clone()))
            .load()
            .await;
        let client = aws_sdk_sts::Client::new(&sdk_config);
        let resp = client
            .assume_role()
            .role_arn(&self.role_arn)
            .role_session_name(&self.session_name)
            .send()
            .await?;
        let sts_creds = resp
            .credentials
            .ok_or("AssumeRole response carried no credentials")?;
        let expiry: SystemTime = sts_creds.expiration.try_into()?;
        let cred = Arc::new(AwsCredential {
            key_id: sts_creds.access_key_id,
            secret_key: sts_creds.secret_access_key,
            token: Some(sts_creds.session_token),
        });

        *self
            .cached
            .write()
            .expect("assume-role credential cache poisoned") = Some((cred.clone(), expiry));
        Ok(cred)
    }
}

#[async_trait]
impl CredentialProvider for AssumeRoleCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> StoreResult<Arc<AwsCredential>> {
        if let Some(cred) = self.fresh_cached() {
            return Ok(cred);
        }
        self.refresh().await.map_err(|source| StoreError::Generic {
            store: "S3",
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_cached_is_none_before_first_refresh() {
        let p = AssumeRoleCredentialProvider::new(
            "arn:aws:iam::123456789012:role/weft-poolctl/analytics".to_string(),
            None,
            "us-west-2".to_string(),
        );
        assert!(p.fresh_cached().is_none());
    }

    #[test]
    fn fresh_cached_returns_credential_before_expiry() {
        let p = AssumeRoleCredentialProvider::new(
            "arn:aws:iam::123456789012:role/weft-poolctl/analytics".to_string(),
            Some("custom-session".to_string()),
            "us-west-2".to_string(),
        );
        let cred = Arc::new(AwsCredential {
            key_id: "AKIA...".to_string(),
            secret_key: "secret".to_string(),
            token: Some("token".to_string()),
        });
        *p.cached.write().unwrap() = Some((cred, SystemTime::now() + Duration::from_secs(3600)));
        assert!(p.fresh_cached().is_some());
        assert_eq!(p.session_name, "custom-session");
    }

    #[test]
    fn fresh_cached_is_none_within_refresh_margin_of_expiry() {
        let p = AssumeRoleCredentialProvider::new(
            "arn:aws:iam::123456789012:role/weft-poolctl/analytics".to_string(),
            None,
            "us-west-2".to_string(),
        );
        let cred = Arc::new(AwsCredential {
            key_id: "AKIA...".to_string(),
            secret_key: "secret".to_string(),
            token: None,
        });
        // Expires in 1 minute — well within the 5-minute REFRESH_MARGIN, so this must NOT be
        // treated as fresh (an in-flight request must never race a credential this close to
        // expiring).
        *p.cached.write().unwrap() = Some((cred, SystemTime::now() + Duration::from_secs(60)));
        assert!(p.fresh_cached().is_none());
    }

    #[test]
    fn default_session_name_is_stable() {
        let p = AssumeRoleCredentialProvider::new(
            "arn:aws:iam::123456789012:role/weft-poolctl/analytics".to_string(),
            None,
            "us-west-2".to_string(),
        );
        assert_eq!(p.session_name, "weft-cluster");
    }
}
