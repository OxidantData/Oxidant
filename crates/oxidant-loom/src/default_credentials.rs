//! S3 credentials from the **full AWS default chain**, including `~/.aws/credentials`.
//!
//! `object_store`'s `AmazonS3Builder::from_env()` resolves credentials from environment variables,
//! IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE`), the ECS container endpoint, and EC2 instance metadata.
//! That is the right set for how Oxidant runs in production — and it silently excludes the one
//! source a developer actually has: a named profile in `~/.aws/credentials` or `~/.aws/config`.
//!
//! The failure is not a clear "no credentials". With no env vars set, the S3 client falls through
//! to the instance-metadata endpoint, which on a laptop is an unroutable link-local address, and
//! the request dies after the full retry budget:
//!
//! ```text
//! Generic S3 error: Error performing PUT http://169.254.169.254/latest/api/token
//!   in 2.013255041s, after 10 retries
//! ```
//!
//! …which reads as a network fault rather than "you are not authenticated". Meanwhile `aws s3 ls`
//! against the same bucket works, because the CLI reads the profile.
//!
//! This provider closes that gap by resolving through `aws-config`'s [`DefaultCredentialsChain`],
//! a **superset** of what `from_env` covers. The chain's own order decides precedence, so nothing
//! that works today changes rank:
//!
//! 1. environment variables
//! 2. web identity token (IRSA)
//! 3. the shared profile — `~/.aws/credentials`, `~/.aws/config`, including SSO,
//!    `credential_process`, and `source_profile` chains  ← the part that was missing
//! 4. the ECS container credential endpoint
//! 5. EC2 instance metadata (IMDS)
//!
//! Static keys supplied explicitly in a table's `storage_options`, and `fs.s3a.assumed.role.arn`,
//! both outrank this entirely — neither reaches this provider (see
//! [`crate::catalog_bridge::ensure_remote_store`]).

use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use aws_sdk_sts::config::ProvideCredentials;
use object_store::aws::AwsCredential;
use object_store::{CredentialProvider, Error as StoreError, Result as StoreResult};
use tokio::sync::Mutex as AsyncMutex;

/// Re-resolve once the cached credential is within this margin of expiring, so an in-flight
/// request never races a credential that expires mid-request.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

/// How long to hold a credential that carries **no** expiry of its own.
///
/// Static profile keys never expire, so this is not about correctness — it is about picking up a
/// rotated `~/.aws/credentials` without a restart. Re-reading a local file hourly costs nothing;
/// caching forever means a rotation cannot take effect while the process lives.
const STATIC_TTL: Duration = Duration::from_secs(60 * 60);

/// [`CredentialProvider`] backed by the AWS default credential chain.
#[derive(Debug)]
pub struct DefaultChainCredentialProvider {
    region: String,
    cached: RwLock<Option<(Arc<AwsCredential>, SystemTime)>>,
    /// Serializes resolution so N concurrent cache-miss callers do one lookup, not N — held across
    /// an `.await`, hence the async mutex. Without it, a burst of queries starting together each
    /// walk the chain (and each hit IMDS, which is rate-limited).
    refresh_lock: AsyncMutex<()>,
}

impl DefaultChainCredentialProvider {
    pub fn new(region: String) -> Self {
        Self {
            region,
            cached: RwLock::new(None),
            refresh_lock: AsyncMutex::new(()),
        }
    }

    /// A cached credential, if one exists and is not within `REFRESH_MARGIN` of expiring. Never
    /// holds the lock across an `.await` — read, decide, drop, all synchronously.
    fn fresh_cached(&self) -> Option<Arc<AwsCredential>> {
        let guard = self
            .cached
            .read()
            .expect("default-chain credential cache poisoned");
        let (cred, expiry) = guard.as_ref()?;
        (SystemTime::now() + REFRESH_MARGIN < *expiry).then(|| cred.clone())
    }

    async fn refresh(
        &self,
    ) -> Result<Arc<AwsCredential>, Box<dyn std::error::Error + Send + Sync>> {
        let _guard = self.refresh_lock.lock().await;
        if let Some(cred) = self.fresh_cached() {
            return Ok(cred);
        }

        let chain = aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
            .region(aws_config::Region::new(self.region.clone()))
            .build()
            .await;
        let resolved = chain.provide_credentials().await?;

        let expiry = resolved
            .expiry()
            .unwrap_or_else(|| SystemTime::now() + STATIC_TTL);
        let cred = Arc::new(AwsCredential {
            key_id: resolved.access_key_id().to_string(),
            secret_key: resolved.secret_access_key().to_string(),
            token: resolved.session_token().map(str::to_string),
        });

        *self
            .cached
            .write()
            .expect("default-chain credential cache poisoned") = Some((cred.clone(), expiry));
        Ok(cred)
    }
}

#[async_trait]
impl CredentialProvider for DefaultChainCredentialProvider {
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

    fn credential(token: Option<&str>) -> Arc<AwsCredential> {
        Arc::new(AwsCredential {
            key_id: "AKIAEXAMPLE".to_string(),
            secret_key: "secret".to_string(),
            token: token.map(str::to_string),
        })
    }

    #[test]
    fn nothing_is_cached_before_the_first_resolution() {
        let p = DefaultChainCredentialProvider::new("us-west-2".to_string());
        assert!(p.fresh_cached().is_none());
    }

    #[test]
    fn a_credential_is_reused_until_it_nears_expiry() {
        let p = DefaultChainCredentialProvider::new("us-west-2".to_string());
        *p.cached.write().unwrap() = Some((
            credential(Some("t")),
            SystemTime::now() + Duration::from_secs(3600),
        ));
        assert!(p.fresh_cached().is_some());
    }

    /// The window this rules out: a credential handed to a request that outlives it. Anything
    /// inside `REFRESH_MARGIN` of expiry is treated as already stale.
    #[test]
    fn a_credential_within_the_refresh_margin_is_not_reused() {
        let p = DefaultChainCredentialProvider::new("us-west-2".to_string());
        *p.cached.write().unwrap() = Some((
            credential(None),
            SystemTime::now() + Duration::from_secs(60),
        ));
        assert!(p.fresh_cached().is_none());
    }

    #[test]
    fn an_expired_credential_is_not_reused() {
        let p = DefaultChainCredentialProvider::new("us-west-2".to_string());
        *p.cached.write().unwrap() =
            Some((credential(None), SystemTime::now() - Duration::from_secs(1)));
        assert!(p.fresh_cached().is_none());
    }

    // ---- Resolution through the real chain -------------------------------------------------
    //
    // The cache tests above prove nothing about the thing this module exists for. These call
    // `get_credential()` for real, with the credential sources swapped out underneath.
    //
    // `aws-config`'s in-memory `Env`/`Fs` injection is `pub(crate)`, so the sources are steered
    // with the same environment variables the AWS CLI honours. That makes these tests mutate
    // process-global state, hence the lock — and it also means they exercise exactly the
    // resolution path a real deployment uses, rather than a test-only seam.

    /// Serializes the env-var swapping below. An async mutex because the guard is held across the
    /// `get_credential().await` it is protecting — a `std` guard there both trips
    /// `clippy::await_holding_lock` and can block the runtime. Tokio's has no poisoning, so a
    /// failing test does not cascade into the others.
    static ENV_LOCK: std::sync::OnceLock<AsyncMutex<()>> = std::sync::OnceLock::new();

    fn env_lock() -> &'static AsyncMutex<()> {
        ENV_LOCK.get_or_init(|| AsyncMutex::new(()))
    }

    struct AwsEnv {
        _dir: tempfile::TempDir,
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl AwsEnv {
        /// Point the credential chain at a throwaway shared-credentials file, and clear every
        /// variable that would otherwise short-circuit ahead of it.
        fn with_profile(profile: &str, body: &str) -> Self {
            let dir = tempfile::TempDir::new().expect("temp dir");
            let path = dir.path().join("credentials");
            std::fs::write(&path, body).expect("write credentials file");

            let keys = [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_PROFILE",
                "AWS_SHARED_CREDENTIALS_FILE",
                "AWS_CONFIG_FILE",
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                "AWS_ROLE_ARN",
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            ];
            let saved = keys
                .iter()
                .map(|k| (*k, std::env::var(k).ok()))
                .collect::<Vec<_>>();
            for (k, _) in &saved {
                std::env::remove_var(k);
            }
            std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &path);
            std::env::set_var("AWS_PROFILE", profile);
            // An empty config file keeps the chain off the developer's real ~/.aws/config.
            let config = dir.path().join("config");
            std::fs::write(&config, "").expect("write config file");
            std::env::set_var("AWS_CONFIG_FILE", &config);

            Self { _dir: dir, saved }
        }

        fn set(&self, key: &str, value: &str) {
            std::env::set_var(key, value);
        }
    }

    impl Drop for AwsEnv {
        fn drop(&mut self) {
            for (key, previous) in &self.saved {
                match previous {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    /// The regression this whole module exists for: credentials that live **only** in
    /// `~/.aws/credentials` must resolve. Before this, they did not — the request fell through to
    /// the instance-metadata endpoint and failed there looking like a network fault.
    #[tokio::test]
    async fn a_profile_in_the_shared_credentials_file_is_resolved() {
        let _guard = env_lock().lock().await;
        let _env = AwsEnv::with_profile(
            "oxidant-test",
            "[oxidant-test]\n\
             aws_access_key_id = AKIAPROFILEKEY\n\
             aws_secret_access_key = profile-secret\n",
        );

        let provider = DefaultChainCredentialProvider::new("us-west-2".to_string());
        let cred = provider.get_credential().await.expect("resolve profile");
        assert_eq!(cred.key_id, "AKIAPROFILEKEY");
        assert_eq!(cred.secret_key, "profile-secret");
        assert_eq!(cred.token, None);
    }

    /// A session token in the profile has to survive to the signer; dropping it turns a valid
    /// temporary credential into a 403 that reads like a permissions problem.
    #[tokio::test]
    async fn a_profile_session_token_is_carried_through() {
        let _guard = env_lock().lock().await;
        let _env = AwsEnv::with_profile(
            "oxidant-temp",
            "[oxidant-temp]\n\
             aws_access_key_id = AKIATEMP\n\
             aws_secret_access_key = temp-secret\n\
             aws_session_token = temp-token\n",
        );

        let cred = DefaultChainCredentialProvider::new("us-west-2".to_string())
            .get_credential()
            .await
            .expect("resolve profile");
        assert_eq!(cred.token.as_deref(), Some("temp-token"));
    }

    /// Precedence, asserted rather than assumed. Every existing deployment sets credentials in the
    /// environment; if a stray profile on the host could outrank them, this change would silently
    /// re-identify production traffic.
    #[tokio::test]
    async fn environment_variables_still_outrank_a_profile() {
        let _guard = env_lock().lock().await;
        let env = AwsEnv::with_profile(
            "oxidant-test",
            "[oxidant-test]\n\
             aws_access_key_id = AKIAPROFILEKEY\n\
             aws_secret_access_key = profile-secret\n",
        );
        env.set("AWS_ACCESS_KEY_ID", "AKIAENVKEY");
        env.set("AWS_SECRET_ACCESS_KEY", "env-secret");

        let cred = DefaultChainCredentialProvider::new("us-west-2".to_string())
            .get_credential()
            .await
            .expect("resolve environment");
        assert_eq!(cred.key_id, "AKIAENVKEY", "the environment must win");
        assert_eq!(cred.secret_key, "env-secret");
    }

    /// The second call must be served from the cache, not by walking the chain again — otherwise
    /// every S3 request re-reads the profile (and, on an instance, re-hits rate-limited IMDS).
    #[tokio::test]
    async fn a_resolved_credential_is_cached_for_the_next_request() {
        let _guard = env_lock().lock().await;
        let _env = AwsEnv::with_profile(
            "oxidant-test",
            "[oxidant-test]\n\
             aws_access_key_id = AKIAFIRST\n\
             aws_secret_access_key = first-secret\n",
        );

        let provider = DefaultChainCredentialProvider::new("us-west-2".to_string());
        let first = provider.get_credential().await.expect("first resolve");
        assert_eq!(first.key_id, "AKIAFIRST");
        assert!(provider.fresh_cached().is_some(), "nothing was cached");

        // Rewrite the profile under it. A cached credential must be served regardless, which is
        // observable proof the chain was not consulted a second time.
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIASECOND");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "second-secret");
        let second = provider.get_credential().await.expect("second resolve");
        assert_eq!(second.key_id, "AKIAFIRST", "the credential was re-resolved");
    }
}
