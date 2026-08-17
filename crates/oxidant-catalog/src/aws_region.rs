//! The ambient AWS region, resolved the way the AWS CLI resolves it.
//!
//! Every AWS-facing part of Oxidant — the S3 object store, the Glue catalog, the Lake Formation
//! client — needs a region, and each used to end its own lookup at a hardcoded `us-west-2`. That
//! default is invisible when it is wrong: credentials resolve, the client is built, and the
//! request goes to the wrong regional endpoint. Depending on the call that surfaces as a bucket
//! that "does not exist", an empty catalog, or a redirect — never as "no region configured".
//!
//! [`ambient_region`] closes the gap between "authenticated" and "pointed at the right place":
//!
//! 1. `AWS_REGION`
//! 2. `AWS_DEFAULT_REGION`
//! 3. the shared profile's `region` (`~/.aws/config`, honouring `AWS_PROFILE`)
//! 4. EC2 instance metadata
//!
//! An explicitly configured region — a catalog's `region` option, a table's `s3.region` — is not
//! in this list on purpose. It outranks all of it, and is applied by the caller.

use std::sync::OnceLock;

/// Where the region lookup ends when nothing configures one.
///
/// Kept only so a deployment that has been relying on it does not change endpoint underneath a
/// running cluster. Reaching it means every source above came up empty, which is a
/// misconfiguration rather than a default worth having.
pub const LEGACY_FALLBACK_REGION: &str = "us-west-2";

static AMBIENT_REGION: OnceLock<Option<String>> = OnceLock::new();

/// The ambient region, or `None` when nothing configures one.
///
/// Resolved once per process and cached: the profile step reads a file and the IMDS step makes a
/// network call, and this is consulted every time a store or catalog client is built.
pub fn ambient_region() -> Option<String> {
    AMBIENT_REGION.get_or_init(resolve_ambient_region).clone()
}

/// The uncached resolution, so tests can exercise the chain more than once per process.
fn resolve_ambient_region() -> Option<String> {
    if let Some(region) = region_from_env() {
        return Some(region);
    }
    region_from_profile_or_imds()
}

/// The two environment variables, checked synchronously.
///
/// Worth doing before the async chain even though the chain checks them too: it is the
/// overwhelmingly common case, and it answers without standing up a runtime.
fn region_from_env() -> Option<String> {
    ["AWS_REGION", "AWS_DEFAULT_REGION"]
        .iter()
        .filter_map(|key| std::env::var(key).ok())
        .find(|value| !value.trim().is_empty())
}

/// The shared profile, then IMDS — via `aws-config`, on a thread of its own.
///
/// The chain is async and every caller here is sync. Blocking on the *current* runtime is not an
/// option: these callers run both inside a Tokio worker (where `block_on` panics) and in plain
/// sync code (where there is no runtime to borrow). A short-lived thread with its own
/// single-threaded runtime is correct in both, and it runs at most once per process.
///
/// Composed from the profile and IMDS providers rather than `DefaultRegionChain`, deliberately.
/// That chain leads with its own environment step, which treats an **empty** `AWS_REGION` as a
/// value and short-circuits — so `export AWS_REGION=$SOMETHING_UNSET`, an everyday shell accident,
/// would suppress a perfectly good profile and resolve to nothing. Environment handling lives in
/// [`region_from_env`] above, which treats empty as absent.
fn region_from_profile_or_imds() -> Option<String> {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .ok()?;
                runtime.block_on(async {
                    let chain = aws_config::meta::region::RegionProviderChain::first_try(
                        aws_config::profile::region::ProfileFileRegionProvider::builder().build(),
                    )
                    .or_else(aws_config::imds::region::ImdsRegionProvider::builder().build());
                    chain.region().await.map(|region| region.to_string())
                })
            })
            .join()
            .ok()
            .flatten()
    })
    .filter(|region| !region.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the environment swapping below — these tests mutate process-global state.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct RegionEnv {
        _dir: tempfile::TempDir,
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl RegionEnv {
        /// Clear every region source, then point the profile lookup at a throwaway config file.
        /// `config` is written verbatim as `~/.aws/config`.
        fn new(config: &str, profile: Option<&str>) -> Self {
            let dir = tempfile::TempDir::new().expect("temp dir");
            let path = dir.path().join("config");
            std::fs::write(&path, config).expect("write config");

            let keys = [
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
                "AWS_PROFILE",
                "AWS_CONFIG_FILE",
                "AWS_SHARED_CREDENTIALS_FILE",
                // Keep the IMDS step from reaching the real metadata endpoint if the chain gets
                // that far; an unreachable endpoint fails fast instead of hanging the test.
                "AWS_EC2_METADATA_DISABLED",
            ];
            let saved = keys
                .iter()
                .map(|k| (*k, std::env::var(k).ok()))
                .collect::<Vec<_>>();
            for (k, _) in &saved {
                std::env::remove_var(k);
            }
            std::env::set_var("AWS_CONFIG_FILE", &path);
            std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
            if let Some(profile) = profile {
                std::env::set_var("AWS_PROFILE", profile);
            }
            Self { _dir: dir, saved }
        }

        fn set(&self, key: &str, value: &str) {
            std::env::set_var(key, value);
        }
    }

    impl Drop for RegionEnv {
        fn drop(&mut self) {
            for (key, previous) in &self.saved {
                match previous {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    /// The regression: a developer whose region lives only in `~/.aws/config` used to silently get
    /// `us-west-2`, so an authenticated client talked to the wrong regional endpoint.
    #[test]
    fn the_region_comes_from_the_shared_profile_when_no_variable_is_set() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _env = RegionEnv::new("[default]\nregion = eu-west-1\n", None);
        assert_eq!(resolve_ambient_region().as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn a_named_profile_is_honoured() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _env = RegionEnv::new(
            "[default]\nregion = eu-west-1\n\n[profile frankfurt]\nregion = eu-central-1\n",
            Some("frankfurt"),
        );
        assert_eq!(resolve_ambient_region().as_deref(), Some("eu-central-1"));
    }

    /// Precedence, asserted rather than assumed — an existing deployment sets `AWS_REGION`, and a
    /// stray profile on the host must not move it.
    #[test]
    fn aws_region_outranks_the_profile() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let env = RegionEnv::new("[default]\nregion = eu-west-1\n", None);
        env.set("AWS_REGION", "ap-southeast-2");
        assert_eq!(resolve_ambient_region().as_deref(), Some("ap-southeast-2"));
    }

    #[test]
    fn aws_region_outranks_aws_default_region() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let env = RegionEnv::new("", None);
        env.set("AWS_DEFAULT_REGION", "us-east-2");
        env.set("AWS_REGION", "ap-southeast-2");
        assert_eq!(resolve_ambient_region().as_deref(), Some("ap-southeast-2"));
    }

    /// `AWS_DEFAULT_REGION` is the AWS CLI's other standard variable. The S3 path used to read
    /// only `AWS_REGION` and overwrite whatever else had been resolved with its hardcoded default.
    #[test]
    fn aws_default_region_is_used_when_aws_region_is_unset() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let env = RegionEnv::new("", None);
        env.set("AWS_DEFAULT_REGION", "us-east-2");
        assert_eq!(resolve_ambient_region().as_deref(), Some("us-east-2"));
    }

    /// An empty variable is not a region. Left as-is it would win over a perfectly good profile.
    #[test]
    fn an_empty_variable_does_not_shadow_the_profile() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let env = RegionEnv::new("[default]\nregion = eu-west-1\n", None);
        env.set("AWS_REGION", "");
        assert_eq!(resolve_ambient_region().as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn nothing_configured_resolves_to_nothing() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _env = RegionEnv::new("", None);
        assert_eq!(resolve_ambient_region(), None);
    }
}
