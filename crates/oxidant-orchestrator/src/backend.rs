//! Cluster backend trait and implementations.

use std::process::Command;

use oxidant_common::{Error, Result};
use oxidant_execution::membership::{ClusterMembership, StaticMembership};

use crate::spec::ClusterSpec;

/// Live cluster metadata returned after provision.
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub cluster_id: String,
    pub connect_endpoint: String,
    pub worker_endpoints: Vec<String>,
}

/// Provision and tear down per-user compute clusters.
pub trait ClusterBackend: Send + Sync {
    fn provision(&self, spec: &ClusterSpec) -> Result<ClusterInfo>;
    fn delete(&self, cluster_id: &str) -> Result<()>;
    fn worker_endpoints(&self, spec: &ClusterSpec) -> Result<Vec<String>>;

    /// Increase the desired worker count. Backends that do not have an incremental scale primitive
    /// can safely fall back to reprovisioning the desired state.
    fn scale_up(&self, spec: &ClusterSpec, desired_workers: u32) -> Result<ClusterInfo> {
        let mut next = spec.clone();
        next.worker_count = desired_workers.max(spec.worker_count);
        self.provision(&next)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkerBounds {
    pub(crate) desired: u32,
    pub(crate) min: u32,
    pub(crate) max: u32,
}

pub(crate) fn worker_bounds(spec: &ClusterSpec) -> WorkerBounds {
    let min = spec.min_workers.max(1);
    let max = spec.max_workers.max(min);
    let desired = spec.worker_count.max(min).min(max);
    WorkerBounds { desired, min, max }
}

/// Static worker list (local dev / CI).
pub struct StaticBackend {
    endpoints: Vec<String>,
}

impl StaticBackend {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self { endpoints }
    }

    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("OXIDANT_WORKERS").ok()?;
        let endpoints: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|ep| {
                if ep.starts_with("http://") || ep.starts_with("https://") {
                    ep.to_string()
                } else {
                    format!("http://{ep}")
                }
            })
            .collect();
        if endpoints.is_empty() {
            None
        } else {
            Some(Self { endpoints })
        }
    }
}

impl ClusterBackend for StaticBackend {
    fn provision(&self, spec: &ClusterSpec) -> Result<ClusterInfo> {
        let eps = if self.endpoints.is_empty() {
            (0..worker_bounds(spec).desired)
                .map(|i| format!("http://127.0.0.1:{}", spec.worker_port + i as u16))
                .collect()
        } else {
            self.endpoints.clone()
        };
        Ok(ClusterInfo {
            cluster_id: spec.cluster_id.clone(),
            connect_endpoint: "sc://127.0.0.1:50051".to_string(),
            worker_endpoints: eps,
        })
    }

    fn delete(&self, _cluster_id: &str) -> Result<()> {
        Ok(())
    }

    fn worker_endpoints(&self, spec: &ClusterSpec) -> Result<Vec<String>> {
        if !self.endpoints.is_empty() {
            return Ok(self.endpoints.clone());
        }
        Ok((0..worker_bounds(spec).desired)
            .map(|i| format!("http://127.0.0.1:{}", spec.worker_port + i as u16))
            .collect())
    }
}

/// Kubernetes backend: applies manifests via `kubectl` (HPA scales workers).
pub struct K8sBackend {
    /// When set, use DNS membership instead of kubectl for endpoint discovery.
    pub use_dns: bool,
}

impl Default for K8sBackend {
    fn default() -> Self {
        Self { use_dns: true }
    }
}

impl K8sBackend {
    /// Apply the rendered resources. The gateway service account needs RBAC scoped to the target
    /// namespace for server-side apply on Deployments, Services, HPAs, and the Deployment `scale`
    /// subresource. Cluster-wide Secret read/list is intentionally not part of this contract; data
    /// credentials should arrive through IRSA or explicitly bound SecretProviderClass objects.
    pub fn apply_manifests(&self, yaml: &str) -> Result<()> {
        let mut child = Command::new("kubectl")
            .args(["apply", "--server-side", "-f", "-"])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| Error::Io(format!("kubectl spawn: {e}")))?;
        use std::io::Write;
        child
            .stdin
            .take()
            .ok_or_else(|| Error::Io("kubectl stdin".into()))?
            .write_all(yaml.as_bytes())
            .map_err(|e| Error::Io(format!("kubectl write: {e}")))?;
        let status = child
            .wait()
            .map_err(|e| Error::Io(format!("kubectl wait: {e}")))?;
        if !status.success() {
            return Err(Error::Io("kubectl apply failed".into()));
        }
        Ok(())
    }

    /// Scale workers upward without rewriting unrelated resources. Idle scale-down/reap remains a
    /// platform concern: the gateway should delete the per-cluster namespace after its idle timeout
    /// rather than relying on workers to self-terminate.
    pub fn scale_worker_deployment(&self, spec: &ClusterSpec, desired_workers: u32) -> Result<()> {
        let mut next = spec.clone();
        next.worker_count = desired_workers.max(spec.worker_count);
        let bounds = worker_bounds(&next);
        let replicas = bounds.desired.to_string();
        let status = Command::new("kubectl")
            .arg("-n")
            .arg(&spec.namespace)
            .args(["scale", "deployment/oxidant-worker", "--replicas"])
            .arg(replicas)
            .status()
            .map_err(|e| Error::Io(format!("kubectl scale: {e}")))?;
        if !status.success() {
            return Err(Error::Io(
                "kubectl scale deployment/oxidant-worker failed".into(),
            ));
        }
        Ok(())
    }

    fn worker_deployment_yaml(spec: &ClusterSpec) -> String {
        let bounds = worker_bounds(spec);
        let memory_env = spec
            .worker_memory_limit_bytes
            .map(|bytes| {
                format!(
                    r#"        env:
        - name: OXIDANT_MEMORY_LIMIT_BYTES
          value: "{bytes}"
"#
                )
            })
            .unwrap_or_default();
        format!(
            r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: oxidant-worker
  namespace: {ns}
  annotations:
    oxidant.dev/idle-policy: "gateway deletes the cluster namespace after idle timeout"
    oxidant.dev/scale-policy: "parallelism-driven via gateway scale API and oxidant_pending_stage_tasks external metric"
spec:
  replicas: {replicas}
  selector:
    matchLabels:
      app: oxidant-worker
  template:
    metadata:
      labels:
        app: oxidant-worker
    spec:
      containers:
      - name: worker
        image: {image}
        args: ["worker", "--port", "{port}", "--foreground"]
{memory_env}        ports:
        - containerPort: {port}
---
apiVersion: v1
kind: Service
metadata:
  name: oxidant-worker
  namespace: {ns}
spec:
  clusterIP: None
  selector:
    app: oxidant-worker
  ports:
  - port: {port}
    targetPort: {port}
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: oxidant-worker
  namespace: {ns}
  annotations:
    oxidant.dev/scale-policy: "query parallelism (oxidant_pending_stage_tasks), not idle CPU"
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: oxidant-worker
  minReplicas: {min}
  maxReplicas: {max}
  metrics:
  - type: External
    external:
      metric:
        name: oxidant_pending_stage_tasks
        selector:
          matchLabels:
            cluster_id: {cluster_id}
      target:
        type: AverageValue
        averageValue: "1"
"#,
            ns = spec.namespace,
            replicas = bounds.desired,
            image = spec.worker_image,
            port = spec.worker_port,
            memory_env = memory_env,
            min = bounds.min,
            max = bounds.max,
            cluster_id = spec.cluster_id,
        )
    }
}

impl ClusterBackend for K8sBackend {
    fn provision(&self, spec: &ClusterSpec) -> Result<ClusterInfo> {
        self.apply_manifests(&Self::worker_deployment_yaml(spec))?;
        let eps = self.worker_endpoints(spec)?;
        Ok(ClusterInfo {
            cluster_id: spec.cluster_id.clone(),
            connect_endpoint: format!(
                "sc://oxidant-connect.{}.svc.cluster.local:50051",
                spec.namespace
            ),
            worker_endpoints: eps,
        })
    }

    fn delete(&self, cluster_id: &str) -> Result<()> {
        let ns = format!("oxidant-cl-{cluster_id}");
        let status = Command::new("kubectl")
            .args(["delete", "namespace", &ns, "--ignore-not-found"])
            .status()
            .map_err(|e| Error::Io(format!("kubectl delete: {e}")))?;
        if !status.success() {
            return Err(Error::Io("kubectl delete namespace failed".into()));
        }
        Ok(())
    }

    fn worker_endpoints(&self, spec: &ClusterSpec) -> Result<Vec<String>> {
        if self.use_dns {
            #[cfg(feature = "k8s")]
            {
                use oxidant_execution::membership::K8sMembership;
                let m = K8sMembership::new(spec.worker_service_host(), spec.worker_port);
                return Ok(m.endpoints());
            }
        }
        let _ = spec;
        let membership = StaticMembership::new(vec![]);
        Ok(membership.endpoints())
    }

    fn scale_up(&self, spec: &ClusterSpec, desired_workers: u32) -> Result<ClusterInfo> {
        self.scale_worker_deployment(spec, desired_workers)?;
        let mut next = spec.clone();
        next.worker_count = desired_workers.max(spec.worker_count);
        let bounds = worker_bounds(&next);
        next.worker_count = bounds.desired;
        next.min_workers = bounds.min;
        next.max_workers = bounds.max;
        Ok(ClusterInfo {
            cluster_id: next.cluster_id.clone(),
            connect_endpoint: format!(
                "sc://oxidant-connect.{}.svc.cluster.local:50051",
                next.namespace
            ),
            worker_endpoints: self.worker_endpoints(&next)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_backend_from_env() {
        std::env::set_var("OXIDANT_WORKERS", "127.0.0.1:50561,127.0.0.1:50562");
        let b = StaticBackend::from_env().unwrap();
        assert_eq!(b.endpoints.len(), 2);
    }

    #[test]
    fn hpa_manifest_uses_parallelism_external_metric() {
        let spec = ClusterSpec::local_demo("abc", 2);
        let yaml = K8sBackend::worker_deployment_yaml(&spec);
        assert!(yaml.contains("HorizontalPodAutoscaler"));
        assert!(yaml.contains("oxidant-worker"));
        assert!(yaml.contains("replicas: 2"));
        assert!(yaml.contains("minReplicas: 2"));
        assert!(yaml.contains("maxReplicas: 8"));
        assert!(yaml.contains("oxidant.dev/idle-policy"));
        assert!(yaml.contains("oxidant_pending_stage_tasks"));
        assert!(yaml.contains("cluster_id: abc"));
        assert!(!yaml.contains("averageUtilization"));
    }

    /// The Deployment overrides `args` only, never `command`, so the worker image's `CMD`
    /// migration does not cover this manifest — its own `args` must carry `--foreground`.
    /// Without it every provisioned pod hits `run_worker`'s refusal ("`oxidant worker` runs a
    /// long-lived process, and those run as daemons"), exits 1, and CrashLoopBackOffs: the
    /// control plane's paid surface, down, on every cluster it provisions.
    #[test]
    fn worker_manifest_runs_the_worker_in_the_foreground() {
        let spec = ClusterSpec::local_demo("abc", 2);
        let yaml = K8sBackend::worker_deployment_yaml(&spec);
        let args = yaml
            .lines()
            .find(|l| l.trim_start().starts_with("args:"))
            .expect("the worker container's args");
        assert!(
            args.contains("--foreground"),
            "a worker pod without --foreground CrashLoopBackOffs: {args}"
        );
        assert!(args.contains("\"worker\""), "{args}");
    }

    #[test]
    fn worker_manifest_includes_memory_limit_env() {
        let mut spec = ClusterSpec::local_demo("abc", 2);
        spec.worker_memory_limit_bytes = Some(26_000_000_000);
        let yaml = K8sBackend::worker_deployment_yaml(&spec);
        assert!(yaml.contains("OXIDANT_MEMORY_LIMIT_BYTES"));
        assert!(yaml.contains("26000000000"));
    }

    #[test]
    fn worker_bounds_clamps_min_max_and_desired() {
        let mut spec = ClusterSpec::local_demo("abc", 0);
        spec.min_workers = 0;
        spec.max_workers = 0;
        assert_eq!(
            worker_bounds(&spec),
            WorkerBounds {
                desired: 1,
                min: 1,
                max: 1
            }
        );

        spec.worker_count = 10;
        spec.min_workers = 2;
        spec.max_workers = 4;
        assert_eq!(
            worker_bounds(&spec),
            WorkerBounds {
                desired: 4,
                min: 2,
                max: 4
            }
        );
    }
}
