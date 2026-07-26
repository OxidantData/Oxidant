//! Cluster backend trait and implementations.

use std::process::Command;

use weft_common::{Error, Result};
use weft_execution::membership::{ClusterMembership, StaticMembership};

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
struct WorkerBounds {
    desired: u32,
    min: u32,
    max: u32,
}

fn worker_bounds(spec: &ClusterSpec) -> WorkerBounds {
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
        let raw = std::env::var("WEFT_WORKERS").ok()?;
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
            .args(["scale", "deployment/weft-worker", "--replicas"])
            .arg(replicas)
            .status()
            .map_err(|e| Error::Io(format!("kubectl scale: {e}")))?;
        if !status.success() {
            return Err(Error::Io(
                "kubectl scale deployment/weft-worker failed".into(),
            ));
        }
        Ok(())
    }

    fn worker_deployment_yaml(spec: &ClusterSpec) -> String {
        let bounds = worker_bounds(spec);
        format!(
            r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: weft-worker
  namespace: {ns}
  annotations:
    weft.dev/idle-policy: "gateway deletes the cluster namespace after idle timeout"
spec:
  replicas: {replicas}
  selector:
    matchLabels:
      app: weft-worker
  template:
    metadata:
      labels:
        app: weft-worker
    spec:
      containers:
      - name: worker
        image: {image}
        args: ["worker", "--port", "{port}"]
        ports:
        - containerPort: {port}
---
apiVersion: v1
kind: Service
metadata:
  name: weft-worker
  namespace: {ns}
spec:
  clusterIP: None
  selector:
    app: weft-worker
  ports:
  - port: {port}
    targetPort: {port}
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: weft-worker
  namespace: {ns}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: weft-worker
  minReplicas: {min}
  maxReplicas: {max}
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
"#,
            ns = spec.namespace,
            replicas = bounds.desired,
            image = spec.worker_image,
            port = spec.worker_port,
            min = bounds.min,
            max = bounds.max,
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
                "sc://weft-connect.{}.svc.cluster.local:50051",
                spec.namespace
            ),
            worker_endpoints: eps,
        })
    }

    fn delete(&self, cluster_id: &str) -> Result<()> {
        let ns = format!("weft-cl-{cluster_id}");
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
                use weft_execution::membership::K8sMembership;
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
                "sc://weft-connect.{}.svc.cluster.local:50051",
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
        std::env::set_var("WEFT_WORKERS", "127.0.0.1:50561,127.0.0.1:50562");
        let b = StaticBackend::from_env().unwrap();
        assert_eq!(b.endpoints.len(), 2);
    }

    #[test]
    fn hpa_manifest_contains_replicas() {
        let spec = ClusterSpec::local_demo("abc", 2);
        let yaml = K8sBackend::worker_deployment_yaml(&spec);
        assert!(yaml.contains("HorizontalPodAutoscaler"));
        assert!(yaml.contains("weft-worker"));
        assert!(yaml.contains("replicas: 2"));
        assert!(yaml.contains("minReplicas: 2"));
        assert!(yaml.contains("maxReplicas: 8"));
        assert!(yaml.contains("weft.dev/idle-policy"));
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
