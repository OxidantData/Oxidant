# Cluster provisioning → Kubernetes (secure redesign)

> **Note:** `oxidant-orchestrator`, `oxidant-gateway`, and `deploy/helm/oxidant/` ship in this repo
> (worker HPA + gateway Deployment). Enable with `OXIDANT_ORCHESTRATOR=k8s` on the gateway.

This documents the migration of per-user compute "clusters" off the EC2
shell-`user_data` path and onto hardened Kubernetes pods, the security findings it
closes, how to enable it, and the one remaining gate before cluster-routed SQL is
re-enabled.

## Why

A multi-agent review of the control-plane (`feat/platform-control-plane`) found
the gateway was a path to remote code execution and had no real authorization.
The fix is structural: provision compute through declarative manifests (no shell
on the path) and enforce governance on every route and on the SQL data path.

## What changed (and where)

| Finding | Severity | Closed by |
|---|---|---|
| `aws_bin` → `Command::new` arbitrary-exec on the gateway host | HIGH | `validate_connection` allowlist + `GlueCatalog::from_config` reads `OXIDANT_AWS_BIN` from operator env only (`crates/oxidant-gateway/src/server.rs`, `crates/oxidant-catalog-glue/src/lib.rs`) |
| EC2 `user_data` single-quote breakout → root RCE at boot | HIGH | `ec2_user_data` carries catalog conf as inert base64, decoded at runtime — never a shell literal; **and** the new K8s backend has no shell at all |
| No authz on control-plane routes; RBAC decorative | MEDIUM | `require_admin` (server-side `admins` membership) on every mutating cluster/connection/user/group/grant route (`crates/oxidant-gateway/src/authz.rs`) |
| `run_sql` never consulted grants | MEDIUM | resolved-plan table-scan authorization via `oxidant_govern::Evaluator`; file-scan TVFs / `CREATE EXTERNAL TABLE` / `COPY` denied for governed sessions |
| Weak/default `OXIDANT_JWT_SECRET`, default admin password → forge admin token | MEDIUM | `serve` refuses to boot with weak secret / default password unless `OXIDANT_DEV_MODE=1` |
| IDOR on notebooks / saved queries | MEDIUM | server-set `owner` + `owns_or_admin` on read/update/delete; owner-filtered lists |
| One hardcoded Spark Connect `session_id` for all users | LOW | per-(user,cluster) session id (`session_id_for`) |
| Provisioning via a shared node instance profile | — | per-cluster IRSA (`deploy/terraform/modules/cluster-irsa`) |

The Kubernetes backend lives in the `oxidant-orchestrator` crate: a `ClusterBackend`
trait, a typed `ClusterSpec`, manifest builders, and `K8sBackend` (applies via
`kubectl apply --server-side -f -`). Pods are PodSecurity-`restricted`:
non-root, read-only root fs, drop ALL caps, seccomp `RuntimeDefault`, no
auto-mounted SA token, per-cluster namespace, default-deny + allowlist
NetworkPolicies, ResourceQuota/LimitRange, and per-cluster least-privilege IRSA.

Deploy artifacts:
- `deploy/docker/` — hardened images (non-root, read-only-rootfs-ready; gateway image bundles `kubectl`).
- `deploy/helm/oxidant/` — gateway Deployment + **scoped** RBAC (no cluster-wide Secret access), a `ValidatingAdmissionPolicy` bounding the gateway to `oxidant-cl-*` namespaces, and reference per-cluster object templates.
- `deploy/terraform/` — EKS OIDC, **`sub`+`aud`-pinned** per-cluster IRSA roles (least-priv S3/Glue), IMDSv2 `http_put_response_hop_limit=1` (defeats pod→IMDS node-credential theft), and etcd KMS encryption-at-rest.

## Enabling the K8s backend

Set on the gateway (all server-side / operator-controlled — never request body):

```
OXIDANT_ORCHESTRATOR=k8s
OXIDANT_CLUSTER_IMAGE=<registry>/oxidant-connect-server:<tag>
OXIDANT_WORKER_IMAGE=<registry>/oxidant-worker:<tag>
OXIDANT_CLUSTER_IRSA_ROLE_PREFIX=arn:aws:iam::<acct>:role/oxidant-cl-   # role = prefix + <cluster-id>
OXIDANT_CLUSTER_EGRESS_CIDRS=<s3-cidr>,<glue-cidr>,<hms-cidr>
OXIDANT_CLUSTER_SECRET_CLASS=<SecretProviderClass>                   # optional, CSI catalog creds
AWS_REGION=<region>
# production identity hardening (required unless OXIDANT_DEV_MODE=1):
OXIDANT_JWT_SECRET=<≥32 random bytes>
OXIDANT_ADMIN_PASSWORD=<non-default>
```

`provision` then applies the manifests and advertises the stable Service-DNS
endpoint `sc://oxidant-cl-<id>.oxidant-cl-<id>.svc.cluster.local:50051`; `delete`/idle
reap deletes the namespace (cascade GC). Process/EC2 paths remain; **rollback is
`OXIDANT_ORCHESTRATOR=ec2` (or unset) — a flag flip, no redeploy.**

## The remaining gate — Step 4.5 (engine-side enforcement)

Cluster-routed user SQL is **fail-closed for non-admins today** (it falls back to
the governed embedded engine). Gateway-side pre-authorization cannot be the
boundary for routed SQL because `oxidant-connect` re-parses raw SQL against
*different* catalogs than the gateway's embedded engine, can't see cluster-local
temp views, and currently performs **no authentication** of the caller. Before
cluster routing is re-enabled for non-admins, `oxidant-connect` must:

1. Instantiate a `oxidant_govern::GovernedCatalog` **per session**, with the identity
   bound to the gateway-minted `session_id`, authorizing at the catalog the
   cluster actually executes against (so temp views/CTEs resolve to governed base
   tables).
2. **Authenticate the gateway** — mTLS or a per-session signed token minted by the
   gateway — and reject any direct dial.
3. Disable filesystem TVFs / external `LOCATION` / arbitrary `SET` / UDF
   reflection for governed sessions, so mounted secret and IRSA-token files can't
   be exfiltrated via SQL.
4. Scope the per-cluster IRSA S3 policy to exactly the needed prefix (done in
   `deploy/terraform/modules/cluster-irsa`).

Until (1)–(3) ship and are deployed in an environment, leave non-admin cluster
routing disabled; governed queries run on the embedded engine. This is enforced
in code: `run_sql` returns a restriction error for non-admin cluster routing.

## Cutover checklist (Step 7)

1. Build + push `deploy/docker/` images.
2. `terraform apply deploy/terraform` → EKS OIDC + IRSA roles + KMS + IMDS hop-limit.
3. `helm install` `deploy/helm/oxidant` with scoped gateway RBAC + the namespace admission policy; verify the gateway SA has **no** cluster-wide Secret get/list.
4. Verify a policy-enforcing CNI (Calico/Cilium) and that IMDS is unreachable from a cluster pod.
5. Set the gateway env above; flip `OXIDANT_ORCHESTRATOR=k8s`. New clusters land on K8s; existing EC2 clusters keep running until drained.
6. Keep cluster routing disabled for non-admins until Step 4.5 is deployed.
7. After soak: remove the EC2 path.
