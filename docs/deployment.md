# Deploying Weft as a self-hosted data platform on AWS

> **Status: outline / contract for the full platform** (SSO, gateway operator, Terraform).
> For a **runnable** distributed data-plane today (Kind or BYO EKS: connect-server + workers,
> AWS CLI in the image), use [`distributed-k8s.md`](distributed-k8s.md) instead.
>
> The Terraform modules and complete Helm control-plane referenced below are **not all in-tree
> yet**. This document remains the long-term user-facing deploy guide.

Weft deploys entirely into **your own AWS account** (self-hosted, single-account). You get a
Databricks-like workspace: SSO login, EKS-backed compute clusters you spin up and down, a local or
external (HMS / Glue / Unity) catalog with Unity-Catalog-style ACLs, and a web UI for SQL,
notebooks, dashboards, and scheduled jobs.

## Architecture (what gets created)

- **Control plane** (always-on, small): the gateway (public via ALB), the cluster-manager
  operator, the scheduler, and an RDS Postgres metastore — all on EKS.
- **Data plane** (elastic): each *cluster* = one Spark Connect driver pod fronting N Arrow Flight
  worker pods, autoscaled. The control plane never touches query data.
- **Storage**: your S3 data buckets + a workspace bucket (notebooks, query results, artifacts).
  Worker pods get scoped S3 access via **IRSA** — no static keys.

## Prerequisites

- An AWS account + admin credentials for `terraform apply`.
- `terraform`, `kubectl`, `helm`, and the AWS CLI installed.
- An OIDC or SAML IdP (Okta / Azure AD / Google / Cognito) for SSO.
- An Anthropic API key (for the AI assist feature) — stored in Secrets Manager, never in the browser.

## Steps (target flow)

1. **Provision infrastructure** — `cd deploy/terraform && terraform apply`. Creates the VPC, EKS
   cluster, ECR repos, RDS Postgres, ALB, S3 workspace bucket, the EKS OIDC provider, and the
   per-cluster IAM roles for IRSA.
2. **Build & push images** — the connect-server, worker, gateway, cluster-manager, scheduler, and
   pyworker images to ECR (`deploy/docker/`).
3. **Install the platform** — `helm install weft deploy/helm/weft` with your RDS endpoint, OIDC/SAML
   config, workspace bucket, and Anthropic secret ARN as values. Installs the `WeftCluster` CRD,
   the control-plane Deployments, IRSA ServiceAccounts, HPAs, and NetworkPolicies.
4. **Configure SSO** — point the gateway at your IdP; enable SCIM so users/groups sync.
5. **First query** — log in, create a cluster, create a local catalog + table over an `s3://`
   path (or attach an external catalog), `GRANT SELECT` to a group, and run a query in the SQL
   editor.

## Operations

- **Clusters**: create/start/stop/resize from the UI; ephemeral *job clusters* are created per
  scheduled run and torn down automatically.
- **Governance**: `GRANT`/`REVOKE`/`SHOW GRANTS` in SQL or the Permissions UI; row filters and
  column masks apply at query time.
- **Cost**: clusters auto-stop after an idle timeout; a small warm worker-pool trims cold-start.
- **Migration from Spark/Databricks**: point existing PySpark at the cluster's Spark Connect
  endpoint (`sc://…`); use the catalog browser + AI assist to port SQL.
