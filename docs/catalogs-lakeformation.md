# AWS Lake Formation fine-grained access control

Oxidant enforces AWS Lake Formation column-level and row-level security on Glue tables. With it
enabled, a table's denied columns are absent from its schema and its row filters are applied inside
the scan, on the driver and on every worker.

This layers on top of the Glue catalog — see [`catalogs-glue.md`](catalogs-glue.md) first.

> **Off by default.** Without `lakeformation=true` nothing changes: tables resolve with every column
> and every row visible, exactly as before.

## How it works

Oxidant follows the same protocol Amazon Athena and EMR Spark use for Lake Formation-registered
data — AWS calls it *application integration for third-party query engines*:

1. On first reference to a table, Oxidant calls **`glue:GetUnfilteredTableMetadata`**. Lake
   Formation returns the *effective* decision for the querying principal: the authorized column
   list, a row-filter `WHERE` fragment, and whether Lake Formation governs the table at all.
2. Oxidant calls **`lakeformation:GetTemporaryGlueTableCredentials`** and reads that table's S3
   objects with the short-lived credentials Lake Formation scoped to it — not with the engine's own
   identity. A table the principal was not granted is therefore unreadable even if the engine's role
   could reach the prefix.
3. Oxidant applies the column and row policy to every scan of that table.

Credentials are re-vended before they expire, so a long query never dies holding a stale token.
Re-vending goes back through the authorizer, which means a permission revoked mid-query stops
working at the next refresh rather than lasting until the process restarts.

Two implementation notes, because DataFusion's object-store model does not map onto this directly:

- **Vended credentials are per table; DataFusion registers object stores per bucket.** Oxidant
  installs a routing store for the bucket the first time a governed table appears in it, and
  dispatches each request by path prefix. Two governed tables in one bucket keep separate
  credentials, and ungoverned tables in the same bucket keep using the ambient identity.
- Credential vending is S3-only. A governed table on other storage still gets its column and row
  policy applied, just not scoped credentials.

Note this is *not* the same as Lake Formation's `ListPermissions` API. That one enumerates grants
for administrative tools; `GetUnfilteredTableMetadata` computes the effective answer for one caller,
which is what an engine needs.

**Tables that are not registered with Lake Formation are unaffected.** Lake Formation reports
`IsRegisteredWithLakeFormation=false` for them and Oxidant reads them exactly as it would with no
authorizer. This is what makes it safe to switch enforcement on for a catalog that also contains
ungoverned tables.

## Identity

Lake Formation grants are held against IAM principals, so the identity Oxidant enforces as is
always an **IAM role** — never a client-supplied user name. This is EMR's *runtime role* model.

| `lakeformation.identity` | Behavior |
|---|---|
| `hybrid` (default) | Use the configured runtime role when set; otherwise the engine's own identity |
| `user` | Require a runtime role; fail the query if none is configured |
| `machine` | Always the engine's own identity; ignore any runtime role |

The runtime role is named by `lakeformation.runtime_role_arn` and assumed with `sts:AssumeRole`.

**Why naming a role is not a way to escalate:** the assume only succeeds if that role's *trust
policy* allows Oxidant's own role. IAM decides whether the claim is legitimate, so an arbitrary role
ARN cannot be used to borrow someone else's grants.

When the engine's own identity is used, Oxidant normalizes what `sts:GetCallerIdentity` reports
(`arn:aws:sts::123:assumed-role/Analyst/session-1`) to the ARN grants are written against
(`arn:aws:iam::123:role/Analyst`). Without that rewrite every lookup would match nothing and read as
a total denial.

### Current limitation: the runtime role is per-deployment, not per-session

Catalogs are registered once per engine process, so `runtime_role_arn` is a deployment-level
setting. One Oxidant server enforces as one principal. Running several roles today means running
several servers — one per role — behind whatever routes users to them.

Per-session identity needs Spark Connect's `UserContext` plumbed through the engine and the
catalog's table cache keyed by session, which is tracked separately. The cache already keys on the
principal, so this is a plumbing change rather than a redesign.

## Configure

```sh
oxidant spark server --port 50051 \
  --catalog-conf spark.sql.catalog.glue.type=glue \
  --catalog-conf spark.sql.catalog.glue.region=us-west-2 \
  --catalog-conf spark.sql.catalog.glue.lakeformation=true \
  --catalog-conf spark.sql.catalog.glue.lakeformation.identity=hybrid \
  --catalog-conf spark.sql.catalog.glue.lakeformation.runtime_role_arn=arn:aws:iam::123456789012:role/oxidant-analyst \
  --catalog-conf spark.sql.catalog.glue.lakeformation.authorized_caller=arn:aws:iam::123456789012:role/oxidant-engine
```

| Option | Required | Purpose |
|--------|----------|---------|
| `…lakeformation` | yes | `true` enables enforcement |
| `…lakeformation.identity` | no | `hybrid` (default), `user`, `machine` |
| `…lakeformation.runtime_role_arn` | for `user` | IAM role representing the querying user |
| `…lakeformation.authorized_caller` | for vending | Value of the `LakeFormationAuthorizedCaller` session tag Lake Formation requires of a registered query engine |
| `…lakeformation.catalog_id` | no | Glue catalog (account) ID; defaults to the caller's account |
| `…lakeformation.vend_credentials` | no | Request Lake Formation-vended credentials. **Defaults `false`; see below before enabling** |

A bad value is a startup error, not a silent default — a typo in a security switch must not quietly
leave enforcement off.

### Distributed mode

Workers resolve tables through the same catalog bridge and authorize independently rather than
trusting the driver, so each reaches its own Lake Formation decision and applies its own filtering.

A worker started *without* the Lake Formation config would have no authorizer and nothing to apply —
and could not detect that on its own, since without an authorizer it has no way to know a table is
governed. So the driver, which did resolve the policy, stamps the requirement on every stage ticket,
and a worker that cannot enforce **fails the stage** rather than returning its shard unfiltered:

```
this query reads Lake Formation-governed data, but catalog `glue` in this process has no
Lake Formation authorizer configured — refusing to read `secure.customers` unfiltered.
```

The check runs before the worker's table cache, so a table it resolved earlier for an ungoverned
query cannot be served to a governed one.

The ticket also carries the principal the driver resolved the policy as, and a worker configured
with a *different* `identity` or `runtime_role_arn` is refused rather than allowed to apply its own,
possibly broader, policy to its shard:

```
Lake Formation principal mismatch reading `secure.customers`: the driver resolved this
query's policy as `arn:…:role/analyst`, but catalog `glue` in this process enforces as
`arn:…:role/admin`.
```

This turns a silent data leak into a loud failure; it is not a substitute for configuring workers.
Pass the identical `--catalog-conf` / `OXIDANT_CATALOG_CONF` to every `oxidant worker`.

## What enforcement looks like

Given a grant of `SELECT` on `(id, region, amount)` and a data-cell filter `region = 'us'`:

```sql
SELECT * FROM glue.secure.customers;
-- id | region | amount        <- `ssn` is simply absent
-- only region='us' rows

SELECT ssn FROM glue.secure.customers;
-- Error: No field named ssn
```

A denied column is removed from the schema rather than kept and rejected on reference. That is what
Athena and EMR do: `SELECT *` keeps working and narrows, and nothing reveals that the column exists.

A row filter may reference a column the principal cannot see — Oxidant reads it to evaluate the
predicate and projects it away before anything else in the plan can observe it.

`DESCRIBE` and `EXPLAIN` render the restricted schema, so they cannot be used to discover a column
the principal is not granted.

## Fail-closed behavior

AWS's contract for integrated engines is *distributed enforcement with explicit deny on failure*:
the engine is trusted to apply the policy and **must fail the query if it cannot**. Oxidant refuses
rather than under-filtering in every one of these cases:

| Situation | Result |
|---|---|
| Lake Formation unreachable or erroring | Query fails |
| `PermissionTypeMismatchException` (table needs nested column/cell filtering) | Query fails |
| Different row filters on different columns | Query fails — one scan predicate cannot express it |
| Row filter that will not parse, or names an unknown column | Query fails |
| No authorized column on the table | Query fails |
| `identity=user` with no runtime role | Query fails |

The only permissive outcome is Lake Formation explicitly reporting that it does not govern the
table.

Revoking a grant takes effect within the catalog cache TTL (`OXIDANT_CATALOG_CACHE_TTL_MS`, default
60s) — the decision is part of the cache fingerprint, so a revocation is picked up on the next
revalidation instead of surviving until restart.

## Security boundary — read this before relying on it

**Oxidant does not isolate user-supplied code from the enforcement path.** EMR runs a privileged
*System* driver that holds the Lake Formation credentials and does the filtering, and a separate
*User* driver that runs user code and can reach neither. To keep that boundary intact EMR blocks
RDDs, custom UDFs and UDTs, custom data sources, and extra jars for Lake Formation-enabled jobs.

Oxidant has no equivalent sandbox. A user-supplied UDF runs in the same process as the scan of a
protected table. Treat this feature as enforcing policy for *ordinary SQL against a trusted engine
deployment*, not as a boundary against an adversary who can execute arbitrary code in the engine.

Two further consequences worth stating plainly:

- **There is no authentication on the Spark Connect endpoint.** Anyone who can reach the port
  queries as the deployment's configured principal. Put an authenticating front-end in front of it.
- **The worker Flight plane is trusted and unauthenticated.** Anyone who can reach a worker's Flight
  port can already read whatever that worker's own credentials reach, Lake Formation or not. The
  stage-ticket guard protects against *misconfiguration*, not against someone on that network path.
  Keep the Flight port on a private network.

Note that with credential vending on (the default), turning `vend_credentials=false` drops back to
reading with the engine's own S3 credentials — enforcement then holds only for queries that go
through Oxidant, and anyone with direct S3 access to the prefix bypasses it.

## AWS-side setup

Lake Formation must be told that Oxidant is an allowed third-party engine:

**1–2. Data lake settings** — one `put-data-lake-settings` call. All three of these are required
for a third-party engine; missing the third is the most common cause of `Not authorized to call
this API`:

```json
{
  "DataLakeAdmins": [{"DataLakePrincipalIdentifier": "arn:aws:iam::ACCOUNT:user/admin"}],
  "CreateDatabaseDefaultPermissions": [
    {"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": ["ALL"]}
  ],
  "CreateTableDefaultPermissions": [
    {"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": ["ALL"]}
  ],
  "AllowExternalDataFiltering": true,
  "ExternalDataFilteringAllowList": [{"DataLakePrincipalIdentifier": "ACCOUNT"}],
  "AuthorizedSessionTagValueList": ["oxidant"]
}
```

`AuthorizedSessionTagValueList` registers the *value* Oxidant sets for the
`LakeFormationAuthorizedCaller` session tag. It must match
`spark.sql.catalog.<n>.lakeformation.authorized_caller`. Keeping the two
`…DefaultPermissions` entries as `IAM_ALLOWED_PRINCIPALS` is what leaves every existing database on
plain IAM access control.

```sh
# 3. Register only the location you want governed.
aws lakeformation register-resource --resource-arn arn:aws:s3:::bucket --use-service-linked-role

# 4. Revoke IAM_ALLOWED_PRINCIPALS on that database AND table — this is the step that
#    actually turns enforcement on, and it is per-resource.
aws lakeformation revoke-permissions \
  --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
  --resource '{"Table":{"DatabaseName":"secure","Name":"customers"}}' --permissions ALL

# 5. Create the data-cell filter and grant it to the runtime role.
aws lakeformation create-data-cells-filter --table-data '{...,"RowFilter":{"FilterExpression":"region = '"'"'us'"'"'"},"ColumnNames":["id","region","amount"]}'
aws lakeformation grant-permissions --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:role/analyst \
  --resource '{"DataCellsFilter":{...}}' --permissions SELECT
```

Steps 1–2 are account-level but additive: neither registers a location nor revokes
`IAM_ALLOWED_PRINCIPALS` anywhere, so no existing database changes access behavior. Enforcement
begins only for resources explicitly registered in step 3.

The runtime role's IAM policy needs `glue:GetUnfilteredTableV2` and
`glue:GetUnfilteredPartitionsV2` in addition to `glue:GetUnfilteredTableMetadata` — the public
action name and the one actually authorized differ, and the error message names the V2 form.

## Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| Everything reads unrestricted | The table is not registered with Lake Formation — check `register-resource`, and that `IAM_ALLOWED_PRINCIPALS` was revoked on the database/table |
| Every table denies | The principal does not match the grant. Check the resolved ARN — a role's grant is on `arn:aws:iam::…:role/Name`, not the `assumed-role` session ARN |
| `PermissionTypeMismatchException` | The table uses nested column or nested cell filtering, which Oxidant does not enforce and therefore refuses |
| "different row filters to different columns" | Several data-cell filters give different row visibility per column; grant a single filter covering all columns |
| `Not authorized to call this API` | The session tag value is not in `AuthorizedSessionTagValueList`. Setting `AllowExternalDataFiltering` and `ExternalDataFilteringAllowList` alone is not enough — all three are required |
| `not authorized to perform: glue:GetUnfilteredTableV2` | The role's IAM policy lists only `glue:GetUnfilteredTableMetadata`; add the `…V2` actions |
| `Invalid permission type` | More than one `SupportedPermissionTypes` tier named. Oxidant sends exactly one; seeing this means a client or proxy rewrote the request |
| Credential vending refused | Missing `LakeFormationAuthorizedCaller` session tag (`…lakeformation.authorized_caller`), or the account is not in `ExternalDataFilteringAllowList` |
| Works on the driver, fails on workers | Workers need the same catalog config — pass it to every `oxidant worker` |

## References

- [How Athena accesses data registered with Lake Formation](https://docs.aws.amazon.com/athena/latest/ug/lf-athena-access.html)
- [How Lake Formation application integration works](https://docs.aws.amazon.com/lake-formation/latest/dg/how-vending-works.html)
- [`GetUnfilteredTableMetadata`](https://docs.aws.amazon.com/glue/latest/webapi/API_GetUnfilteredTableMetadata.html)
- [EMR on EKS with Lake Formation](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/security_iam_fgac-lf-works.html)
