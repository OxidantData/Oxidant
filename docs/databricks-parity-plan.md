# Oxidant SQL parity with Databricks SQL (Glue + Lake Formation)

## Goal
Systematically validate Oxidant SQL against the [Databricks SQL language manual](https://docs.databricks.com/aws/en/sql/language-manual/) and close the gaps required for Oxidant to run Databricks-style workloads on **AWS Glue Data Catalog** and **AWS Lake Formation**. All AWS-side implementations must use the official AWS SDK for Rust (`aws-sdk-*`) in-process; shelling out to the `aws` CLI is explicitly out of scope.

## Current state

| Area | Status | Evidence |
|------|--------|----------|
| Spark SQL parity baseline | strict 4.2% (536/12,641), semantic 39.4% (4,984/12,641) vs. Spark v4.0.0 | `parity/baseline.json` |
| SQL engine | DataFusion with `Dialect::Databricks`; front-end rewrites live in `oxidant_loom::normalize_spark_sql` and the stub `oxidant-sql` crate | `crates/oxidant-loom/src/lib.rs`, `crates/oxidant-sql/src/dialect.rs` |
| Glue catalog | Read + CTAS via `aws-sdk-glue` in-process; no CLI dependency | `crates/oxidant-catalog-glue/src/lib.rs`, `docs/catalogs-glue.md` |
| Lake Formation | **No support** — no `aws-sdk-lakeformation` usage, no row/column filtering, no cross-account resource links | Grep returned zero matches |
| DDL/DML | Read-only external catalogs; `CREATE TABLE … USING <format>` is the largest parser/missing-relation driver | `crates/oxidant-spark-compat/ROADMAP.md`, `CREATE_TABLE_USING_DESIGN.md` |

## Scope boundaries

**In scope**
- Databricks SQL language surface that maps to Oxidant’s existing single-node/distributed engine (SELECT, DDL, DML, functions, type system).
- Glue Data Catalog: full DDL lifecycle, partitioned tables, column stats, table properties.
- Lake Formation: fine-grained access control (data filters, column/row permissions), cross-account grants, resource links, and credential vending via `AssumeDecoratedRoleWithSAML`/ `GetTemporaryGlueCredentials` where needed, all via AWS SDK.
- Parity measurement and CI gates.

**Out of scope**
- Databricks-only platform concepts that require a Databricks control plane (Unity Catalog sharing, Delta Live Tables pipelines, Databricks SQL warehouses, serverless compute, widgets).
- AWS CLI wrappers; any gap requiring AWS CLI is rejected in design review.

## Recommended approach

A single, phased approach is recommended:

1. **Inventory and measure** — map every Databricks SQL manual section to a coverage cell, run the existing Spark v4.0.0 golden corpus, and add Databricks-specific supplement tests.
2. **Catalog/security foundation** — extend the Glue provider with missing DDL and add a Lake Formation access-control layer.
3. **SQL dialect work** — implement parser/planner gaps in `oxidant-sql` (the intended home per `ROADMAP.md`), not as lossy string rewrites in `Engine::sql`.
4. **Gating** — add a Databricks-parity ratchet to CI so regressions are caught.

This keeps Lake Formation as a first-class design constraint rather than a bolt-on, which matters because Lake Formation changes the catalog contract: table metadata may be readable but data access must be authorized.

## Execution phases

### Phase 0 — Inventory and baseline (2–3 weeks)
- Build a coverage matrix of the Databricks SQL manual sections vs. Oxidant capabilities.
- Categorize each cell as: **pass**, **Spark-parity gap**, **Databricks-specific gap**, or **not applicable**.
- Run the existing `oxidant-spark-compat` golden harness and record per-section results.
- Add a new `databricks-tests/` corpus (or flag existing Spark tests as Databricks-relevant) covering: `USING`, `TBLPROPERTIES`, `PARTITIONED BY`, `CLUSTER BY`, `DISTRIBUTE BY`, `SORT BY`, `LATERAL VIEW`, `PIVOT`, `UNPIVOT`, `MATCH_RECOGNIZE`, `QUALIFY`, `COPY INTO`, `MERGE INTO`, Delta Lake SQL, Lake Formation `GRANT`/`REVOKE` patterns.

### Phase 1 — SQL dialect and parser gaps (4–6 weeks)
- Stand up the staged `oxidant-sql::dialect::lower` pipeline (string prefilter → AST intercept → output naming) described in `ROADMAP.md`.
- Implement, in priority order:
  1. Function-registration backlog (Wave A aliases → Waves B–F) — additive, low risk, biggest semantic lever.
  2. `CREATE TABLE … USING <format>` lowered to the catalog-backed create path.
  3. `USE CATALOG` / `USE DATABASE` / `USE SCHEMA` via AST intercept.
  4. `LIKE ANY` / `LIKE ALL`, `PIVOT` / `UNPIVOT`.
  5. `SHOW` / `DESCRIBE` metadata statements.
  6. Spark output-name pass for the `schema-only` bucket.
- Keep rewrites faithful; lossy rewrites (e.g. silently making a persistent table in-memory) are forbidden.

### Phase 2 — Glue Data Catalog completeness (3–4 weeks)
Extend `oxidant-catalog-glue` using `aws-sdk-glue` only:
- `ALTER TABLE` (add/rename/change columns, set/unset table properties).
- `DROP TABLE` / `DROP DATABASE` with `IF EXISTS` / `CASCADE`.
- `CREATE DATABASE` / `CREATE SCHEMA`.
- `REPAIR TABLE` / `MSCK REPAIR TABLE` to discover new partitions.
- `SHOW PARTITIONS`.
- Column/statistics support: `ANALYZE TABLE … COMPUTE STATISTICS` (store via `aws-sdk-glue` `UpdateTable`/`ColumnStatistics`).
- Table properties round-trip (`TBLPROPERTIES`) through Glue `Parameters`.

### Phase 3 — Lake Formation integration (4–6 weeks)
Add a new `oxidant-catalog-lakeformation` crate (or extend the Glue provider) using `aws-sdk-lakeformation`:
- Authenticate via the standard AWS credential chain (no CLI).
- On table load, call `GetDataLakeSettings` / `GetEffectivePermissionsForPath` / `GetDataCellsFilter` to determine authorized columns/rows.
- Apply row filters and column masks by rewriting the table scan plan (push filters into DataFusion `TableProvider`) rather than post-filtering.
- Support cross-account shared catalogs via Lake Formation resource links (`GetResourceLinks`, `ListPermissions`).
- Honor `GRANT`/`REVOKE` semantics for securable objects; surface `AccessDenied` errors with the Lake Formation error code.
- Integration tests using a local stub HTTP server mirroring AWS JSON 1.1 (same pattern as `oxidant-catalog-glue` stub tests).

### Phase 4 — Delta Lake SQL on Glue (3–4 weeks)
- `CONVERT TO DELTA` (Parquet → Delta in place on S3).
- `DESCRIBE HISTORY`, `OPTIMIZE`, `VACUUM`, `RESTORE`.
- Time travel (`VERSION AS OF`, `TIMESTAMP AS OF`) already partially supported; validate against Glue-registered Delta tables.
- `MERGE INTO`, `UPDATE`, `DELETE FROM` for Delta tables backed by Glue.

### Phase 5 — Gating and continuous validation (2 weeks)
- Add a Databricks-parity CI job: `cargo run -p oxidant-spark-compat --bin oxidant-parity -- databricks-ratchet --baseline parity/databricks-baseline.json`.
- Add a Glue/Lake Formation integration test job using LocalStack or recorded AWS stubs.
- Update `docs/catalogs-glue.md` and add `docs/catalogs-lakeformation.md`.
- Re-baseline and commit `parity/databricks-baseline.json`.

## Jira epic and tickets

> **Note:** I cannot create tickets directly in Jira from this environment. The tickets below are ready to copy into an epic. Use the epic summary as the parent for all stories.

### Epic
- **Key:** `OXIDANT-DBR-PARITY` (placeholder; assign real key in Jira)
- **Summary:** Oxidant SQL parity with Databricks SQL on AWS Glue + Lake Formation
- **Description:** Systematically validate Oxidant against the Databricks SQL language manual, close parser/dialect/function gaps, extend Glue catalog DDL, and add Lake Formation fine-grained access control. All AWS interactions use AWS SDK for Rust in-process; AWS CLI is not allowed.
- **Acceptance criteria:**
  - Coverage matrix exists for every Databricks SQL manual section.
  - Databricks parity ratchet is green in CI.
  - Glue catalog supports full DDL lifecycle via `aws-sdk-glue`.
  - Lake Formation row/column filters work for Glue tables via `aws-sdk-lakeformation`.

### Stories

| ID | Summary | Component | AC | Effort |
|----|---------|-----------|----|--------|
| `OXIDANT-DBR-001` | Build Databricks SQL coverage matrix | docs / parity | Matrix in `docs/databricks-coverage.md`; each manual section mapped to pass/gap/NA with baseline counts | 1w |
| `OXIDANT-DBR-002` | Add Databricks-specific SQL corpus to parity harness | oxidant-spark-compat | New `databricks-tests/` inputs + goldens; harness runs them; skipped files recorded with reason | 2w |
| `OXIDANT-DBR-003` | Implement staged `oxidant-sql` dialect pipeline | oxidant-sql | Registry shell + migrate `strip_temporary_view` + AST intercept + output naming pass | 2w |
| `OXIDANT-DBR-004` | Register Wave A function aliases | oxidant-loom / functions | `starts_with`/`ends_with`, `var_samp`, `length`, `approx_distinct`, `bool_or`/`bool_and`, `signum`, `pow`, `ucase`/`lcase`/`char` aliases pass their golden blocks | 1w |
| `OXIDANT-DBR-005` | Implement high-value scalar UDF backlog | oxidant-functions | `split`, `mask`, `to_char`/`to_varchar`/`to_number`, `format_string`, `typeof`, `elt`, `bit_count`, `size`/`array_size`, `sort_array`, `map_contains_key`, `parse_url`, `url_encode`/`decode` | 3w |
| `OXIDANT-DBR-006` | Implement `CREATE TABLE … USING <format>` for Glue/local catalog | oxidant-sql / catalog | `CREATE TABLE glue.db.t (…) USING parquet LOCATION 's3://…' TBLPROPERTIES (…)` creates Glue table + writes Parquet files; golden blocks pass | 3w |
| `OXIDANT-DBR-007` | Implement `USE CATALOG` / `USE DATABASE` / `USE SCHEMA` | oxidant-sql | AST intercept sets current catalog/namespace; handles quoting, comments, semicolons; rejects invalid catalogs as Spark does | 1w |
| `OXIDANT-DBR-008` | Implement `LIKE ANY` / `LIKE ALL` | oxidant-sql | Rewritten to OR/AND chain at AST level; golden blocks pass | 1w |
| `OXIDANT-DBR-009` | Implement `PIVOT` / `UNPIVOT` | oxidant-sql / plan | Requires child-schema resolution; emits correct `LogicalPlan`; golden blocks pass | 3w |
| `OXIDANT-DBR-010` | Implement `SHOW` / `DESCRIBE` metadata statements | oxidant-sql / catalog | `SHOW DATABASES IN glue`, `SHOW TABLES IN glue.db`, `DESCRIBE TABLE glue.db.t`, `SHOW CREATE TABLE` return catalog metadata directly | 2w |
| `OXIDANT-DBR-011` | Spark output-name reconciliation pass | oxidant-sql | `schema-only` bucket reduced by matching Spark `Expression.sql` headers for common expressions | 3w |
| `OXIDANT-DBR-012` | Extend Glue catalog DDL: ALTER/DROP/CREATE DATABASE/REPAIR | oxidant-catalog-glue | Uses `aws-sdk-glue`; no CLI; integration tests with stub server | 3w |
| `OXIDANT-DBR-013` | Glue column statistics via ANALYZE TABLE | oxidant-catalog-glue | `ANALYZE TABLE glue.db.t COMPUTE STATISTICS` persists and reads column stats via Glue SDK | 2w |
| `OXIDANT-DBR-014` | Add Lake Formation authorization crate | oxidant-catalog-lakeformation | New crate using `aws-sdk-lakeformation`; resolves effective permissions per table; unit tests with stub server | 3w |
| `OXIDANT-DBR-015` | Apply Lake Formation row filters and column masks to scans | oxidant-loom / plan | Plan rewrite injects filters/masks from OXIDANT-DBR-014; honors `AccessDenied`; integration tests pass | 3w |
| `OXIDANT-DBR-016` | Lake Formation cross-account resource links | oxidant-catalog-lakeformation | `GetResourceLinks`, `ListPermissions` support; shared Glue tables resolve through resource links | 2w |
| `OXIDANT-DBR-017` | Delta Lake SQL on Glue: CONVERT/VACUUM/OPTIMIZE/RESTORE | oxidant-datasource / delta | Delta operations work on Glue-registered tables; stubs + S3 integration tests | 3w |
| `OXIDANT-DBR-018` | Delta `MERGE`/`UPDATE`/`DELETE` on Glue tables | oxidant-datasource / delta | DML executes and updates Glue table metadata as needed | 3w |
| `OXIDANT-DBR-019` | Databricks parity ratchet CI job | ci | New GitHub Actions job runs Databricks corpus and fails on regression; baseline committed | 1w |
| `OXIDANT-DBR-020` | Document Glue + Lake Formation SQL support | docs | Update `catalogs-glue.md`, add `catalogs-lakeformation.md`, add Databricks parity section to README | 1w |

## Dependencies and ordering

```
OXIDANT-DBR-001/002 (baseline) → OXIDANT-DBR-003 (pipeline shell)
  → OXIDANT-DBR-004–011 (dialect/function)  ─┬─→ OXIDANT-DBR-019 (ratchet)
  → OXIDANT-DBR-012–013 (Glue DDL)          ─┤
  → OXIDANT-DBR-014–016 (Lake Formation)    ─┤
  → OXIDANT-DBR-017–018 (Delta DML)         ─┘
  → OXIDANT-DBR-020 (docs)
```

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| 100% strict parity is structurally unreachable (error-text divergence, naming long-tail) | Set semantic parity as the product goal; cap error-text matching investment |
| Lake Formation requires real AWS account for realistic tests | Use stub HTTP server for unit tests; schedule periodic real-account integration run |
| Lossy string rewrites silently corrupt production data | Enforce the faithfulness principle; all DDL rewrites live in `oxidant-sql` AST intercept |
| AWS CLI temptation for quick fixes | Code-review gate: reject any PR introducing `std::process::Command("aws")` or shelling out |

## Success metrics

- Semantic parity vs. Databricks SQL corpus ≥ 85%.
- Strict parity vs. Databricks SQL corpus ≥ 60%.
- All Glue DDL operations pass integration tests without AWS CLI.
- Lake Formation row/column filters demonstrate `GRANT`/`REVOKE` behavior in integration tests.
- CI ratchet prevents regression.
