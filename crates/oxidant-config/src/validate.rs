//! Config validation.
//!
//! Everything checkable without touching the network or the filesystem is checked here, at
//! load, so a typo fails before the binary opens a broker connection or creates a database.
//! What is deliberately *not* checked here: whether a catalog is reachable, whether a table's
//! location exists, or whether a table's SQL resolves — those need the engine, and belong to
//! `oxidant pipeline validate`.

use std::collections::{BTreeMap, BTreeSet};

use oxidant_common::{Error, Result};

use crate::{OxidantConfig, SourceConfig, TableKind};

/// Catalog types that implement enough write DDL to be a pipeline sink.
///
/// Hive lacks `create_database` and REST lacks all write DDL, so a pipeline pointed at either
/// fails at the first table creation — after the source has already been read. Rejecting it
/// at load turns a confusing mid-run failure into a config error.
const SINK_CAPABLE_CATALOGS: &[&str] = &["local", "glue"];

/// Catalog types the provider factory knows how to build.
const KNOWN_CATALOG_TYPES: &[&str] = &["local", "glue", "hive", "rest", "unity", "iceberg"];

/// Table formats Oxidant can read. `orc`/`avro` are deliberately absent.
const KNOWN_FORMATS: &[&str] = &["parquet", "delta", "iceberg", "csv", "json"];

/// Formats a pipeline table can be written as.
const WRITABLE_SINK_FORMATS: &[&str] = &["delta", "parquet", "csv", "json"];

impl OxidantConfig {
    /// Validate the whole config. Errors name the offending key so the fix is obvious.
    pub(crate) fn validate(&self) -> Result<()> {
        self.validate_catalogs()?;
        self.validate_default_catalog()?;
        self.validate_pipeline()?;
        self.validate_tables()?;
        self.validate_dag()?;
        Ok(())
    }

    fn validate_catalogs(&self) -> Result<()> {
        for (name, catalog) in &self.catalogs {
            if name.trim().is_empty() {
                return Err(Error::Io("a catalog name cannot be empty".into()));
            }
            let kind = catalog.catalog_type.trim().to_ascii_lowercase();
            if !KNOWN_CATALOG_TYPES.contains(&kind.as_str()) {
                return Err(Error::Io(format!(
                    "catalog `{name}` has unknown type `{}` (expected one of: {})",
                    catalog.catalog_type,
                    KNOWN_CATALOG_TYPES.join(", ")
                )));
            }
            let is_local = kind == "local";
            if !is_local && !catalog.tables.is_empty() {
                return Err(Error::Io(format!(
                    "catalog `{name}` is type `{kind}`, which owns its own table list; \
                     `tables:` is only meaningful on a `local` catalog"
                )));
            }
            if !is_local && !catalog.discover.is_empty() {
                return Err(Error::Io(format!(
                    "catalog `{name}` is type `{kind}`; `discover:` is only meaningful on a \
                     `local` catalog"
                )));
            }
            if is_local && catalog.warehouse.is_none() {
                return Err(Error::Io(format!(
                    "local catalog `{name}` needs a `warehouse:` — it is where tables created \
                     by a pipeline or by CREATE TABLE are written"
                )));
            }
            for (table, entry) in &catalog.tables {
                if table.split('.').count() != 2 {
                    return Err(Error::Io(format!(
                        "catalog `{name}` table key `{table}` must be `namespace.table`"
                    )));
                }
                let format = entry.format.trim().to_ascii_lowercase();
                if !KNOWN_FORMATS.contains(&format.as_str()) {
                    return Err(Error::Io(format!(
                        "catalog `{name}` table `{table}` has unreadable format `{}` \
                         (expected one of: {})",
                        entry.format,
                        KNOWN_FORMATS.join(", ")
                    )));
                }
                if entry.location.trim().is_empty() {
                    return Err(Error::Io(format!(
                        "catalog `{name}` table `{table}` needs a `location:`"
                    )));
                }
            }
            for entry in &catalog.discover {
                if entry.namespace.trim().is_empty() || entry.path.trim().is_empty() {
                    return Err(Error::Io(format!(
                        "catalog `{name}` has a `discover:` entry missing `namespace:` or `path:`"
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_default_catalog(&self) -> Result<()> {
        let Some(default) = &self.default_catalog else {
            return Ok(());
        };
        // `spark_catalog` is the engine's own built-in and is never declared in `catalogs:`.
        if default == "spark_catalog" || self.catalogs.contains_key(default) {
            return Ok(());
        }
        Err(Error::Io(format!(
            "default_catalog `{default}` is not declared in `catalogs:` (declared: {})",
            declared_names(self)
        )))
    }

    fn validate_pipeline(&self) -> Result<()> {
        match (&self.pipeline, self.tables.is_empty()) {
            (None, true) => return Ok(()),
            (None, false) => {
                return Err(Error::Io(
                    "`tables:` declares a pipeline but there is no `pipeline:` section saying \
                     which catalog and schema they materialize into"
                        .into(),
                ))
            }
            (Some(pipeline), true) => {
                return Err(Error::Io(format!(
                    "pipeline `{}` declares no `tables:` — it would start and do nothing",
                    pipeline.name
                )))
            }
            (Some(_), false) => {}
        }
        let pipeline = self.pipeline.as_ref().expect("checked above");
        if pipeline.name.trim().is_empty() {
            return Err(Error::Io("`pipeline.name` cannot be empty".into()));
        }
        if pipeline.schema.trim().is_empty() {
            return Err(Error::Io("`pipeline.schema` cannot be empty".into()));
        }
        if pipeline.checkpoints.trim().is_empty() {
            return Err(Error::Io(
                "`pipeline.checkpoints` cannot be empty — it is the source of truth for replay \
                 position, so it must be a durable location"
                    .into(),
            ));
        }
        let Some(catalog) = self.catalogs.get(&pipeline.catalog) else {
            return Err(Error::Io(format!(
                "pipeline.catalog `{}` is not declared in `catalogs:` (declared: {})",
                pipeline.catalog,
                declared_names(self)
            )));
        };
        let kind = catalog.catalog_type.trim().to_ascii_lowercase();
        if !SINK_CAPABLE_CATALOGS.contains(&kind.as_str()) {
            return Err(Error::Io(format!(
                "pipeline.catalog `{}` is type `{kind}`, which cannot create databases or \
                 tables; a pipeline sink needs `local` or `glue`",
                pipeline.catalog
            )));
        }
        validate_sink_format(&pipeline.format, "pipeline.format")?;
        Ok(())
    }

    fn validate_tables(&self) -> Result<()> {
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        // Collected up front rather than as the loop goes: an AUTO CDC target is allowed to
        // name a source declared *after* it, and checking against a half-filled set would
        // reject a legal ordering.
        let declared: BTreeSet<&str> = self.tables.iter().map(|t| t.name.trim()).collect();
        for table in &self.tables {
            let name = table.name.trim();
            if name.is_empty() {
                return Err(Error::Io("a table has an empty `name:`".into()));
            }
            if name.contains('.') {
                return Err(Error::Io(format!(
                    "table `{name}` must be unqualified — it is materialized into \
                     `{{pipeline.catalog}}.{{pipeline.schema}}`, so a dotted name is ambiguous"
                )));
            }
            if !seen.insert(name) {
                return Err(Error::Io(format!(
                    "table `{name}` is declared more than once"
                )));
            }
            if let Some(format) = &table.format {
                validate_sink_format(format, &format!("table `{name}` format"))?;
            }
            match table.kind() {
                TableKind::Derived => {
                    if table.sql.as_deref().map(str::trim).unwrap_or("").is_empty() {
                        return Err(Error::Io(format!(
                            "table `{name}` has neither a `source:` nor a `sql:` — it is not \
                             defined by anything"
                        )));
                    }
                    if !table.dedup_columns.is_empty() {
                        return Err(Error::Io(format!(
                            "table `{name}` sets `dedup_columns:`, which deduplicates within a \
                             streaming window; a derived table is recomputed in full, so use \
                             SELECT DISTINCT in its `sql:` instead"
                        )));
                    }
                }
                TableKind::AutoCdc => {
                    // `kind()` answers AutoCdc for *any* table carrying an `auto_cdc:` block,
                    // so this arm — not the Derived one — is where a target with no `source:`
                    // arrives. Reporting it here is the difference between an error and a panic.
                    let Some(source) = table.source.as_ref() else {
                        return Err(Error::Io(format!(
                            "table `{name}` sets `auto_cdc:` but has no `source:` — an AUTO CDC \
                             target is fed by a streaming source, so it must declare one"
                        )));
                    };
                    if source.format.trim().is_empty() {
                        return Err(Error::Io(format!(
                            "table `{name}` has a `source:` with no `format:`"
                        )));
                    }
                    validate_dedup_watermark(name, source, &table.dedup_columns)?;
                    let Some(cdc) = table.auto_cdc.as_ref() else {
                        return Err(Error::Io(format!(
                            "table `{name}` is an AUTO CDC table with no `auto_cdc:` options"
                        )));
                    };
                    crate::auto_cdc::validate(cdc, name)?;
                    // `auto_cdc.source` is what puts the dependency edge in the graph. A name
                    // that matches nothing produces no edge and no error, so the merge just runs
                    // in an arbitrary order against whatever the bronze table happens to hold —
                    // the Connect surface rejects the same typo outright.
                    let cdc_source = cdc.source.trim();
                    if cdc_source == name {
                        return Err(Error::Io(format!(
                            "table `{name}` declares itself as its own `auto_cdc.source:`"
                        )));
                    }
                    let Some(upstream) = self.tables.iter().find(|t| t.name.trim() == cdc_source)
                    else {
                        return Err(Error::Io(format!(
                            "table `{name}` sets `auto_cdc.source: {cdc_source}`, which is not a \
                             table declared in `tables:` (declared: {})",
                            declared.iter().copied().collect::<Vec<_>>().join(", ")
                        )));
                    };
                    // An AUTO CDC target re-ingests the source's stream rather than reading its
                    // table, so its `source:`/`sql:`/`expect:` are a *copy* of the source's. Let
                    // them drift and the target quietly merges a different projection — or a
                    // different set of rows — than the table it claims to follow. The SDP SQL
                    // surface copies them outright; here they are written twice, so check.
                    if table.source != upstream.source {
                        return Err(Error::Io(format!(
                            "table `{name}` has a different `source:` from its \
                             `auto_cdc.source: {cdc_source}` — an AUTO CDC target re-reads the \
                             same stream, so the two must be identical"
                        )));
                    }
                    let sql_of = |t: &crate::TableConfig| {
                        t.sql.as_deref().map(str::trim).unwrap_or("").to_string()
                    };
                    if sql_of(table) != sql_of(upstream) {
                        return Err(Error::Io(format!(
                            "table `{name}` has a different `sql:` from its \
                             `auto_cdc.source: {cdc_source}` — the merge would run over a \
                             different projection than `{cdc_source}` holds"
                        )));
                    }
                    if table.expect != upstream.expect {
                        return Err(Error::Io(format!(
                            "table `{name}` has different `expect:` entries from its \
                             `auto_cdc.source: {cdc_source}` — the CDC target would merge rows \
                             `{cdc_source}` dropped, or drop rows it kept"
                        )));
                    }
                    if table.dedup_columns != upstream.dedup_columns {
                        return Err(Error::Io(format!(
                            "table `{name}` has different `dedup_columns:` from its \
                             `auto_cdc.source: {cdc_source}` — both read the same stream, so \
                             both must deduplicate it the same way"
                        )));
                    }
                }
                TableKind::Streaming => {
                    let source = table.source.as_ref().expect("streaming implies a source");
                    if source.format.trim().is_empty() {
                        return Err(Error::Io(format!(
                            "table `{name}` has a `source:` with no `format:`"
                        )));
                    }
                    validate_dedup_watermark(name, source, &table.dedup_columns)?;
                }
            }
            for (label, expectation) in &table.expect {
                if expectation.check.trim().is_empty() {
                    return Err(Error::Io(format!(
                        "table `{name}` expectation `{label}` has an empty `check:`"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Reject a dependency cycle among derived tables.
    ///
    /// This is a *lexical* pre-check over names that literally appear in each table's SQL —
    /// enough to catch `a -> b -> a` at load with no engine. The authoritative edge set comes
    /// from resolving table references through the SQL parser at pipeline startup; this exists
    /// so an obviously circular config fails in milliseconds rather than after connecting to a
    /// broker.
    fn validate_dag(&self) -> Result<()> {
        let declared: BTreeSet<&str> = self.tables.iter().map(|t| t.name.trim()).collect();
        let mut edges: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for table in &self.tables {
            let name = table.name.trim();
            let Some(sql) = table.sql.as_deref() else {
                edges.entry(name).or_default();
                continue;
            };
            let mut deps: Vec<&str> = Vec::new();
            for candidate in &declared {
                if *candidate == name {
                    continue;
                }
                if mentions_identifier(sql, candidate) {
                    deps.push(candidate);
                }
            }
            edges.insert(name, deps);
        }
        if let Some(cycle) = find_cycle(&edges) {
            return Err(Error::Io(format!(
                "the table graph has a cycle: {}",
                cycle.join(" -> ")
            )));
        }
        Ok(())
    }
}

fn declared_names(config: &OxidantConfig) -> String {
    if config.catalogs.is_empty() {
        return "none".to_string();
    }
    config
        .catalogs
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_sink_format(format: &str, label: &str) -> Result<()> {
    let normalized = format.trim().to_ascii_lowercase();
    if WRITABLE_SINK_FORMATS.contains(&normalized.as_str()) {
        return Ok(());
    }
    // Iceberg gets its own message: it is readable and it *is* published over Delta tables,
    // so "unknown format" would be actively misleading.
    if normalized == "iceberg" {
        return Err(Error::Io(format!(
            "{label}: `iceberg` is not a sink format. Write `delta` with `iceberg_compat: true` \
             — Iceberg metadata is published over the same Parquet files, so the table is \
             readable by both Delta and Iceberg engines."
        )));
    }
    Err(Error::Io(format!(
        "{label}: unwritable format `{format}` (expected one of: {})",
        WRITABLE_SINK_FORMATS.join(", ")
    )))
}

/// Whether `sql` references `name` as a standalone identifier.
///
/// Word-boundary matched so `orders` does not match inside `orders_silver` or `my_orders`.
/// Backticks and double quotes count as boundaries because a quoted identifier is still a
/// reference to the table.
fn mentions_identifier(sql: &str, name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let is_ident_char = |c: char| c.is_alphanumeric() || c == '_';
    let mut haystack = sql;
    let mut base = 0usize;
    while let Some(offset) = haystack.find(name) {
        let start = base + offset;
        let end = start + name.len();
        let before_ok = sql[..start]
            .chars()
            .next_back()
            .map_or(true, |c| !is_ident_char(c));
        let after_ok = sql[end..]
            .chars()
            .next()
            .map_or(true, |c| !is_ident_char(c));
        if before_ok && after_ok {
            return true;
        }
        base = start + 1;
        haystack = &sql[base..];
    }
    false
}

/// Depth-first cycle search returning the cycle path, so the error can name it.
fn find_cycle<'a>(edges: &BTreeMap<&'a str, Vec<&'a str>>) -> Option<Vec<&'a str>> {
    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        Visiting,
        Done,
    }
    fn walk<'a>(
        node: &'a str,
        edges: &BTreeMap<&'a str, Vec<&'a str>>,
        marks: &mut BTreeMap<&'a str, Mark>,
        stack: &mut Vec<&'a str>,
    ) -> Option<Vec<&'a str>> {
        match marks.get(node) {
            Some(Mark::Done) => return None,
            Some(Mark::Visiting) => {
                // Report the cycle from where it was entered, closing the loop, so the
                // message reads `a -> b -> a` rather than an arbitrary suffix.
                let start = stack.iter().position(|n| *n == node).unwrap_or(0);
                let mut cycle: Vec<&str> = stack[start..].to_vec();
                cycle.push(node);
                return Some(cycle);
            }
            None => {}
        }
        marks.insert(node, Mark::Visiting);
        stack.push(node);
        for dep in edges.get(node).map(Vec::as_slice).unwrap_or(&[]) {
            if let Some(cycle) = walk(dep, edges, marks, stack) {
                return Some(cycle);
            }
        }
        stack.pop();
        marks.insert(node, Mark::Done);
        None
    }

    let mut marks: BTreeMap<&str, Mark> = BTreeMap::new();
    for node in edges.keys() {
        let mut stack = Vec::new();
        if let Some(cycle) = walk(node, edges, &mut marks, &mut stack) {
            return Some(cycle);
        }
    }
    None
}

/// Deduplicating means remembering keys, and remembering them forever is not an option — so
/// there has to be a rule for forgetting. The watermark is that rule: it is the only thing that
/// can say a key is safe to drop. A count-based bound cannot, which is why an earlier version
/// silently started re-admitting duplicates once it hit one.
fn validate_dedup_watermark(
    name: &str,
    source: &SourceConfig,
    dedup_columns: &[String],
) -> Result<()> {
    if !dedup_columns.is_empty()
        && !source.options.contains_key("eventTimeColumn")
        && !source.options.contains_key("watermarkColumn")
    {
        return Err(Error::Io(format!(
            "table `{name}` sets `dedup_columns:` but no watermark. Deduplication has to \
             remember every key it has seen, and only a watermark can say when one is safe to \
             forget — add `eventTimeColumn` (and optionally `delayMs`) to the source's \
             `options:`"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_err(yaml: &str) -> String {
        OxidantConfig::parse(yaml)
            .expect_err("expected a validation failure")
            .to_string()
    }

    const LOCAL_CATALOG: &str = r#"
catalogs:
  local:
    type: local
    warehouse: /srv/warehouse
"#;

    fn with_pipeline(tables: &str) -> String {
        format!(
            "{LOCAL_CATALOG}
pipeline:
  name: test
  catalog: local
  schema: live
  checkpoints: /srv/ckpt
tables:
{tables}"
        )
    }

    /// A bronze table plus an AUTO CDC target over it, with `extra` spliced into the target.
    fn cdc_yaml(extra: &str) -> String {
        with_pipeline(&format!(
            r#"  - name: customers_bronze
    source:
      format: kafka
      options:
        subscribe: customers
  - name: customers_scd1
    source:
      format: kafka
      options:
        subscribe: customers
    auto_cdc:
      source: customers_bronze
      keys: [id]
      sequence_by: seq
{extra}"#
        ))
    }

    #[test]
    fn an_auto_cdc_table_without_a_source_errors_instead_of_panicking() {
        // `TableConfig::kind()` answers AutoCdc for any table with an `auto_cdc:` block, so this
        // config lands in the AutoCdc arm, not the Derived one — it used to unwrap its way to a
        // panic there.
        let err = parse_err(&with_pipeline(
            r#"  - name: customers_bronze
    source:
      format: kafka
      options:
        subscribe: customers
  - name: customers_scd1
    auto_cdc:
      source: customers_bronze
      keys: [id]
      sequence_by: seq
"#,
        ));
        assert!(err.contains("no `source:`"), "got: {err}");
        assert!(err.contains("customers_scd1"), "got: {err}");
    }

    #[test]
    fn an_auto_cdc_source_must_name_a_declared_table() {
        // A typo here produces no dependency edge, so the merge would silently run against
        // whatever the bronze table happened to hold at the time.
        let err = parse_err(&with_pipeline(
            r#"  - name: customers_bronze
    source:
      format: kafka
      options:
        subscribe: customers
  - name: customers_scd1
    source:
      format: kafka
      options:
        subscribe: customers
    auto_cdc:
      source: customers_bronz
      keys: [id]
      sequence_by: seq
"#,
        ));
        assert!(err.contains("customers_bronz"), "got: {err}");
        assert!(err.contains("not a table declared"), "got: {err}");

        let err = parse_err(&cdc_yaml("").replace(
            "source: customers_bronze\n      keys",
            "source: customers_scd1\n      keys",
        ));
        assert!(err.contains("its own `auto_cdc.source:`"), "got: {err}");
    }

    #[test]
    fn an_auto_cdc_target_may_precede_the_source_it_names() {
        // The source is allowed to be declared later in the file; validation collects every
        // declared name before checking.
        let config = OxidantConfig::parse(&with_pipeline(
            r#"  - name: customers_scd1
    source:
      format: kafka
      options:
        subscribe: customers
    auto_cdc:
      source: customers_bronze
      keys: [id]
      sequence_by: seq
  - name: customers_bronze
    source:
      format: kafka
      options:
        subscribe: customers
"#,
        ));
        assert!(config.is_ok(), "got: {config:?}");
    }

    #[test]
    fn an_auto_cdc_expression_key_is_rejected_at_load() {
        let err = parse_err(&cdc_yaml("").replace("keys: [id]", "keys: [\"lower(id)\"]"));
        assert!(err.contains("plain column name"), "got: {err}");
    }

    #[test]
    fn an_auto_cdc_target_must_mirror_the_source_it_follows() {
        // The target re-ingests the source's stream rather than reading its table, so the two
        // declarations are a copy of one another. Drift means merging a different projection —
        // or a different set of rows — than the table it claims to follow.
        let err = parse_err(&cdc_yaml("").replace(
            "        subscribe: customers
    auto_cdc",
            "        subscribe: other
    auto_cdc",
        ));
        assert!(err.contains("different `source:`"), "got: {err}");

        let drifted_sql = cdc_yaml(
            "    sql: SELECT id, seq FROM stream
",
        );
        let err = parse_err(&drifted_sql);
        assert!(err.contains("different `sql:`"), "got: {err}");

        let drifted_expect = cdc_yaml(
            "    expect:
      positive:
        check: id > 0
        action: drop
",
        );
        let err = parse_err(&drifted_expect);
        assert!(err.contains("different `expect:`"), "got: {err}");
    }

    #[test]
    fn an_auto_cdc_table_needs_a_watermark_to_dedup() {
        let err = parse_err(&cdc_yaml("    dedup_columns: [id]\n"));
        assert!(err.contains("no watermark"), "got: {err}");
    }

    #[test]
    fn identifier_matching_respects_word_boundaries() {
        assert!(mentions_identifier("SELECT * FROM orders", "orders"));
        assert!(mentions_identifier("SELECT * FROM `orders`", "orders"));
        assert!(mentions_identifier(
            "SELECT * FROM orders WHERE x",
            "orders"
        ));
        // The bug this guards: a substring match would make `orders_silver` depend on
        // `orders`, inventing an edge and possibly a cycle that is not there.
        assert!(!mentions_identifier(
            "SELECT * FROM orders_silver",
            "orders"
        ));
        assert!(!mentions_identifier("SELECT * FROM my_orders", "orders"));
    }

    #[test]
    fn a_cycle_is_rejected_and_named() {
        let err = parse_err(&with_pipeline(
            r#"  - name: a
    sql: SELECT * FROM b
  - name: b
    sql: SELECT * FROM a
"#,
        ));
        assert!(err.contains("cycle"), "got: {err}");
        assert!(err.contains("a") && err.contains("b"), "got: {err}");
    }

    #[test]
    fn a_self_referencing_table_is_not_a_cycle() {
        // A derived table selecting from a *catalog* table of the same name is legitimate:
        // the edge set only covers names declared in this pipeline.
        let config = OxidantConfig::parse(&with_pipeline(
            r#"  - name: orders
    sql: SELECT * FROM raw.orders
"#,
        ));
        assert!(config.is_ok(), "got: {config:?}");
    }

    #[test]
    fn a_valid_three_stage_dag_passes() {
        let config = OxidantConfig::parse(&with_pipeline(
            r#"  - name: bronze
    source:
      format: kafka
      options:
        kafka.bootstrap.servers: b:9092
        subscribe: t
  - name: silver
    sql: SELECT * FROM bronze WHERE amount > 0
  - name: gold
    sql: SELECT sum(amount) AS revenue FROM silver
"#,
        ))
        .expect("valid DAG");
        assert_eq!(config.tables.len(), 3);
        assert_eq!(config.tables[0].kind(), TableKind::Streaming);
        assert_eq!(config.tables[2].kind(), TableKind::Derived);
    }

    #[test]
    fn duplicate_table_names_are_rejected() {
        let err = parse_err(&with_pipeline(
            r#"  - name: a
    sql: SELECT 1
  - name: a
    sql: SELECT 2
"#,
        ));
        assert!(err.contains("declared more than once"), "got: {err}");
    }

    #[test]
    fn a_table_defined_by_nothing_is_rejected() {
        let err = parse_err(&with_pipeline("  - name: orphan\n"));
        assert!(
            err.contains("neither a `source:` nor a `sql:`"),
            "got: {err}"
        );
    }

    #[test]
    fn a_pipeline_pointed_at_a_read_only_catalog_fails_at_load() {
        // The failure this prevents happens mid-run today, after the source has been read.
        let err = parse_err(
            r#"
catalogs:
  hms:
    type: hive
    uri: thrift://hms:9083
pipeline:
  name: test
  catalog: hms
  schema: live
  checkpoints: /srv/ckpt
tables:
  - name: a
    sql: SELECT 1
"#,
        );
        assert!(err.contains("cannot create databases"), "got: {err}");
    }

    #[test]
    fn an_undeclared_pipeline_catalog_is_rejected() {
        let err = parse_err(
            r#"
pipeline:
  name: test
  catalog: nope
  schema: live
  checkpoints: /srv/ckpt
tables:
  - name: a
    sql: SELECT 1
"#,
        );
        assert!(err.contains("not declared in `catalogs:`"), "got: {err}");
    }

    #[test]
    fn a_counted_expectation_on_a_streaming_table_is_accepted() {
        // These used to be rejected: `warn` and `fail` need a violation *count*, and the
        // micro-batch loop had no hook for one. It has one now, and the countable unit is the
        // micro-batch — so the check fires per batch, and a `fail` aborts that batch before it
        // reaches the sink rather than being silently inert.
        for action in ["warn", "fail"] {
            OxidantConfig::parse(&format!(
                r#"
catalogs:
  local: {{ type: local, warehouse: /srv/w }}
pipeline:
  name: p
  catalog: local
  schema: live
  checkpoints: /srv/ckpt
tables:
  - name: bronze
    source:
      format: kafka
      options: {{ subscribe: orders }}
    expect:
      positive: {{ check: "amount > 0", action: {action} }}
"#
            ))
            .unwrap_or_else(|e| panic!("`{action}` on a streaming table should parse: {e}"));
        }
    }

    #[test]
    fn dedup_without_a_watermark_is_rejected() {
        // Deduplication has to remember keys, and only a watermark can say when one is safe to
        // forget. Accepting this used to mean an in-memory set that cleared itself at an
        // arbitrary size — so a high-cardinality stream started re-admitting duplicates
        // part-way through a run, with no error.
        let err = OxidantConfig::parse(
            r#"
catalogs:
  local: { type: local, warehouse: /srv/w }
pipeline:
  name: p
  catalog: local
  schema: live
  checkpoints: /srv/ckpt
tables:
  - name: bronze
    source:
      format: kafka
      options: { subscribe: orders }
    dedup_columns: [order_id]
"#,
        )
        .expect_err("dedup without a watermark must be rejected");
        let message = err.to_string();
        assert!(message.contains("bronze"), "name the table: {message}");
        assert!(
            message.contains("eventTimeColumn"),
            "say what to add: {message}"
        );
    }

    #[test]
    fn dedup_with_a_watermark_is_accepted() {
        OxidantConfig::parse(
            r#"
catalogs:
  local: { type: local, warehouse: /srv/w }
pipeline:
  name: p
  catalog: local
  schema: live
  checkpoints: /srv/ckpt
tables:
  - name: bronze
    source:
      format: kafka
      options: { subscribe: orders, eventTimeColumn: timestamp, delayMs: "60000" }
    dedup_columns: [order_id]
"#,
        )
        .expect("a watermarked dedup is exactly what is supported");
    }

    #[test]
    fn a_drop_expectation_on_a_streaming_table_is_accepted() {
        // `drop` composes into the streaming query's own SQL, so it genuinely works there.
        OxidantConfig::parse(
            r#"
catalogs:
  local: { type: local, warehouse: /srv/w }
pipeline:
  name: p
  catalog: local
  schema: live
  checkpoints: /srv/ckpt
tables:
  - name: bronze
    source:
      format: kafka
      options: { subscribe: orders }
    expect:
      positive: { check: "amount > 0", action: drop }
"#,
        )
        .expect("a drop expectation on a streaming table is supported");
    }

    #[test]
    fn iceberg_as_a_sink_format_explains_the_alternative() {
        let err = parse_err(&format!(
            "{LOCAL_CATALOG}
pipeline:
  name: test
  catalog: local
  schema: live
  checkpoints: /srv/ckpt
  format: iceberg
tables:
  - name: a
    sql: SELECT 1
"
        ));
        assert!(err.contains("iceberg_compat"), "got: {err}");
    }

    #[test]
    fn a_local_catalog_without_a_warehouse_is_rejected() {
        let err = parse_err("catalogs:\n  local:\n    type: local\n");
        assert!(err.contains("warehouse"), "got: {err}");
    }

    #[test]
    fn tables_on_a_non_local_catalog_are_rejected() {
        let err = parse_err(
            r#"
catalogs:
  glue:
    type: glue
    tables:
      raw.t: { format: parquet, location: s3://b/t/ }
"#,
        );
        assert!(
            err.contains("only meaningful on a `local` catalog"),
            "got: {err}"
        );
    }

    #[test]
    fn an_unreadable_table_format_is_rejected() {
        let err = parse_err(
            r#"
catalogs:
  local:
    type: local
    warehouse: /srv/w
    tables:
      raw.t: { format: orc, location: /srv/data/t/ }
"#,
        );
        assert!(err.contains("orc"), "got: {err}");
    }

    #[test]
    fn tables_without_a_pipeline_section_are_rejected() {
        let err = parse_err(&format!(
            "{LOCAL_CATALOG}tables:\n  - name: a\n    sql: SELECT 1\n"
        ));
        assert!(err.contains("no `pipeline:` section"), "got: {err}");
    }

    #[test]
    fn a_qualified_table_name_is_rejected() {
        let err = parse_err(&with_pipeline("  - name: live.a\n    sql: SELECT 1\n"));
        assert!(err.contains("must be unqualified"), "got: {err}");
    }
}
