//! The table dependency graph.
//!
//! Edges come from resolving each derived table's SQL to the table references it actually
//! makes, using DataFusion's own parser. That matters more than it might look: a regex or a
//! substring scan would invent an edge for `orders` inside `orders_archive`, and would treat a
//! CTE (`WITH orders AS (...)`) as a dependency on a table of that name. The parser knows the
//! difference.
//!
//! Ordering is a depth-first topological sort. A cycle is reported with the path that closes it
//! rather than a bare "cycle detected", because in a fifteen-table pipeline the message is the
//! only practical way to find which tables to look at.

use std::collections::{BTreeMap, BTreeSet};

use oxidant_common::{Error, Result};
use oxidant_config::TableConfig;

/// One node: a declared table plus the declared tables it reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub name: String,
    /// Declared tables this one reads.
    pub depends_on: Vec<String>,
    /// True when the table also reads something the pipeline does not build.
    ///
    /// Load-bearing for skipping idle work: a table whose inputs are all built here has not
    /// changed if none of them changed this pass. One that reads an outside lake table has no
    /// such guarantee — that table can be rewritten by anything — so it is always recomputed.
    /// Getting this backwards would serve stale numbers indefinitely, and silently.
    pub reads_outside_pipeline: bool,
}

/// The resolved graph, in an order where every table follows its dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Graph {
    pub order: Vec<Node>,
}

impl Graph {
    /// Build the graph from the declared tables.
    pub fn build(tables: &[TableConfig]) -> Result<Self> {
        let declared: BTreeSet<&str> = tables.iter().map(|t| t.name.trim()).collect();
        let mut edges: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut outside: BTreeMap<String, bool> = BTreeMap::new();
        for table in tables {
            let name = table.name.trim().to_string();
            let mut deps: Vec<String> = Vec::new();
            let mut reads_outside = false;
            if let Some(sql) = table.sql.as_deref() {
                for reference in table_references(sql)? {
                    // Only *declared* names are edges. Everything else is a catalog table the
                    // pipeline reads but does not build — exactly what a bronze table over an
                    // existing lake table looks like.
                    //
                    // Both the full reference and its last segment are checked so a
                    // fully-qualified `local.live.bronze` still matches the declared `bronze`.
                    let short = reference.rsplit('.').next().unwrap_or(&reference);
                    let mut matched = reference == name || short == name;
                    for candidate in [reference.as_str(), short] {
                        if candidate != name && declared.contains(candidate) {
                            matched = true;
                            if !deps.iter().any(|d| d == candidate) {
                                deps.push(candidate.to_string());
                            }
                        }
                    }
                    if !matched {
                        reads_outside = true;
                    }
                }
            }
            outside.insert(name.clone(), reads_outside);
            edges.insert(name, deps);
        }
        let mut order = topological_order(tables, &edges)?;
        for node in &mut order {
            node.reads_outside_pipeline = outside.get(&node.name).copied().unwrap_or(false);
        }
        Ok(Self { order })
    }

    /// The tables to update, in order, restricted to `wanted` and everything they depend on.
    ///
    /// Ancestors are included rather than assumed fresh: running `--table gold` against stale
    /// silver would report a successful update of numbers computed from old data.
    pub fn subgraph(&self, wanted: &[String]) -> Result<Vec<Node>> {
        if wanted.is_empty() {
            return Ok(self.order.clone());
        }
        let known: BTreeSet<&str> = self.order.iter().map(|n| n.name.as_str()).collect();
        for name in wanted {
            if !known.contains(name.as_str()) {
                return Err(Error::Io(format!(
                    "`--table {name}` is not a declared table (declared: {})",
                    self.order
                        .iter()
                        .map(|n| n.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
        let by_name: BTreeMap<&str, &Node> =
            self.order.iter().map(|n| (n.name.as_str(), n)).collect();
        let mut needed: BTreeSet<String> = BTreeSet::new();
        let mut stack: Vec<String> = wanted.to_vec();
        while let Some(name) = stack.pop() {
            if !needed.insert(name.clone()) {
                continue;
            }
            if let Some(node) = by_name.get(name.as_str()) {
                stack.extend(node.depends_on.iter().cloned());
            }
        }
        // Filter the full order rather than rebuilding one, so a subgraph keeps the same
        // relative ordering the whole pipeline uses.
        Ok(self
            .order
            .iter()
            .filter(|n| needed.contains(&n.name))
            .cloned()
            .collect())
    }
}

/// The table names a SQL statement reads, via DataFusion's own parser.
///
/// CTE names are excluded: `resolve_table_references` reports them separately, and a
/// `WITH orders AS (...)` is a local definition, not a read of a table called `orders`.
fn table_references(sql: &str) -> Result<Vec<String>> {
    use oxidant_loom::datafusion::sql::parser::DFParser;
    use oxidant_loom::datafusion::sql::resolve::resolve_table_references;

    let statements = DFParser::parse_sql(sql)
        .map_err(|e| Error::Plan(format!("could not parse table SQL: {e}")))?;
    let mut out = Vec::new();
    for statement in &statements {
        let (references, _ctes) = resolve_table_references(statement, true)
            .map_err(|e| Error::Plan(format!("could not resolve table references: {e}")))?;
        for reference in references {
            let name = reference.to_string();
            if !out.contains(&name) {
                out.push(name);
            }
        }
    }
    Ok(out)
}

/// Depth-first topological sort, reporting the cycle path when there is one.
fn topological_order(
    tables: &[TableConfig],
    edges: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<Node>> {
    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        Visiting,
        Done,
    }

    fn visit(
        name: &str,
        edges: &BTreeMap<String, Vec<String>>,
        marks: &mut BTreeMap<String, Mark>,
        stack: &mut Vec<String>,
        out: &mut Vec<String>,
    ) -> Result<()> {
        match marks.get(name) {
            Some(Mark::Done) => return Ok(()),
            Some(Mark::Visiting) => {
                let start = stack.iter().position(|n| n == name).unwrap_or(0);
                let mut cycle: Vec<String> = stack[start..].to_vec();
                cycle.push(name.to_string());
                return Err(Error::Io(format!(
                    "the table graph has a cycle: {}",
                    cycle.join(" -> ")
                )));
            }
            None => {}
        }
        marks.insert(name.to_string(), Mark::Visiting);
        stack.push(name.to_string());
        for dep in edges.get(name).map(Vec::as_slice).unwrap_or(&[]) {
            visit(dep, edges, marks, stack, out)?;
        }
        stack.pop();
        marks.insert(name.to_string(), Mark::Done);
        out.push(name.to_string());
        Ok(())
    }

    let mut marks = BTreeMap::new();
    let mut order = Vec::new();
    // Seed in declaration order so two independent tables keep the order the file lists them
    // in — stable and explainable, rather than whatever a map happens to iterate in.
    for table in tables {
        let mut stack = Vec::new();
        visit(table.name.trim(), edges, &mut marks, &mut stack, &mut order)?;
    }
    Ok(order
        .into_iter()
        .map(|name| Node {
            depends_on: edges.get(&name).cloned().unwrap_or_default(),
            name,
            reads_outside_pipeline: false,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_config::OxidantConfig;

    fn tables(yaml: &str) -> Vec<TableConfig> {
        let config = format!(
            "catalogs:
  local:
    type: local
    warehouse: /srv/w
pipeline:
  name: p
  catalog: local
  schema: live
  checkpoints: /srv/ckpt
tables:
{yaml}"
        );
        OxidantConfig::parse(&config).expect("config parses").tables
    }

    /// Build a derived table directly, bypassing config parsing.
    ///
    /// Needed for the cycle case: `OxidantConfig::parse` runs its own lexical cycle check and
    /// rejects the file before a graph could ever be built from it. That earlier check is the
    /// right layering — a user gets the error in milliseconds — but the graph's own detection
    /// still has to be exercised, since it is the authoritative one and works from resolved
    /// references rather than name matching.
    fn derived(name: &str, sql: &str) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            source: None,
            sql: Some(sql.to_string()),
            sql_by_name: false,
            append_flows: vec![],
            output_schema: None,
            partition_by: vec![],
            format: None,
            iceberg_compat: None,
            iceberg_table_suffix: None,
            checkpoint_interval: None,
            dedup_columns: vec![],
            expect: Default::default(),
            comment: None,
        }
    }

    fn names(graph: &Graph) -> Vec<&str> {
        graph.order.iter().map(|n| n.name.as_str()).collect()
    }

    #[test]
    fn dependencies_order_bronze_before_silver_before_gold() {
        // Declared deliberately out of order — the sort, not the file, decides.
        let graph = Graph::build(&tables(
            "  - name: gold
    sql: SELECT sum(amount) AS revenue FROM silver
  - name: bronze
    source: { format: kafka, options: { oxidant.spool.dir: /srv/s } }
  - name: silver
    sql: SELECT * FROM bronze WHERE amount > 0
",
        ))
        .expect("builds");
        assert_eq!(names(&graph), vec!["bronze", "silver", "gold"]);
    }

    #[test]
    fn a_cte_is_not_mistaken_for_a_dependency() {
        // `WITH bronze AS (...)` defines a local name. Treating it as an edge would order the
        // pipeline around a table this query never reads.
        let graph = Graph::build(&tables(
            "  - name: bronze
    source: { format: kafka, options: { oxidant.spool.dir: /srv/s } }
  - name: standalone
    sql: WITH bronze AS (SELECT 1 AS x) SELECT * FROM bronze
",
        ))
        .expect("builds");
        let standalone = graph
            .order
            .iter()
            .find(|n| n.name == "standalone")
            .expect("present");
        assert!(
            standalone.depends_on.is_empty(),
            "a CTE must not become an edge, got {:?}",
            standalone.depends_on
        );
    }

    #[test]
    fn a_similarly_named_table_is_not_a_dependency() {
        // The substring trap: `orders` must not be read out of `orders_archive_2024`.
        let graph = Graph::build(&tables(
            "  - name: orders
    source: { format: kafka, options: { oxidant.spool.dir: /srv/s } }
  - name: unrelated
    sql: SELECT * FROM orders_archive_2024
",
        ))
        .expect("builds");
        let unrelated = graph
            .order
            .iter()
            .find(|n| n.name == "unrelated")
            .expect("present");
        assert!(
            unrelated.depends_on.is_empty(),
            "expected no dependency, got {:?}",
            unrelated.depends_on
        );
    }

    #[test]
    fn a_qualified_reference_to_a_declared_table_still_forms_an_edge() {
        // Writing the pipeline's own table fully-qualified is natural and must not silently
        // produce an unordered graph.
        let graph = Graph::build(&tables(
            "  - name: bronze
    source: { format: kafka, options: { oxidant.spool.dir: /srv/s } }
  - name: silver
    sql: SELECT * FROM local.live.bronze
",
        ))
        .expect("builds");
        assert_eq!(names(&graph), vec!["bronze", "silver"]);
        let silver = graph.order.iter().find(|n| n.name == "silver").unwrap();
        assert_eq!(silver.depends_on, vec!["bronze".to_string()]);
    }

    #[test]
    fn a_cycle_names_the_path_that_closes_it() {
        let err = Graph::build(&[
            derived("a", "SELECT * FROM b"),
            derived("b", "SELECT * FROM c"),
            derived("c", "SELECT * FROM a"),
        ])
        .expect_err("cycle");
        let message = err.to_string();
        assert!(message.contains("cycle"), "got: {message}");
        for name in ["a", "b", "c"] {
            assert!(
                message.contains(name),
                "the cycle path should name `{name}`: {message}"
            );
        }
    }

    #[test]
    fn a_reference_to_an_undeclared_catalog_table_is_not_an_edge() {
        // Reading a table the pipeline does not build is ordinary — that is what a bronze
        // table over an existing lake table looks like.
        let graph = Graph::build(&tables(
            "  - name: enriched
    sql: SELECT * FROM local.raw.customers
",
        ))
        .expect("builds");
        assert!(graph.order[0].depends_on.is_empty());
    }

    #[test]
    fn a_subgraph_includes_the_ancestors_of_what_was_asked_for() {
        // Refreshing gold from stale silver would report success over old numbers.
        let graph = Graph::build(&tables(
            "  - name: bronze
    source: { format: kafka, options: { oxidant.spool.dir: /srv/s } }
  - name: silver
    sql: SELECT * FROM bronze
  - name: gold
    sql: SELECT count(*) AS n FROM silver
  - name: unrelated
    sql: SELECT 1 AS x
",
        ))
        .expect("builds");
        let subgraph = graph.subgraph(&["gold".to_string()]).expect("subgraph");
        let names: Vec<&str> = subgraph.iter().map(|n| n.name.as_str()).collect();
        assert_eq!(names, vec!["bronze", "silver", "gold"]);
        assert!(!names.contains(&"unrelated"));
    }

    #[test]
    fn an_unknown_table_name_is_rejected_with_the_list_of_real_ones() {
        let graph = Graph::build(&tables("  - name: a\n    sql: SELECT 1\n")).expect("builds");
        let err = graph
            .subgraph(&["typo".to_string()])
            .expect_err("unknown table");
        assert!(err.to_string().contains("typo"), "got: {err}");
        assert!(err.to_string().contains("declared"), "got: {err}");
    }

    #[test]
    fn unparseable_sql_fails_the_graph_rather_than_looking_dependency_free() {
        // Silently zero dependencies would order the table first and run it against nothing.
        let err = Graph::build(&tables(
            "  - name: broken\n    sql: SELECT FROM WHERE ***\n",
        ))
        .expect_err("bad SQL");
        assert!(err.to_string().contains("parse"), "got: {err}");
    }
}
