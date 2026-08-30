//! Function-coverage gap report: Oxidant's live function registry vs. the Databricks SQL
//! builtin-function surface.
//!
//! The Databricks side is [`databricks-functions.json`](../databricks-functions.json) — one entry
//! per distinct function name documented at
//! <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin>, carrying the
//! manual category it appears under and whether Oxidant intends to implement it (`in_scope`).
//! Overload pages are merged and names come from the rendered signature, not the URL slug — see
//! `scripts/scrape-databricks-functions.py`, which regenerates the catalog.
//! Entries that are out of scope name *why* (`excluded_reason`): operator syntax, Databricks
//! control-plane dependencies, or the Apache DataSketches binary formats.
//!
//! The Oxidant side is [`oxidant_loom::Engine::registered_function_names`] — the same union that
//! backs `SHOW FUNCTIONS`, so this report and the engine can never disagree about what resolves.
//!
//! **Why this exists rather than a grep.** DataFusion generates a large share of its registry
//! through macros (`make_math_unary_udf!`) and `aliases()`, so a source-level search under-counts
//! what is registered by roughly eighty names and correspondingly over-reports the gap. Only the
//! live registry is authoritative.
//!
//! Origin is resolved by differencing the engine's registry against a stock
//! [`SessionContext::new`], which carries DataFusion's default feature set and nothing else:
//! a name present in both is a DataFusion built-in, a name only Oxidant has comes from
//! `crates/oxidant-loom/src/spark_functions/` or the alias tables in `oxidant-loom/src/lib.rs`.

use std::collections::{BTreeMap, BTreeSet};

/// The checked-in Databricks function catalog.
pub const CATALOG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/databricks-functions.json");

/// One entry as it appears in `databricks-functions.json`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CatalogEntry {
    pub name: String,
    #[serde(default)]
    pub categories: Vec<String>,
    pub in_scope: bool,
    #[serde(default)]
    pub excluded_reason: Option<String>,
    #[serde(default)]
    pub excluded_detail: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct Catalog {
    source: String,
    scraped: String,
    functions: Vec<CatalogEntry>,
}

/// Where a registered name comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    /// Present in a stock DataFusion `SessionContext` — a DataFusion built-in (or one of its
    /// own aliases).
    Datafusion,
    /// Only present once Oxidant's Spark layer is registered: a UDF/UDAF in
    /// `crates/oxidant-loom/src/spark_functions/`, or a Spark-name alias from
    /// `register_spark_function_aliases`.
    Oxidant,
}

/// Per-function verdict.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FunctionRow {
    pub name: String,
    pub categories: Vec<String>,
    pub in_scope: bool,
    /// `registered` | `missing` | `out-of-scope`.
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excluded_reason: Option<String>,
}

/// Per-category rollup over in-scope functions only.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CategoryRow {
    pub category: String,
    pub in_scope: usize,
    pub registered: usize,
    pub missing: Vec<String>,
}

/// The whole report.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FunctionsReport {
    pub source: String,
    pub scraped: String,
    /// Every documented Databricks function page.
    pub documented: usize,
    /// Those Oxidant intends to implement.
    pub in_scope: usize,
    /// In-scope names the engine resolves today.
    pub registered: usize,
    /// In-scope names the engine does not resolve.
    pub missing: usize,
    /// Everything the engine resolves, including names Databricks does not document.
    pub engine_registry_size: usize,
    /// Out-of-scope tallies, keyed by `excluded_reason`.
    pub excluded: BTreeMap<String, usize>,
    pub categories: Vec<CategoryRow>,
    pub functions: Vec<FunctionRow>,
}

impl FunctionsReport {
    /// Share of the in-scope surface that resolves today.
    pub fn coverage_pct(&self) -> f64 {
        if self.in_scope == 0 {
            return 0.0;
        }
        self.registered as f64 * 100.0 / self.in_scope as f64
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("serialize functions report") + "\n"
    }

    /// Human-readable matrix, suitable for committing as `docs/databricks-functions.md`.
    pub fn to_markdown(&self) -> String {
        let mut s = String::new();
        s.push_str("# Databricks SQL builtin-function coverage\n\n");
        s.push_str(
            "**Generated — do not edit by hand.** Regenerate with:\n\n\
             ```sh\n\
             cargo build -p oxidant-spark-compat --bin oxidant-parity\n\
             ./target/debug/oxidant-parity functions --markdown > docs/databricks-functions.md\n\
             ```\n\n",
        );
        s.push_str(&format!(
            "Databricks surface scraped {} from <{}>.\n\n",
            self.scraped, self.source
        ));
        s.push_str(
            "Oxidant's side is the live function registry — the same union that answers \
             `SHOW FUNCTIONS` (`Engine::registered_function_names`), so this table cannot drift \
             from what the engine actually resolves. A source-level grep would under-count it: \
             DataFusion generates much of its registry through macros and `aliases()`.\n\n",
        );

        s.push_str("## Headline\n\n");
        s.push_str("| | |\n|---|---:|\n");
        s.push_str(&format!(
            "| Documented Databricks functions | {} |\n",
            self.documented
        ));
        s.push_str(&format!("| In scope for Oxidant | {} |\n", self.in_scope));
        s.push_str(&format!(
            "| **Registered today** | **{} ({:.1}%)** |\n",
            self.registered,
            self.coverage_pct()
        ));
        s.push_str(&format!("| Missing | {} |\n", self.missing));
        s.push_str(&format!(
            "| Engine registry size (incl. non-Databricks names) | {} |\n\n",
            self.engine_registry_size
        ));

        s.push_str("### Out of scope\n\n| Reason | Count |\n|---|---:|\n");
        for (reason, n) in &self.excluded {
            s.push_str(&format!("| {reason} | {n} |\n"));
        }
        s.push('\n');

        s.push_str("## By manual category\n\n");
        s.push_str("| Category | Registered | In scope | Missing |\n|---|---:|---:|---|\n");
        for c in &self.categories {
            let missing = if c.missing.is_empty() {
                "—".to_string()
            } else {
                c.missing
                    .iter()
                    .map(|m| format!("`{m}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            s.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                c.category, c.registered, c.in_scope, missing
            ));
        }
        s.push('\n');

        s.push_str("## Every in-scope function\n\n");
        s.push_str(
            "`origin` is where a registered name comes from: `datafusion` for a \
                    DataFusion built-in, `oxidant` for a Spark UDF in \
                    `crates/oxidant-loom/src/spark_functions/` or a Spark-name alias from \
                    `register_spark_function_aliases`.\n\n",
        );
        s.push_str("| Function | Status | Origin | Category |\n|---|---|---|---|\n");
        for f in self.functions.iter().filter(|f| f.in_scope) {
            let origin = match f.origin {
                Some(Origin::Datafusion) => "datafusion",
                Some(Origin::Oxidant) => "oxidant",
                None => "—",
            };
            let status = if f.status == "registered" {
                "registered"
            } else {
                "**missing**"
            };
            s.push_str(&format!(
                "| `{}` | {} | {} | {} |\n",
                f.name,
                status,
                origin,
                f.categories.join("; ")
            ));
        }
        s
    }
}

/// Build the report: boot an engine, read its registry, diff against the catalog.
pub async fn run() -> FunctionsReport {
    let catalog: Catalog = serde_json::from_str(
        &std::fs::read_to_string(CATALOG_PATH)
            .unwrap_or_else(|e| panic!("read {CATALOG_PATH}: {e}")),
    )
    .expect("parse databricks-functions.json");

    let engine = oxidant_loom::Engine::new();
    let registered: BTreeSet<String> = engine.registered_function_names().into_iter().collect();

    // Stock DataFusion, for origin attribution only.
    let stock = datafusion::prelude::SessionContext::new();
    let stock_names: BTreeSet<String> = {
        let state = stock.state();
        let mut n: BTreeSet<String> = state.scalar_functions().keys().cloned().collect();
        n.extend(state.aggregate_functions().keys().cloned());
        n.extend(state.window_functions().keys().cloned());
        n
    };

    let mut excluded: BTreeMap<String, usize> = BTreeMap::new();
    let mut cats: BTreeMap<String, (usize, usize, Vec<String>)> = BTreeMap::new();
    let mut rows = Vec::with_capacity(catalog.functions.len());
    let (mut in_scope, mut hits) = (0usize, 0usize);

    for e in &catalog.functions {
        if !e.in_scope {
            *excluded
                .entry(e.excluded_reason.clone().unwrap_or_else(|| "other".into()))
                .or_default() += 1;
            rows.push(FunctionRow {
                name: e.name.clone(),
                categories: e.categories.clone(),
                in_scope: false,
                status: "out-of-scope",
                origin: None,
                excluded_reason: e.excluded_reason.clone(),
            });
            continue;
        }
        in_scope += 1;
        let is_registered = registered.contains(&e.name);
        if is_registered {
            hits += 1;
        }
        for c in &e.categories {
            let slot = cats.entry(c.clone()).or_insert((0, 0, Vec::new()));
            slot.0 += 1;
            if is_registered {
                slot.1 += 1;
            } else {
                slot.2.push(e.name.clone());
            }
        }
        rows.push(FunctionRow {
            name: e.name.clone(),
            categories: e.categories.clone(),
            in_scope: true,
            status: if is_registered {
                "registered"
            } else {
                "missing"
            },
            origin: if !is_registered {
                None
            } else if stock_names.contains(&e.name) {
                Some(Origin::Datafusion)
            } else {
                Some(Origin::Oxidant)
            },
            excluded_reason: None,
        });
    }

    let categories = cats
        .into_iter()
        .map(|(category, (in_scope, registered, missing))| CategoryRow {
            category,
            in_scope,
            registered,
            missing,
        })
        .collect();

    FunctionsReport {
        source: catalog.source,
        scraped: catalog.scraped,
        documented: catalog.functions.len(),
        in_scope,
        registered: hits,
        missing: in_scope - hits,
        engine_registry_size: registered.len(),
        excluded,
        categories,
        functions: rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The catalog must parse, cover the whole documented surface, and agree with the scope
    /// boundary the plan committed to.
    #[test]
    fn catalog_parses_and_scopes() {
        let catalog: Catalog =
            serde_json::from_str(&std::fs::read_to_string(CATALOG_PATH).unwrap()).unwrap();
        assert_eq!(
            catalog.functions.len(),
            606,
            "distinct documented function names"
        );
        let in_scope = catalog.functions.iter().filter(|f| f.in_scope).count();
        assert_eq!(in_scope, 440, "in-scope functions");
        // Every excluded entry must say why — an unexplained exclusion is a silent gap.
        for f in catalog.functions.iter().filter(|f| !f.in_scope) {
            assert!(
                f.excluded_reason.is_some() && f.excluded_detail.is_some(),
                "{} is excluded without a reason",
                f.name
            );
        }
        // Every entry must carry at least one manual category.
        for f in &catalog.functions {
            assert!(!f.categories.is_empty(), "{} has no category", f.name);
        }
    }

    /// The report must reflect the live registry, not the catalog's wishes.
    #[tokio::test]
    async fn report_reflects_the_live_registry() {
        let r = run().await;
        assert_eq!(r.documented, 606);
        assert_eq!(r.in_scope, 440);
        assert_eq!(r.registered + r.missing, r.in_scope);
        assert!(r.engine_registry_size > 200, "{}", r.engine_registry_size);

        let by_name = |n: &str| r.functions.iter().find(|f| f.name == n).unwrap().clone();
        // `upper` is a DataFusion built-in; `typeof` only exists because oxidant registers it.
        assert_eq!(by_name("upper").status, "registered");
        assert_eq!(by_name("upper").origin, Some(Origin::Datafusion));
        assert_eq!(by_name("typeof").status, "registered");
        assert_eq!(by_name("typeof").origin, Some(Origin::Oxidant));
        // Out-of-scope entries are never scored.
        assert_eq!(by_name("ai_query").status, "out-of-scope");
        assert!(by_name("ai_query").origin.is_none());
    }
}
