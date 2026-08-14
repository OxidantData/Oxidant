//! Left-deep shuffle-join chains for two or more sharded tables.
//!
//! A single equijoin between exactly two sharded leaf scans reuses the proven two-table path in
//! [`super::stage_planner`] (**inner only**). Longer chains — and LEFT / RIGHT / FULL / LEFT SEMI /
//! LEFT ANTI equijoins — emit pairwise shuffle stages (intermediate consume+produce) with flattened
//! column names (`alias__col`), then partial/final aggregate.
//!
//! ## Outer / semi / anti correctness
//!
//! Both sides of a sharded–sharded equijoin are hash-partitioned on the join key so every key's
//! rows co-locate on one worker. Arrow row-format hashing treats NULL as an ordinary key value
//! (null-key rows are not dropped), so:
//! - **LEFT / RIGHT / FULL OUTER**: null-extension is local to the worker that owns the key;
//!   unmatched rows (including NULL-key rows) never need another worker's partition.
//! - **LEFT SEMI / LEFT ANTI**: matching is presence-only and likewise key-local.
//!
//! After an intermediate **FULL** or **RIGHT** join, unmatched right rows have NULL on the left
//! key; the intermediate projection therefore emits
//! `COALESCE(l.<join_key>, r.<join_key>) AS <left_flat>` so the next stage still hashes on the
//! real key value. `RIGHT SEMI` / `RIGHT ANTI` are rejected — Spark SQL has no such keywords;
//! use `LEFT SEMI` / `LEFT ANTI` (or swap inputs upstream).
//!
//! Replicated dimensions that appear in the chain fold into the next shuffle-join stage as
//! local broadcast joins (always `JOIN`, since the dim is complete on every worker).
//! Replicated dimensions *trailing* the last sharded shuffle boundary (TPC-DS Q72's
//! `LEFT JOIN promotion` / `LEFT JOIN catalog_returns` after `catalog_sales ⋈ inventory`)
//! fold into the final chain stage the same way — matching and null-extension are key-local
//! against a complete right side — with outer joins keeping their `LEFT JOIN` keyword and
//! ON-clause residuals. Shapes where that fold is unsafe (a `Filter` still parked above the
//! chain, a non-INNER/LEFT trailing join, a boundary that null-extends, or a reference the
//! stage's FROM cannot bind) keep the historical rejection instead of planning wrong.
//!
//! ## Semi-join runtime filters (KAN-150)
//!
//! A fold alone filters fact rows only AFTER the shuffle: the leaf scan stages ship every
//! row. So an INNER equijoin to a *filtered* replicated dim additionally injects an
//! `<key> IN (SELECT <dim key> FROM <dim> WHERE <dim filters>)` conjunct into the leaf
//! scan's stage SQL ([`semi_join_leaf_filters`]) — a runtime/semi-join filter crossing the
//! stage boundary as SQL. The conjunct only removes rows the INNER join itself would drop
//! (skipped entirely when the boundary preserves the leaf's side, when the dim is
//! unfiltered, or when its filter is volatile), and on the worker the subquery re-plans to
//! a hash semi join whose DataFusion dynamic filter prunes the fact's parquet scan — fact
//! reduction BEFORE the shuffle write, Trino-style.

use std::collections::{HashMap, HashSet};

use datafusion::logical_expr::{Expr, JoinType, LogicalPlan};
use datafusion::sql::unparser::Unparser;
use oxidant_common::{Error, Result};

use super::stage_planner::{
    base_tables, build_finalize, build_remap, collect_equijoin_keys, column_name,
    distinct_stage_sql, expr_sql, plan_distributed_logical, recombine_stage_sql,
    sanitize_generated_sql, shuffle_join_two_tables, simple_table_scan, sql_contains_volatile,
    AggSpec, DistributedQuery, Peeled, SimpleScan,
};
use crate::driver::{ExchangeMode, StageDef};

/// Plan a left-deep shuffle-join chain (+ grouped aggregation) over `sharded.len() >= 2` tables.
pub(crate) fn plan_shuffle_join_chain(
    p: &Peeled<'_>,
    sharded: &[&str],
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let (leftmost, steps, crossed_filter) = extract_equijoin_chain(&p.agg.input, replicated)?;
    if steps.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: expected at least one equijoin between sharded tables".into(),
        ));
    }

    // Fast path: exactly one *inner* join, both sides sharded leaf scans, no other base tables.
    // Outer / semi / anti use the general chain builder (same shuffle co-location, different JOIN).
    if steps.len() == 1
        && steps[0].join_type == JoinType::Inner
        && sharded.len() == 2
        && base_tables(&p.agg.input).len() == 2
        && leftmost.table().is_some_and(|t| sharded.contains(&t))
        && steps[0].right.table().is_some_and(|t| sharded.contains(&t))
    {
        return shuffle_join_two_tables(p, sharded);
    }

    for step in &steps {
        match &step.right {
            ChainSide::Scan(scan) => {
                let t = scan.table;
                if !sharded.contains(&t) && !replicated.contains(&t) {
                    return Err(Error::Unsupported(format!(
                        "auto-distribute: join chain table `{t}` is neither sharded nor replicated"
                    )));
                }
            }
            // A derived leg shuffles only ever as an INNER equijoin side (the comma-join
            // shapes it exists for); outer / semi / anti boundaries keep the rejection.
            ChainSide::Derived(_) if step.join_type != JoinType::Inner => {
                return Err(Error::Unsupported(
                    "auto-distribute: derived shuffle join side is only supported on INNER joins"
                        .into(),
                ));
            }
            ChainSide::Derived(_) => {}
        }
    }
    if let ChainSide::Scan(scan) = &leftmost {
        if !sharded.contains(&scan.table) {
            return Err(Error::Unsupported(
                "auto-distribute: left-deep shuffle chain requires a sharded leftmost table".into(),
            ));
        }
    }

    ensure_semi_anti_aggs_ok(p, &steps)?;

    build_chain(p, sharded, replicated, leftmost, &steps, crossed_filter)
}

struct ChainStep<'a> {
    right: ChainSide<'a>,
    /// Equijoin key pairs `(left, right)` — one or more for composite keys (KAN-10).
    keys: Vec<(Expr, Expr)>,
    residual_filter: Option<Expr>,
    join_type: JoinType,
}

/// One side of a shuffle-chain boundary: either a plain leaf scan, or a KAN-162 **opaque
/// derived leg** (TPC-DS q54's `my_customers` / q64's `cs_ui` at the all-facts-sharded
/// classification) — a `SubqueryAlias`-wrapped derived subplan scanning at least one sharded
/// table. The derived leg plans recursively via [`plan_distributed_logical`] and materializes
/// as its own sub-DAG, whose output an export stage re-flattens (`alias__col`) and re-hashes
/// by the boundary join key, exactly like a leaf scan's shuffle stage — the pairwise join
/// machinery downstream is unchanged.
enum ChainSide<'a> {
    Scan(SimpleScan<'a>),
    Derived(DerivedLeg<'a>),
}

struct DerivedLeg<'a> {
    /// The `SubqueryAlias` name the chain's keys / projections qualify the leg's columns by.
    alias: &'a str,
    /// The whole chain-side plan (`Filter`-wrapped `SubqueryAlias` included): single-leaf
    /// conjuncts pushed by the comma connector re-apply inside the leg's own subplan.
    plan: &'a LogicalPlan,
    /// The leg's output schema as the chain sees it (alias-qualified).
    schema: datafusion::common::DFSchemaRef,
}

impl<'a> ChainSide<'a> {
    fn alias(&self) -> &'a str {
        match self {
            ChainSide::Scan(s) => scan_alias(s),
            ChainSide::Derived(d) => d.alias,
        }
    }

    /// The base table a scan side reads; a derived leg has no single table.
    fn table(&self) -> Option<&'a str> {
        match self {
            ChainSide::Scan(s) => Some(s.table),
            ChainSide::Derived(_) => None,
        }
    }

    fn as_scan(&self) -> Option<&SimpleScan<'a>> {
        match self {
            ChainSide::Scan(s) => Some(s),
            ChainSide::Derived(_) => None,
        }
    }

    /// Whether this side becomes a shuffled stream (a scan of a sharded table, or a derived
    /// leg — extraction admitted it only because it scans sharded tables).
    fn is_sharded(&self, sharded: &[&str]) -> bool {
        match self {
            ChainSide::Scan(s) => sharded.contains(&s.table),
            ChainSide::Derived(_) => true,
        }
    }
}

/// Admit a non-scan chain side as an opaque derived leg: `Filter` wrappers (the comma
/// connector's pushed single-leaf conjuncts) see through to exactly one `SubqueryAlias` over
/// a derived subplan. `None` for plain scans and for anything without a single alias — the
/// caller then keeps the original `simple_table_scan` rejection.
fn derived_chain_leg(side: &LogicalPlan) -> Option<DerivedLeg<'_>> {
    let mut node = side;
    while let LogicalPlan::Filter(f) = node {
        node = f.input.as_ref();
    }
    let LogicalPlan::SubqueryAlias(a) = node else {
        return None;
    };
    if matches!(a.input.as_ref(), LogicalPlan::TableScan(_)) {
        return None;
    }
    Some(DerivedLeg {
        alias: a.alias.table(),
        plan: side,
        schema: side.schema().clone(),
    })
}

/// Classify one chain side: a plain leaf scan, an opaque derived leg (only when it scans at
/// least one sharded table — replicated-only derived tables keep the original rejection: the
/// broadcast / gather paths own them), or the original [`simple_table_scan`] error unchanged.
fn chain_side<'a>(side: &'a LogicalPlan, replicated: &[&str]) -> Result<ChainSide<'a>> {
    match simple_table_scan(side) {
        Ok(scan) => Ok(ChainSide::Scan(scan)),
        Err(scan_err) => {
            let Some(leg) = derived_chain_leg(side) else {
                return Err(scan_err);
            };
            if !base_tables(leg.plan)
                .iter()
                .any(|t| !replicated.contains(&t.as_str()))
            {
                return Err(scan_err);
            }
            Ok(ChainSide::Derived(leg))
        }
    }
}

fn supported_shuffle_join_type(jt: JoinType) -> Result<()> {
    match jt {
        JoinType::Inner
        | JoinType::Left
        | JoinType::Right
        | JoinType::Full
        | JoinType::LeftSemi
        | JoinType::LeftAnti => Ok(()),
        // Spark SQL / Databricks dialect has LEFT SEMI / LEFT ANTI only. Emitting RIGHT SEMI
        // would pass planning and fail later when workers re-parse stage SQL.
        JoinType::RightSemi | JoinType::RightAnti => Err(Error::Unsupported(
            "auto-distribute: RIGHT SEMI / RIGHT ANTI shuffle joins are not supported \
             (Spark SQL has no RIGHT SEMI/ANTI) — rewrite as LEFT SEMI/ANTI with swapped inputs"
                .into(),
        )),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: shuffle join type `{other}` is not supported"
        ))),
    }
}

fn sql_join_keyword(jt: JoinType) -> Result<&'static str> {
    match jt {
        JoinType::Inner => Ok("JOIN"),
        JoinType::Left => Ok("LEFT JOIN"),
        JoinType::Right => Ok("RIGHT JOIN"),
        JoinType::Full => Ok("FULL OUTER JOIN"),
        JoinType::LeftSemi => Ok("LEFT SEMI JOIN"),
        JoinType::LeftAnti => Ok("LEFT ANTI JOIN"),
        JoinType::RightSemi | JoinType::RightAnti => Err(Error::Unsupported(
            "auto-distribute: RIGHT SEMI / RIGHT ANTI cannot be emitted as stage SQL \
             (not valid Spark SQL)"
                .into(),
        )),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: cannot emit SQL for join type `{other}`"
        ))),
    }
}

/// SEMI / ANTI joins project only the kept side (left for LeftSemi/LeftAnti).
fn projects_right_side(jt: JoinType) -> bool {
    matches!(
        jt,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
    )
}

/// Reject aggregates/projections that need columns from a semi/anti join's dropped side.
fn ensure_semi_anti_aggs_ok(p: &Peeled<'_>, steps: &[ChainStep<'_>]) -> Result<()> {
    let mut dropped_relations = Vec::new();
    for step in steps {
        if matches!(step.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
            if let Some(scan) = step.right.as_scan() {
                dropped_relations.push(scan.table.to_string());
                if let Some(a) = scan.alias {
                    dropped_relations.push(a.to_string());
                }
            }
        }
    }
    if dropped_relations.is_empty() {
        return Ok(());
    }
    let mut refs = Vec::new();
    for e in p.agg.group_expr.iter().chain(p.agg.aggr_expr.iter()) {
        collect_column_relations(e, &mut refs);
    }
    if let Some(proj) = p.projection {
        for e in proj {
            collect_column_relations(e, &mut refs);
        }
    }
    for h in &p.having {
        collect_column_relations(h, &mut refs);
    }
    for r in &refs {
        if dropped_relations.iter().any(|d| d == r) {
            return Err(Error::Unsupported(format!(
                "auto-distribute: aggregate/projection references `{r}` from a SEMI/ANTI \
                 join's dropped side"
            )));
        }
    }
    Ok(())
}

fn collect_column_relations(e: &Expr, out: &mut Vec<String>) {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            if let Some(rel) = &c.relation {
                let t = rel.table().to_string();
                if !out.iter().any(|x| x == &t) {
                    out.push(t);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

/// Flatten the left-deep equijoin chain under `lp` into its leftmost side + one step per
/// join. Also reports whether the walk crossed any `Filter` on the spine above/between the
/// joins: such a predicate is NOT captured in the chain (the steps carry only their own ON /
/// residual filters), so it would be silently dropped from the plan — callers folding
/// trailing joins into the final stage must refuse rather than ship an unfiltered plan
/// ([`super::join_order::distribute_chain_filter`] normally lands those conjuncts on scans
/// and step residuals first). A side that is not a plain scan may still be admitted as an
/// opaque derived leg ([`chain_side`], KAN-162); anything else keeps the original rejection.
fn extract_equijoin_chain<'a>(
    lp: &'a LogicalPlan,
    replicated: &[&str],
) -> Result<(ChainSide<'a>, Vec<ChainStep<'a>>, bool)> {
    fn walk<'a>(
        lp: &'a LogicalPlan,
        replicated: &[&str],
        crossed_filter: &mut bool,
    ) -> Result<(ChainSide<'a>, Vec<ChainStep<'a>>)> {
        match lp {
            LogicalPlan::Projection(p) => walk(p.input.as_ref(), replicated, crossed_filter),
            LogicalPlan::Filter(f) => {
                *crossed_filter = true;
                walk(f.input.as_ref(), replicated, crossed_filter)
            }
            // KAN-11: CTE / subquery aliases wrap otherwise left-deep equijoin trees.
            LogicalPlan::SubqueryAlias(s) => walk(s.input.as_ref(), replicated, crossed_filter),
            LogicalPlan::Sort(s) => walk(s.input.as_ref(), replicated, crossed_filter),
            LogicalPlan::Limit(l) => walk(l.input.as_ref(), replicated, crossed_filter),
            LogicalPlan::Distinct(d) => walk(d.input().as_ref(), replicated, crossed_filter),
            LogicalPlan::Join(j) => {
                supported_shuffle_join_type(j.join_type)?;
                let (keys, residual_filter) = equijoin_keys(j)?;
                let right = chain_side(j.right.as_ref(), replicated)?;
                match chain_side(j.left.as_ref(), replicated) {
                    Ok(leftmost) => Ok((
                        leftmost,
                        vec![ChainStep {
                            right,
                            keys,
                            residual_filter,
                            join_type: j.join_type,
                        }],
                    )),
                    Err(_) => {
                        let (leftmost, mut steps) =
                            walk(j.left.as_ref(), replicated, crossed_filter)?;
                        steps.push(ChainStep {
                            right,
                            keys,
                            residual_filter,
                            join_type: j.join_type,
                        });
                        Ok((leftmost, steps))
                    }
                }
            }
            other => Err(Error::Unsupported(format!(
                "auto-distribute: expected left-deep equijoin chain, found `{}`",
                other.display().to_string().lines().next().unwrap_or("")
            ))),
        }
    }
    let mut crossed_filter = false;
    let (leftmost, steps) = walk(lp, replicated, &mut crossed_filter)?;
    Ok((leftmost, steps, crossed_filter))
}

/// Extract one or more equijoin key pairs plus any non-equality residual (KAN-10 / D-2.7 / D-2.9).
///
/// A residual on an **outer** join (LEFT / RIGHT / FULL) is part of the join condition, not a
/// post-join filter — TPC-H Q13's `LEFT JOIN orders ON c_custkey = o_custkey AND o_comment NOT
/// LIKE …` must keep the `NOT LIKE` in the ON clause or unmatched left rows would be filtered
/// out instead of null-extended. [`build_chain`] therefore emits it into the ON clause. SEMI /
/// ANTI joins keep rejecting residuals: their dropped side makes the reference scope ambiguous.
fn equijoin_keys(
    join: &datafusion::logical_expr::Join,
) -> Result<super::stage_planner::EquijoinKeys> {
    let (keys, residual) = collect_equijoin_keys(&join.on, join.filter.as_ref())?;
    if residual.is_some() && matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
        return Err(Error::Unsupported(
            "auto-distribute: residual filters on semi/anti shuffle joins are not supported".into(),
        ));
    }
    Ok((keys, residual))
}

pub(crate) fn flat_col(alias: &str, col: &str) -> String {
    format!("{alias}__{col}")
}

pub(crate) fn scan_alias<'a>(scan: &SimpleScan<'a>) -> &'a str {
    scan.alias.unwrap_or(scan.table)
}

pub(crate) fn leaf_stage_sql(scan: &SimpleScan<'_>) -> (String, Vec<String>) {
    let alias = scan_alias(scan);
    let mut flats = Vec::new();
    let mut sels = Vec::new();
    for f in scan.schema.fields() {
        let name = f.name();
        let flat = flat_col(alias, name);
        sels.push(format!("{name} AS {flat}"));
        flats.push(flat);
    }
    let from = match &scan.filter_sql {
        Some(pred) => format!("FROM {} WHERE {pred}", scan.table_sql),
        None => format!("FROM {}", scan.table_sql),
    };
    (
        sanitize_generated_sql(&format!("SELECT {} {from}", sels.join(", "))),
        flats,
    )
}

pub(crate) fn flat_key_index(flats: &[String], alias: &str, col: &str) -> Result<u32> {
    let want = flat_col(alias, col);
    flats
        .iter()
        .position(|c| c == &want)
        .map(|i| i as u32)
        .ok_or_else(|| {
            Error::Unsupported(format!(
                "auto-distribute: join key `{want}` missing from shuffle projection {flats:?}"
            ))
        })
}

/// Union-find over the chain's INNER equijoin keys, keyed by `(alias, column)`.
///
/// KAN-162 (TPC-DS q17/q25/q29 at the all-facts-sharded classification): a sharded
/// boundary's left key can reference a replicated dim that folds into the stage and
/// never becomes a shuffle input — `sr_customer_sk = cs_bill_customer_sk` when
/// `store_returns` is replicated. Conjunctive inner-join equality is transitive, so the
/// key substitutes with an equivalent column the co-located shuffle stream carries
/// (`ss_customer_sk` via the chain's own `ss_customer_sk = sr_customer_sk`); the stage's
/// join result and hash co-location are unchanged. Only INNER steps feed the web and
/// only INNER boundaries substitute: outer / semi / anti keys decide null-extension /
/// presence and keep the historical rejection when the key is not carried.
struct ChainKeyWeb {
    parent: HashMap<(String, String), (String, String)>,
    /// Every linked key, in chain order — the leftmost leaf's keys link first, so the
    /// first carried peer in insertion order is the chain root's when one exists.
    members: Vec<(String, String)>,
}

impl ChainKeyWeb {
    fn build(aliases: &HashMap<String, String>, steps: &[ChainStep<'_>]) -> Self {
        let mut web = ChainKeyWeb {
            parent: HashMap::new(),
            members: Vec::new(),
        };
        for step in steps {
            if step.join_type != JoinType::Inner {
                continue;
            }
            for (l, r) in &step.keys {
                let (Ok(l_rel), Ok(l_name)) = (relation_of(l), column_name(l)) else {
                    continue;
                };
                let (Ok(r_rel), Ok(r_name)) = (relation_of(r), column_name(r)) else {
                    continue;
                };
                let la = aliases.get(&l_rel).cloned().unwrap_or(l_rel);
                let ra = aliases.get(&r_rel).cloned().unwrap_or(r_rel);
                web.link((la, l_name), (ra, r_name));
            }
        }
        web
    }

    fn find(&self, key: &(String, String)) -> (String, String) {
        let mut root = key.clone();
        while let Some(p) = self.parent.get(&root) {
            root = p.clone();
        }
        root
    }

    fn link(&mut self, a: (String, String), b: (String, String)) {
        for k in [&a, &b] {
            if !self.members.contains(k) {
                self.members.push(k.clone());
            }
        }
        let (ra, rb) = (self.find(&a), self.find(&b));
        if ra != rb {
            self.parent.insert(rb, ra);
        }
    }

    /// An equality-web peer of `key` carried by the shuffle stream (`carried` holds the
    /// leftmost alias plus the aliases of the sharded steps placed so far).
    fn carried_peer(&self, key: &(String, String), carried: &[String]) -> Option<(String, String)> {
        let root = self.find(key);
        self.members
            .iter()
            .find(|k| self.find(k) == root && carried.contains(&k.0))
            .cloned()
    }
}

/// The aliases whose columns the left shuffle stream carries at chain step `i`: the
/// leftmost leaf plus every sharded right side placed before it.
fn carried_aliases(
    left_alias: &str,
    steps: &[ChainStep<'_>],
    sharded: &[&str],
    i: usize,
) -> Vec<String> {
    let mut carried = vec![left_alias.to_string()];
    for s in &steps[..i] {
        if s.right.is_sharded(sharded) {
            carried.push(s.right.alias().to_string());
        }
    }
    carried
}

/// Substitute an INNER boundary's left key when it references a folded replicated dim
/// (never a shuffle input): the carried equality-web peer is equal by transitivity, so
/// the shuffle hash and ON clause bind it instead. Uncarried keys with no peer pass
/// through unchanged — the historical "missing from shuffle projection" rejection.
fn substitute_carried_key(
    join_type: JoinType,
    meta: &mut (String, String),
    key_web: &ChainKeyWeb,
    carried: &[String],
) {
    if join_type == JoinType::Inner && !carried.contains(&meta.0) {
        if let Some(peer) = key_web.carried_peer(meta, carried) {
            *meta = peer;
        }
    }
}

fn relation_of(e: &Expr) -> Result<String> {
    match e {
        Expr::Column(c) => c
            .relation
            .as_ref()
            .map(|r| r.table().to_string())
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "auto-distribute: join key `{}` has no table qualifier",
                    c.name
                ))
            }),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: join key must be a column, found {other}"
        ))),
    }
}

/// Flatten qualified column references to the names the chain's final stage binds. Columns
/// from a replicated dimension folded into that stage keep their qualified raw name
/// (`alias.col` — the fold joins the raw table into the stage's FROM); every other relation's
/// columns arrive on shuffle inputs under the flattened `alias__col` name.
fn flatten_expr(
    e: &Expr,
    alias_by_relation: &HashMap<String, String>,
    replicated_aliases: &[String],
) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                if let Some(rel) = &c.relation {
                    let rname = rel.table();
                    if let Some(alias) = alias_by_relation.get(rname) {
                        let rewritten = if replicated_aliases.iter().any(|a| a == alias) {
                            datafusion::common::Column::new(Some(alias.as_str()), c.name.clone())
                        } else {
                            datafusion::common::Column::from_name(flat_col(alias, &c.name))
                        };
                        return Ok(Transformed::yes(Expr::Column(rewritten)));
                    }
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
}

/// Rewrite a logical join residual for the physical names used by a pairwise chain stage.
///
/// Columns already carried by the left intermediate and columns from the current shuffled right
/// input are flattened; pending replicated inputs remain ordinary qualified table columns.
/// Also used by the semi/anti planner's sharded–sharded outer body (TPC-H Q16), which exports
/// its co-located join output under the same flattened names.
pub(crate) fn flatten_join_residual(
    e: &Expr,
    alias_by_relation: &HashMap<String, String>,
    right_alias: &str,
    replicated_aliases: &[String],
) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                if let Some(rel) = &c.relation {
                    let relation = rel.table();
                    let alias = alias_by_relation
                        .get(relation)
                        .map(String::as_str)
                        .unwrap_or(relation);
                    let rewritten = if replicated_aliases.iter().any(|raw| raw == alias) {
                        datafusion::common::Column::new(Some(alias), c.name.clone())
                    } else {
                        let stage_alias = if alias == right_alias { "r" } else { "l" };
                        datafusion::common::Column::new(Some(stage_alias), flat_col(alias, &c.name))
                    };
                    return Ok(Transformed::yes(Expr::Column(rewritten)));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
}

/// KAN-150 semi-join runtime filters (Trino-style dynamic filtering across stage
/// boundaries), **on by default** (`OXIDANT_SEMI_JOIN_FILTERS=0` to disable): INNER
/// equijoins to a *filtered replicated* dimension inject an `IN (SELECT <dim key> FROM <dim>
/// WHERE <dim filters>)` conjunct into the sharded leaf's scan-stage SQL, so the dim's key
/// set filters fact rows BEFORE the shuffle write. Exactness: the join is INNER, so a leaf
/// row whose key matches no (filtered) dim row can never contribute to the output — the
/// conjunct only removes rows the join itself would drop, and the join still runs unaltered
/// downstream (row multiplicity and dim columns are unaffected). On the worker the
/// re-planned subquery becomes a hash semi join building on the replicated dim whose
/// DataFusion dynamic filter reaches the fact's parquet scan (row-group / page-index
/// pruning) — the single-node KAN-2 R2 machinery, re-materialized across the stage
/// boundary as SQL.
pub fn semi_join_filters_enabled() -> bool {
    std::env::var("OXIDANT_SEMI_JOIN_FILTERS")
        .ok()
        .as_deref()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
}

/// Maximum dim-table row count for semi-join filter admission (KAN-160; env
/// `OXIDANT_SEMI_JOIN_FILTER_MAX_DIM_ROWS`, default **1_000_000**): a filtered replicated
/// dim only injects its `IN (SELECT …)` conjunct when the dim's `TableProvider::statistics()`
/// reports an EXACT row count at or below this cap. The subquery's build side (the filtered
/// dim key set, materialized as a hash semi join in every leaf-stage task) is bounded by the
/// dim's cardinality, so an unbounded admit could shuffle-filter a fact leaf against a
/// near-fact-sized dim — pure overhead plus a real memory risk. `Inexact` counts are
/// rejected outright (KAN-146 discipline: provable admission only); `Absent` fails OPEN
/// (providers without statistics, e.g. `MemTable`, keep the KAN-150 behavior). The compared
/// count is the dim's UNFILTERED table cardinality — the conservative bound, since the
/// `filter_sql` conjuncts can only shrink the key set.
pub fn semi_join_filter_max_dim_rows() -> usize {
    std::env::var("OXIDANT_SEMI_JOIN_FILTER_MAX_DIM_ROWS")
        .ok()
        .as_deref()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000)
}

/// Semi-join filter conjuncts to splice into a chain's leaf scan stages: `leftmost` for the
/// chain's leftmost scan, `by_step[i]` for step `i`'s (sharded) right scan.
#[derive(Default)]
struct SemiJoinLeafFilters {
    leftmost: Vec<String>,
    by_step: HashMap<usize, Vec<String>>,
}

/// Compute the semi-join leaf filters for a shuffle-join chain (KAN-150). A step
/// contributes conjuncts only when the injection is provably exact and useful:
///
/// - the step is an INNER equijoin to a **replicated** dim (present in full on every
///   worker, so the leaf stage's IN-subquery resolves there);
/// - the dim scan is **filtered** (`filter_sql`) — an unfiltered FK→PK dim admits ~every
///   fact key, so the subquery would be pure overhead (KAN-146: provable admission only);
/// - the dim filter is non-volatile (the subquery evaluates it a second time in the leaf
///   stage — a duplicated `rand()`/`now()` could disagree with the join-stage fold);
/// - the dim's `TableProvider::statistics()` row count passes the KAN-160 size gate:
///   `Exact(n)` admits iff `n <= semi_join_filter_max_dim_rows()`, `Inexact` rejects
///   (KAN-146 discipline), `Absent` fails open (see [`semi_join_filter_max_dim_rows`]);
/// - when the chain-side key resolves to a sharded step's right leaf, that boundary's join
///   must not PRESERVE the leaf's side (INNER or LEFT — a RIGHT/FULL boundary emits dim-
///   key-less leaf rows with NULL extension, which the filter would wrongly drop). The
///   leftmost leaf needs no such guard: its rows traverse every downstream step linearly,
///   and the INNER dim step drops non-matching rows whatever the intermediate join types.
///
/// Anything unexpected (non-column keys, keys on replicated-only relations, …) skips that
/// step silently — injection is an optimization, never a planning failure.
fn semi_join_leaf_filters(
    leftmost: &ChainSide<'_>,
    steps: &[ChainStep<'_>],
    sharded: &[&str],
    replicated: &[&str],
) -> SemiJoinLeafFilters {
    let mut out = SemiJoinLeafFilters::default();
    if !semi_join_filters_enabled() {
        return out;
    }
    // Injection targets plain scan leaves only: a derived leg's sub-DAG plans on its own.
    let ChainSide::Scan(leftmost) = leftmost else {
        return out;
    };
    // KAN-160: read the admission cap ONCE per call — per-step env reads would be
    // inconsistent with the kill-switch pattern above and could observe a mid-plan change.
    let max_dim_rows = semi_join_filter_max_dim_rows();
    for (j, step) in steps.iter().enumerate() {
        let Some(scan) = step.right.as_scan() else {
            continue;
        };
        if step.join_type != JoinType::Inner
            || sharded.contains(&scan.table)
            || !replicated.contains(&scan.table)
        {
            continue;
        }
        let Some(dim_filter) = &scan.filter_sql else {
            continue;
        };
        if sql_contains_volatile(dim_filter) {
            continue;
        }
        // KAN-160 size gate: admit only a provably small dim (the unfiltered cardinality
        // bounds the filtered key set the leaf stages hash-build).
        use datafusion::common::stats::Precision;
        match scan.stats_num_rows {
            Precision::Exact(n) if n <= max_dim_rows => {}
            Precision::Exact(_) | Precision::Inexact(_) => continue,
            Precision::Absent => {}
        }
        let dim_alias = scan_alias(scan);
        for (lk, rk) in &step.keys {
            let Ok((chain_key, dim_col)) = orient_fold_key(lk, rk, step) else {
                continue;
            };
            let (Ok(rel), Ok(leaf_col)) = (relation_of(chain_key), column_name(chain_key)) else {
                continue;
            };
            let conjunct = format!(
                "{leaf_col} IN (SELECT {dim_col} FROM {} AS {dim_alias} WHERE {dim_filter})",
                scan.table_sql
            );
            // The leaf this conjunct filters: the leftmost scan, or the sharded right
            // leaf of an earlier step whose boundary join does not preserve it.
            let target = if rel == leftmost.table || rel == scan_alias(leftmost) {
                Some(&mut out.leftmost)
            } else {
                (0..j)
                    .find(|&i| {
                        let Some(s_scan) = steps[i].right.as_scan() else {
                            return false;
                        };
                        sharded.contains(&s_scan.table)
                            && (rel == s_scan.table || rel == scan_alias(s_scan))
                            && matches!(steps[i].join_type, JoinType::Inner | JoinType::Left)
                    })
                    .map(|i| out.by_step.entry(i).or_default())
            };
            if let Some(list) = target {
                if !list.contains(&conjunct) {
                    list.push(conjunct);
                }
            }
        }
    }
    out
}

/// [`leaf_stage_sql`] with extra semi-join filter conjuncts ANDed into the leaf's WHERE.
fn leaf_stage_sql_with_semis(scan: &SimpleScan<'_>, semis: &[String]) -> (String, Vec<String>) {
    if semis.is_empty() {
        return leaf_stage_sql(scan);
    }
    let extra = format!("({})", semis.join(") AND ("));
    let filter_sql = Some(match &scan.filter_sql {
        Some(prev) => format!("({prev}) AND {extra}"),
        None => extra,
    });
    let narrowed = SimpleScan {
        table: scan.table,
        table_sql: scan.table_sql.clone(),
        alias: scan.alias,
        filter_sql,
        schema: scan.schema.clone(),
        stats_num_rows: scan.stats_num_rows,
    };
    leaf_stage_sql(&narrowed)
}

fn build_chain(
    p: &Peeled<'_>,
    sharded: &[&str],
    replicated: &[&str],
    leftmost: ChainSide<'_>,
    steps: &[ChainStep<'_>],
    crossed_filter: bool,
) -> Result<DistributedQuery> {
    let mut alias_by_relation: HashMap<String, String> = HashMap::new();
    let left_alias = leftmost.alias().to_string();
    if let Some(t) = leftmost.table() {
        alias_by_relation.insert(t.to_string(), left_alias.clone());
    }
    alias_by_relation.insert(left_alias.clone(), left_alias.clone());

    let mut stages: Vec<StageDef> = Vec::new();
    let mut next_id: u32 = 0;

    enum LeftSide<'s, 'a> {
        Leaf(&'s SimpleScan<'a>),
        Derived(&'s DerivedLeg<'a>),
        Stage { id: u32 },
    }
    let mut left_side = match &leftmost {
        ChainSide::Scan(s) => LeftSide::Leaf(s),
        ChainSide::Derived(d) => LeftSide::Derived(d),
    };
    let mut left_flats: Vec<String> = Vec::new();
    let mut pending_bcast: Vec<usize> = Vec::new(); // indices into steps

    // KAN-150: semi-join runtime filters — filtered replicated dims inner-joining the
    // chain inject an IN-subquery key filter into the sharded leaf scan stages they key
    // on, pruning fact rows before the shuffle write (worker-side, the subquery re-plans
    // to a hash semi join whose dynamic filter prunes the fact's parquet scan).
    let semis = semi_join_leaf_filters(&leftmost, steps, sharded, replicated);

    // KAN-162: the chain-wide inner-join equality web (plus the full relation→alias map
    // it keys on) for folded-dim left-key substitution — see [`ChainKeyWeb`].
    let mut web_aliases: HashMap<String, String> = HashMap::new();
    if let Some(t) = leftmost.table() {
        web_aliases.insert(t.to_string(), left_alias.clone());
    }
    web_aliases.insert(left_alias.clone(), left_alias.clone());
    for s in steps {
        let a = s.right.alias().to_string();
        if let Some(t) = s.right.table() {
            web_aliases.insert(t.to_string(), a.clone());
        }
        web_aliases.insert(a.clone(), a);
    }
    let key_web = ChainKeyWeb::build(&web_aliases, steps);

    let n = steps.len();
    for i in 0..n {
        let step = &steps[i];
        let right_alias = step.right.alias().to_string();
        if let Some(t) = step.right.table() {
            alias_by_relation.insert(t.to_string(), right_alias.clone());
        }
        alias_by_relation.insert(right_alias.clone(), right_alias.clone());

        let right_is_sharded = step.right.is_sharded(sharded);

        if !right_is_sharded {
            // A non-sharded side is always a plain scan (a derived leg `is_sharded`).
            let Some(scan) = step.right.as_scan() else {
                return Err(Error::Unsupported(
                    "auto-distribute: internal: non-sharded chain side is not a leaf scan".into(),
                ));
            };
            if !replicated.contains(&scan.table) {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: `{}` must be listed in replicated",
                    scan.table
                )));
            }
            pending_bcast.push(i);
            continue;
        }

        // Resolve each composite key's left-side alias (usually one relation).
        let mut left_key_metas: Vec<(String, String)> = Vec::with_capacity(step.keys.len());
        let mut right_key_names: Vec<String> = Vec::with_capacity(step.keys.len());
        for (left_key, right_key) in &step.keys {
            let left_key_name = column_name(left_key)?;
            let left_key_rel = relation_of(left_key)?;
            let left_key_alias = alias_by_relation
                .get(&left_key_rel)
                .cloned()
                .unwrap_or(left_key_rel);
            left_key_metas.push((left_key_alias, left_key_name));
            right_key_names.push(column_name(right_key)?);
        }
        // KAN-162: a left key referencing a folded replicated dim (q17/q25/q29's
        // `sr_customer_sk = cs_bill_customer_sk` with store_returns replicated) is not on
        // the shuffle stream — substitute the carried equality-web peer.
        let carried = carried_aliases(&left_alias, steps, sharded, i);
        for meta in &mut left_key_metas {
            substitute_carried_key(step.join_type, meta, &key_web, &carried);
        }
        let left_stage_id = match &left_side {
            LeftSide::Leaf(scan) => {
                let (sql, flats) = leaf_stage_sql_with_semis(scan, &semis.leftmost);
                let mut key_idxs = Vec::with_capacity(left_key_metas.len());
                for (alias, name) in &left_key_metas {
                    key_idxs.push(flat_key_index(&flats, alias, name)?);
                }
                let id = next_id;
                next_id += 1;
                stages.push(StageDef::new(id, sql, vec![], key_idxs));
                left_flats = flats;
                id
            }
            LeftSide::Derived(leg) => {
                // The opaque leg's sub-DAG + export stage stand in for the leaf scan's
                // shuffle stage; the export re-hashes by this boundary's left key(s).
                let key_names: Vec<String> = left_key_metas
                    .iter()
                    .map(|(_, name)| name.clone())
                    .collect();
                let (id, flats) = materialize_derived_leg(
                    leg,
                    replicated,
                    &key_names,
                    &mut stages,
                    &mut next_id,
                )?;
                left_flats = flats;
                id
            }
            LeftSide::Stage { id } => {
                for (alias, name) in &left_key_metas {
                    let _ = flat_key_index(&left_flats, alias, name)?;
                }
                *id
            }
        };

        let (right_id, right_flats) = match &step.right {
            ChainSide::Scan(scan) => {
                let (right_sql, right_flats) = leaf_stage_sql_with_semis(
                    scan,
                    semis.by_step.get(&i).map_or(&[], Vec::as_slice),
                );
                let mut right_key_idxs = Vec::with_capacity(right_key_names.len());
                for name in &right_key_names {
                    right_key_idxs.push(flat_key_index(&right_flats, &right_alias, name)?);
                }
                let right_id = next_id;
                next_id += 1;
                stages.push(StageDef::new(right_id, right_sql, vec![], right_key_idxs));
                (right_id, right_flats)
            }
            // A derived leg materializes as its own sub-DAG + export stage, hash-keyed on
            // this boundary's right key(s) — the join stage consumes it exactly like a
            // leaf scan's shuffle stage.
            ChainSide::Derived(leg) => materialize_derived_leg(
                leg,
                replicated,
                &right_key_names,
                &mut stages,
                &mut next_id,
            )?,
        };

        let replicated_aliases: Vec<String> = pending_bcast
            .iter()
            .map(|&bi| steps[bi].right.alias().to_string())
            .collect();
        let up = Unparser::default();
        let mut on_sql = left_key_metas
            .iter()
            .zip(right_key_names.iter())
            .map(|((l_alias, l_name), r_name)| {
                format!(
                    "l.{} = r.{}",
                    flat_col(l_alias, l_name),
                    flat_col(&right_alias, r_name)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        // Outer join (LEFT/RIGHT/FULL): a non-equality residual is part of the join condition —
        // fold it into the ON clause so unmatched rows still null-extend (TPC-H Q13's
        // `… AND o_comment NOT LIKE …`). It must not reference a replicated dim folded *below*,
        // whose alias is only bound later in the FROM clause.
        let inner_join = step.join_type == JoinType::Inner;
        if !inner_join {
            if let Some(residual) = &step.residual_filter {
                let flattened = flatten_join_residual(
                    residual,
                    &alias_by_relation,
                    &right_alias,
                    &replicated_aliases,
                );
                if expr_references_relations(&flattened, &replicated_aliases) {
                    return Err(Error::Unsupported(
                        "auto-distribute: outer shuffle join residual references a replicated \
                         dimension folded later in the stage — not supported"
                            .into(),
                    ));
                }
                on_sql.push_str(&format!(" AND ({})", expr_sql(&up, &flattened)?));
            }
            if pending_bcast
                .iter()
                .any(|&bi| steps[bi].residual_filter.is_some())
            {
                return Err(Error::Unsupported(
                    "auto-distribute: residual filters on replicated-dimension joins alongside \
                     an outer shuffle join are not supported"
                        .into(),
                ));
            }
        }
        let join_kw = sql_join_keyword(step.join_type)?;
        let mut join_from =
            format!("FROM shuffle_input_0 AS l {join_kw} shuffle_input_1 AS r ON {on_sql}");

        // INNER joins may push residuals to a post-join WHERE (equivalent there); outer joins
        // already folded theirs into the ON clause above. Collected raw and flattened when the
        // WHERE is appended, so trailing-folded dims resolve to their raw qualified names.
        let mut where_residuals: Vec<Expr> = Vec::new();

        // Aliases folded into this stage's FROM so far (emission order), for the raw /
        // `l.` / `r.` reference resolution in [`fold_key_sql`].
        let mut folded_aliases: Vec<String> = Vec::new();
        for &bi in &pending_bcast {
            // Replicated dims are complete on every worker — the established mid-chain fold
            // always emits an inner JOIN.
            emit_dim_fold(
                &mut join_from,
                &steps[bi],
                "JOIN",
                &mut alias_by_relation,
                &right_alias,
                &mut folded_aliases,
                &mut where_residuals,
                &up,
            )?;
        }
        if inner_join {
            // The boundary step's own residual follows the pending dims' (emission order).
            where_residuals.extend(step.residual_filter.clone());
        }
        pending_bcast.clear();

        let trailing = &steps[i + 1..];
        let last_sharded = !trailing.iter().any(|s| s.right.is_sharded(sharded));
        let mut replicated_final = replicated_aliases.clone();
        if last_sharded {
            // Trailing replicated-only joins after the last sharded shuffle boundary fold
            // into this final stage (KAN-2 / TPC-DS Q72): their right sides are complete on
            // every worker, so matching and null-extension are key-local — the same argument
            // as the mid-chain fold, one stage later. Any shape where the fold is unsafe
            // keeps the historical rejection (never a wrong plan).
            if !trailing.is_empty() {
                let unsafe_rejection = || {
                    Error::Unsupported(
                        "auto-distribute: trailing replicated-only joins after the last sharded \
                         shuffle join are not folded here — mark them replicated and keep a \
                         sharded table as the rightmost join, or use the broadcast (1-sharded) \
                         path"
                            .into(),
                    )
                };
                // A `Filter` crossed above the chain was never captured: folding now would
                // silently drop it from the plan. (`distribute_chain_filter` normally lands
                // those conjuncts on scans / step residuals first; when it declines, we do.)
                if crossed_filter {
                    return Err(unsafe_rejection());
                }
                // Null-extension below the fold must not be re-filtered by it: an outer (or
                // semi/anti) boundary, or any earlier non-INNER/LEFT step, declines.
                if !inner_join
                    || steps[..i]
                        .iter()
                        .any(|s| !matches!(s.join_type, JoinType::Inner | JoinType::Left))
                {
                    return Err(unsafe_rejection());
                }
                // The right aliases earlier LEFT steps null-extend: a trailing INNER fold
                // referencing one would re-filter the null-extended rows away.
                let null_extended: Vec<String> = steps[..i]
                    .iter()
                    .filter(|s| s.join_type == JoinType::Left)
                    .map(|s| s.right.alias().to_string())
                    .collect();
                // Aliases the co-located shuffle inputs carry: the leftmost leaf and every
                // sharded step up to the boundary.
                let mut stream_aliases: Vec<String> = vec![left_alias.clone()];
                for s in &steps[..=i] {
                    if s.right.is_sharded(sharded) {
                        stream_aliases.push(s.right.alias().to_string());
                    }
                }
                for t in trailing {
                    // `last_sharded` means every trailing side is a non-shuffled plain scan
                    // (a derived leg `is_sharded`, so it can never appear here).
                    let Some(t_scan) = t.right.as_scan() else {
                        return Err(unsafe_rejection());
                    };
                    if !replicated.contains(&t_scan.table) {
                        return Err(Error::Unsupported(format!(
                            "auto-distribute: `{}` must be listed in replicated",
                            t_scan.table
                        )));
                    }
                    // Only INNER / LEFT folds are provably key-local against a complete
                    // replicated right side (RIGHT/FULL would duplicate preserved dim rows on
                    // every worker; SEMI/ANTI keep their own dedicated paths).
                    if !matches!(t.join_type, JoinType::Inner | JoinType::Left) {
                        return Err(unsafe_rejection());
                    }
                    // Every relation the fold's keys / residual references must already be
                    // bound in this stage's FROM: a shuffle input, an earlier fold, or (for
                    // the residual) the step's own right side. Key pairs are oriented first —
                    // the chain-side reference is the one that must resolve.
                    let t_alias = scan_alias(t_scan).to_string();
                    let mut refs: Vec<String> = Vec::new();
                    for (lk, rk) in &t.keys {
                        let Ok((chain_key, _)) = orient_fold_key(lk, rk, t) else {
                            return Err(unsafe_rejection());
                        };
                        let rel = relation_of(chain_key)?;
                        refs.push(alias_by_relation.get(&rel).cloned().unwrap_or(rel));
                    }
                    let mut residual_refs = Vec::new();
                    if let Some(residual) = &t.residual_filter {
                        collect_column_relations(residual, &mut residual_refs);
                    }
                    let residual_ok = residual_refs.iter().all(|rel| {
                        let alias = alias_by_relation
                            .get(rel)
                            .cloned()
                            .unwrap_or_else(|| rel.clone());
                        stream_aliases.iter().any(|a| a == &alias)
                            || folded_aliases.iter().any(|a| a == &alias)
                            || alias == t_alias
                    });
                    let keys_ok = refs.iter().all(|alias| {
                        stream_aliases.iter().any(|a| a == alias)
                            || folded_aliases.iter().any(|a| a == alias)
                    });
                    if !keys_ok || !residual_ok {
                        return Err(unsafe_rejection());
                    }
                    if t.join_type == JoinType::Inner
                        && refs
                            .iter()
                            .chain(residual_refs.iter())
                            .any(|alias| null_extended.iter().any(|ne| ne == alias))
                    {
                        return Err(unsafe_rejection());
                    }
                    emit_dim_fold(
                        &mut join_from,
                        t,
                        sql_join_keyword(t.join_type)?,
                        &mut alias_by_relation,
                        &right_alias,
                        &mut folded_aliases,
                        &mut where_residuals,
                        &up,
                    )?;
                    replicated_final.push(t_alias);
                }
            }
            append_where_clause(
                &mut join_from,
                &where_residuals,
                &alias_by_relation,
                &right_alias,
                &replicated_final,
                &up,
            )?;
            return finish_with_aggregate(
                p,
                &alias_by_relation,
                &replicated_final,
                &join_from,
                left_stage_id,
                right_id,
                &mut stages,
                &mut next_id,
            );
        }

        append_where_clause(
            &mut join_from,
            &where_residuals,
            &alias_by_relation,
            &right_alias,
            &replicated_aliases,
            &up,
        )?;

        // Intermediate join output; hash by next sharded join's left key(s) when possible.
        // After FULL/RIGHT, unmatched right rows have NULL on the left join key — carry
        // COALESCE(l.key, r.key) under the left flat name so the next shuffle still colocates.
        let coalesce_join_key = matches!(step.join_type, JoinType::Full | JoinType::Right);
        let coalesce_left_flats: Vec<(String, String)> = left_key_metas
            .iter()
            .zip(right_key_names.iter())
            .map(|((l_alias, l_name), r_name)| {
                (flat_col(l_alias, l_name), flat_col(&right_alias, r_name))
            })
            .collect();
        let mut proj = Vec::new();
        let mut new_flats = Vec::new();
        for c in &left_flats {
            if let Some((_, right_flat)) = coalesce_join_key
                .then_some(())
                .and_then(|_| coalesce_left_flats.iter().find(|(lf, _)| lf == c))
            {
                proj.push(format!("COALESCE(l.{c}, r.{right_flat}) AS {c}"));
            } else {
                proj.push(format!("l.{c} AS {c}"));
            }
            new_flats.push(c.clone());
        }
        if projects_right_side(step.join_type) {
            for c in &right_flats {
                // Skip right join-key columns when coalesced into the left flat name.
                if coalesce_join_key && coalesce_left_flats.iter().any(|(_, rf)| rf == c) {
                    continue;
                }
                proj.push(format!("r.{c} AS {c}"));
                new_flats.push(c.clone());
            }
        }
        // LeftSemi/LeftAnti: left columns only (right side is not in the join output schema).
        let hash_keys = next_sharded_left_keys(
            steps,
            i + 1,
            sharded,
            &alias_by_relation,
            &key_web,
            &carried_aliases(&left_alias, steps, sharded, i + 1),
        )
        .unwrap_or_else(|| left_key_metas.clone());
        let mut hash_idxs = Vec::with_capacity(hash_keys.len());
        for (hash_alias, hash_col) in &hash_keys {
            hash_idxs.push(flat_key_index(&new_flats, hash_alias, hash_col)?);
        }
        let join_id = next_id;
        next_id += 1;
        stages.push(StageDef::new(
            join_id,
            sanitize_generated_sql(&format!("SELECT {} {join_from}", proj.join(", "))),
            vec![left_stage_id, right_id],
            hash_idxs,
        ));
        left_side = LeftSide::Stage { id: join_id };
        left_flats = new_flats;
    }

    Err(Error::Unsupported(
        "auto-distribute: shuffle join chain did not produce a final aggregate stage".into(),
    ))
}

/// Materialize an opaque derived leg (KAN-162: TPC-DS q54's `my_customers` / q64's `cs_ui`
/// at the all-facts-sharded classification). The leg's `SubqueryAlias`-wrapped subplan plans
/// recursively as its own sub-DAG via [`plan_distributed_logical`]; its stages splice into
/// the chain's stage list ([`super::dag_splitter::append_branch`]); then one export stage
/// re-flattens the leg's output columns to the same `<alias>__<col>` names a leaf scan's
/// shuffle stage emits, hash-partitioned by the boundary join key(s) (`key_names`, the
/// leg-side column names of the step's equijoin pairs). The export re-partitions whatever
/// layout the sub-DAG produced, so both bucket layouts of the leg's own shuffles are
/// correct, and the pairwise join machinery downstream is unchanged.
///
/// Stage outputs carry the logical plan's field names (the
/// [`super::dag_splitter::placeholder_plan`] invariant: a sub-DAG's output stage re-aliases
/// every column to its schema field name), so the export SELECTs by field name.
///
/// Declines — never a wrong plan — when the leg plans to a `Forward`-exchange output stage
/// (its sharded input must flow through hash-shuffled stages, not a single-worker forward;
/// the same invariant the CrossJoin splitter enforces on its branches), when the leg's
/// output schema has duplicate field names (the flat export could not name them apart), or
/// when a field name is not a plain identifier (the chain's hand-built `l.<flat>` /
/// `r.<flat>` stage SQL does not quote, so e.g. an un-aliased `sum(...)` output column
/// cannot be referenced).
///
/// A `DISTINCT` in the leg's subplan is rewritten to its exact group-by equivalent first
/// ([`rewrite_leg_distincts`]) — the recursive planner has no `Distinct` vocabulary.
fn materialize_derived_leg(
    leg: &DerivedLeg<'_>,
    replicated: &[&str],
    key_names: &[String],
    stages: &mut Vec<StageDef>,
    next_id: &mut u32,
) -> Result<(u32, Vec<String>)> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut flats = Vec::new();
    let mut sels = Vec::new();
    for f in leg.schema.fields() {
        let name = f.name();
        if !seen.insert(name.as_str()) {
            return Err(Error::Unsupported(format!(
                "auto-distribute: derived shuffle join side `{}` outputs duplicate column \
                 `{name}` — cannot name its shuffle flats apart",
                leg.alias
            )));
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || name.chars().next().is_some_and(|c| c.is_ascii_digit())
        {
            return Err(Error::Unsupported(format!(
                "auto-distribute: derived shuffle join side `{}` outputs non-identifier column \
                 `{name}` — alias the derived table's columns to plain names",
                leg.alias
            )));
        }
        let flat = flat_col(leg.alias, name);
        sels.push(format!("{name} AS {flat}"));
        flats.push(flat);
    }
    let leg_plan = rewrite_leg_distincts(leg.plan)?;
    let dq = plan_distributed_logical(&leg_plan, replicated).map_err(|e| {
        Error::Unsupported(format!(
            "auto-distribute: derived shuffle join side `{}` is not distributable: {e}",
            leg.alias
        ))
    })?;
    if dq
        .stages
        .last()
        .is_some_and(|s| s.exchange == ExchangeMode::Forward)
    {
        return Err(Error::Unsupported(format!(
            "auto-distribute: derived shuffle join side `{}` scans a sharded table but outputs \
             via Forward exchange; its sharded input must flow through hash-shuffled stages, \
             not a single-worker forward",
            leg.alias
        )));
    }
    let output_id = super::dag_splitter::append_branch(stages, next_id, dq, 0)?;
    let mut key_idxs = Vec::with_capacity(key_names.len());
    for name in key_names {
        key_idxs.push(flat_key_index(&flats, leg.alias, name)?);
    }
    let id = *next_id;
    *next_id += 1;
    stages.push(StageDef::new(
        id,
        sanitize_generated_sql(&format!("SELECT {} FROM shuffle_input", sels.join(", "))),
        vec![output_id],
        key_idxs,
    ));
    Ok((id, flats))
}

/// Rewrite every `DISTINCT` (`Distinct::All`) in a derived leg's subplan as its exact logical
/// equivalent — an aggregate grouping by every input column — before the recursive
/// [`plan_distributed_logical`] call: the distributed planner has no `Distinct` vocabulary
/// (q54's `my_customers` is a distinct over the union of the two sharded sales facts, which
/// the KAN-162 multi-sharded-union split plans once the distinct reads as a group-by).
/// `Distinct::On` keeps the recursive planner's explicit rejection.
fn rewrite_leg_distincts(plan: &LogicalPlan) -> Result<LogicalPlan> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    plan.clone()
        .transform(|node| {
            let LogicalPlan::Distinct(distinct) = &node else {
                return Ok(Transformed::no(node));
            };
            let datafusion::logical_expr::Distinct::All(input) = distinct else {
                return Ok(Transformed::no(node));
            };
            let group_expr = input
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect();
            let agg =
                datafusion::logical_expr::Aggregate::try_new(input.clone(), group_expr, vec![])?;
            Ok(Transformed::yes(LogicalPlan::Aggregate(agg)))
        })
        .map(|t| t.data)
        .map_err(|e| {
            Error::Plan(format!(
                "auto-distribute: derived-leg DISTINCT rewrite: {e}"
            ))
        })
}

/// The stage-SQL text a folded join's left-key column binds to: the boundary's left shuffle
/// input (`l.<flat>`), its right shuffle input (`r.<flat>` — TPC-DS Q72's `warehouse` and
/// `date_dim d2` folds key on the `inventory` side of the `catalog_sales ⋈ inventory`
/// boundary), or a replicated dim already folded raw into this stage's FROM (`alias.col`,
/// matching [`flatten_expr`]'s rule for folded dims).
fn fold_key_sql(
    chain_key: &Expr,
    alias_by_relation: &HashMap<String, String>,
    right_alias: &str,
    folded_aliases: &[String],
) -> Result<String> {
    let name = column_name(chain_key)?;
    let rel = relation_of(chain_key)?;
    let alias = alias_by_relation.get(&rel).cloned().unwrap_or(rel);
    Ok(if alias == right_alias {
        format!("r.{}", flat_col(&alias, &name))
    } else if folded_aliases.iter().any(|a| a == &alias) {
        format!("{alias}.{name}")
    } else {
        format!("l.{}", flat_col(&alias, &name))
    })
}

/// Orient a fold step's equijoin pair as `(chain-side key, the folded dim's column)`: exactly
/// one side may reference the step's own right leaf — a pair touching it twice or never
/// cannot key this fold (the check [`super::join_order`]'s re-root applies too). Written
/// `ON` clauses park either way up (Q72's `JOIN warehouse ON (w_warehouse_sk =
/// inv_warehouse_sk)` is dim-first), so emission cannot assume the written order.
fn orient_fold_key<'e>(
    left: &'e Expr,
    right: &'e Expr,
    step: &ChainStep<'_>,
) -> Result<(&'e Expr, String)> {
    let Some(scan) = step.right.as_scan() else {
        return Err(Error::Unsupported(
            "auto-distribute: a derived chain side cannot fold into a stage".into(),
        ));
    };
    let alias = scan_alias(scan);
    let refs_right = |e: &Expr| match e {
        Expr::Column(c) => c
            .relation
            .as_ref()
            .is_some_and(|r| r.table() == alias || r.table() == scan.table),
        _ => false,
    };
    match (refs_right(left), refs_right(right)) {
        (false, true) => Ok((left, column_name(right)?)),
        (true, false) => Ok((right, column_name(left)?)),
        _ => Err(Error::Unsupported(format!(
            "auto-distribute: join key ({left}, {right}) does not connect `{}` to the chain",
            scan.table
        ))),
    }
}

/// Emit one replicated-dimension fold into a chain stage's FROM: `<kw> <table> AS <alias> ON
/// <keys> [AND (<residual>)] [AND (<scan filter>)]`. Replicated right sides are complete on
/// every worker, so matching / null-extension is key-local; `join_kw` is `JOIN` for inner
/// folds and `LEFT JOIN` for outer ones (an outer fold's residual stays in its ON clause —
/// moving it to the stage WHERE would re-filter null-extended rows). An inner fold's
/// residual is collected raw into `where_residuals` and flattened only when the WHERE is
/// appended, after every fold is bound.
#[allow(clippy::too_many_arguments)]
fn emit_dim_fold(
    join_from: &mut String,
    step: &ChainStep<'_>,
    join_kw: &str,
    alias_by_relation: &mut HashMap<String, String>,
    right_alias: &str,
    folded_aliases: &mut Vec<String>,
    where_residuals: &mut Vec<Expr>,
    up: &Unparser,
) -> Result<()> {
    let Some(scan) = step.right.as_scan() else {
        return Err(Error::Unsupported(
            "auto-distribute: a derived chain side cannot fold into a stage".into(),
        ));
    };
    let b_alias = scan_alias(scan).to_string();
    let mut on_parts = Vec::with_capacity(step.keys.len());
    for (b_left, b_right) in &step.keys {
        let (chain_key, dim_col) = orient_fold_key(b_left, b_right, step)?;
        on_parts.push(format!(
            "{} = {b_alias}.{dim_col}",
            fold_key_sql(chain_key, alias_by_relation, right_alias, folded_aliases)?
        ));
    }
    join_from.push_str(&format!(
        " {join_kw} {} AS {b_alias} ON {}",
        scan.table,
        on_parts.join(" AND ")
    ));
    alias_by_relation.insert(scan.table.to_string(), b_alias.clone());
    alias_by_relation.insert(b_alias.clone(), b_alias.clone());
    folded_aliases.push(b_alias);
    if let Some(residual) = &step.residual_filter {
        if join_kw == "JOIN" {
            // Inner emission: the residual rides the stage WHERE (equivalent there) —
            // collected raw and flattened once every fold is bound.
            where_residuals.push(residual.clone());
        } else {
            let flattened =
                flatten_join_residual(residual, alias_by_relation, right_alias, folded_aliases);
            join_from.push_str(&format!(" AND ({})", expr_sql(up, &flattened)?));
        }
    }
    if let Some(pred) = &scan.filter_sql {
        join_from.push_str(&format!(" AND ({pred})"));
    }
    Ok(())
}

/// Append the stage WHERE clause for the collected inner-join residuals, flattening each
/// against the physical names bound in the stage's FROM (stream flats for the shuffle
/// inputs, raw qualified names for folded dims).
fn append_where_clause(
    join_from: &mut String,
    where_residuals: &[Expr],
    alias_by_relation: &HashMap<String, String>,
    right_alias: &str,
    replicated_aliases: &[String],
    up: &Unparser,
) -> Result<()> {
    if where_residuals.is_empty() {
        return Ok(());
    }
    let parts = where_residuals
        .iter()
        .map(|residual| {
            expr_sql(
                up,
                &flatten_join_residual(
                    residual,
                    alias_by_relation,
                    right_alias,
                    replicated_aliases,
                ),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    join_from.push_str(&format!(" WHERE {}", parts.join(" AND ")));
    Ok(())
}

/// True when any column in `e` is qualified by one of `relations` (used to keep outer-join
/// residuals from referencing a replicated dim whose alias is bound later in the FROM clause).
fn expr_references_relations(e: &Expr, relations: &[String]) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            if let Some(rel) = &c.relation {
                if relations.iter().any(|r| r == rel.table()) {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn next_sharded_left_keys(
    steps: &[ChainStep<'_>],
    from: usize,
    sharded: &[&str],
    alias_by_relation: &HashMap<String, String>,
    key_web: &ChainKeyWeb,
    carried: &[String],
) -> Option<Vec<(String, String)>> {
    for step in steps.iter().skip(from) {
        if !step.right.is_sharded(sharded) {
            continue;
        }
        let mut out = Vec::with_capacity(step.keys.len());
        for (left_key, _) in &step.keys {
            let col = column_name(left_key).ok()?;
            let rel = relation_of(left_key).ok()?;
            let alias = alias_by_relation.get(&rel).cloned().unwrap_or(rel);
            // KAN-162: the same folded-dim left-key substitution build_chain applies at
            // the boundary — the intermediate stage must hash on the carried peer.
            let mut meta = (alias, col);
            substitute_carried_key(step.join_type, &mut meta, key_web, carried);
            out.push(meta);
        }
        return Some(out);
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn finish_with_aggregate(
    p: &Peeled<'_>,
    alias_by_relation: &HashMap<String, String>,
    replicated_aliases: &[String],
    join_from: &str,
    left_stage_id: u32,
    right_id: u32,
    stages: &mut Vec<StageDef>,
    next_id: &mut u32,
) -> Result<DistributedQuery> {
    let up = Unparser::default();
    let group_sql: Vec<String> = p
        .agg
        .group_expr
        .iter()
        .map(|g| expr_sql(&up, &flatten_expr(g, alias_by_relation, replicated_aliases)))
        .collect::<Result<_>>()?;
    let aggs: Vec<AggSpec> = p
        .agg
        .aggr_expr
        .iter()
        .map(|a| {
            let mut spec = AggSpec::classify(a)?;
            if let Expr::AggregateFunction(af) = a {
                if let Some(arg) = af.params.args.first() {
                    spec.arg_sql = expr_sql(
                        &up,
                        &flatten_expr(arg, alias_by_relation, replicated_aliases),
                    )?;
                }
            }
            Ok(spec)
        })
        .collect::<Result<_>>()?;

    let remap = build_remap(p);

    let (partial_sql, final_sql) = if aggs.iter().any(|a| a.distinct) {
        distinct_stage_sql(&up, p, &group_sql, &aggs, join_from, &remap)?
    } else {
        recombine_stage_sql(p, &group_sql, &aggs, join_from, &remap)?
    };
    let hash_group: Vec<u32> = (0..group_sql.len() as u32).collect();
    let join_id = *next_id;
    *next_id += 1;
    stages.push(StageDef::new(
        join_id,
        partial_sql,
        vec![left_stage_id, right_id],
        hash_group,
    ));
    stages.push(StageDef::new(*next_id, final_sql, vec![join_id], vec![]));
    Ok(DistributedQuery {
        stages: std::mem::take(stages),
        finalize_sql: build_finalize(p)?,
    })
}

/// A re-planned shuffle-join chain tail plus a human-readable account of the decision
/// (measured leaf row counts and the chosen order) for observability.
pub(crate) struct ReplannedTail {
    pub(crate) stages: Vec<StageDef>,
    pub(crate) detail: String,
}

/// Adaptive re-optimization (`OXIDANT_REOPT_JOIN_ORDER`): re-derive the stage DAG of a
/// shuffle-join chain with the **tail** joins re-sequenced by barrier-measured leaf
/// cardinalities, smallest right leaf first. This is the reorder Spark AQE structurally
/// cannot do — it adapts a join strategy per join but never re-plans the join order of an
/// already-dispatched stage graph.
///
/// Called by the driver at the last leaf stage's barrier, once every chain leaf has a
/// measured row count. The leftmost leaf and the first join stay fixed: the leftmost leaf
/// stage's hash key is the first join's left key, while every other leaf stage's SQL and
/// hash keys are order-invariant — so all already-dispatched leaf stages survive
/// byte-identical and the re-planned tail splices onto the dispatched prefix (the driver
/// verifies exactly that before swapping anything in).
///
/// Returns `None` — keep the original plan — when the chain is not a permutable shape
/// (fewer than three joins, any non-inner join, a replicated-dimension fold, a
/// scalar-token plan), when any leaf lacks a measurement, when a dependency cycle or an
/// ambiguous ON column makes the tail unplaceable, or when the measured sizes confirm the
/// current order.
pub(crate) fn replan_chain_tail(
    plan: &LogicalPlan,
    replicated: &[&str],
    stages: &[StageDef],
    stage_rows: &HashMap<u32, Vec<u64>>,
) -> Option<ReplannedTail> {
    // Re-apply the plan-time front-end transforms so re-derived stage SQL matches the
    // dispatched stages byte-for-byte (the same pipeline `plan_distributed_logical` ran).
    let connected = super::join_order::connect_comma_join_chain(plan, replicated);
    let plan = connected.as_ref().unwrap_or(plan);
    let rerooted = super::join_order::reroot_inner_chain_at_sharded(plan, replicated);
    let plan = rerooted.as_ref().unwrap_or(plan);
    let reordered = super::join_order::reorder_filtered_dims_first(plan);
    let plan = reordered.as_ref().unwrap_or(plan);

    let p = super::stage_planner::peel(plan).ok()?;
    let (leftmost, steps, _) = extract_equijoin_chain(&p.agg.input, replicated).ok()?;
    // Three joins are the smallest chain with a permutable two-step tail.
    if steps.len() < 3 || steps.iter().any(|s| s.join_type != JoinType::Inner) {
        return None;
    }
    let tables = base_tables(&p.agg.input);
    let sharded: Vec<&str> = tables
        .iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .map(|t| t.as_str())
        .collect();
    // Every join must be a sharded–sharded shuffle between plain scans: a replicated dim
    // folds into its join stage and has no leaf stage to measure, and an opaque derived leg
    // (KAN-162) materializes as a sub-DAG whose spliced stages this re-plan cannot re-derive
    // byte-for-byte — keep the original plan for both.
    let ChainSide::Scan(leftmost_scan) = &leftmost else {
        return None;
    };
    let mut leaf_scans: Vec<&SimpleScan<'_>> = Vec::with_capacity(steps.len() + 1);
    if !sharded.contains(&leftmost_scan.table) {
        return None;
    }
    leaf_scans.push(leftmost_scan);
    for s in &steps {
        let scan = s.right.as_scan()?;
        if !sharded.contains(&scan.table) {
            return None;
        }
        leaf_scans.push(scan);
    }

    // Measure every leaf through its dispatched stage: leaf SQL is order-invariant, so
    // exact SQL equality finds the stage whose barrier counted its output rows. Any leaf
    // without a match or without a complete barrier sample bails (an undercounted leaf
    // would steer the order on bad data — the safe direction is no re-optimization).
    let mut leaf_rows: Vec<u64> = Vec::with_capacity(steps.len() + 1);
    for scan in &leaf_scans {
        let (sql, _) = leaf_stage_sql(scan);
        let stage = stages
            .iter()
            .find(|s| s.upstream_stage_ids.is_empty() && s.sql == sql)?;
        leaf_rows.push(stage_rows.get(&stage.stage_id)?.iter().sum());
    }
    let leaf_names: Vec<String> = leaf_scans
        .iter()
        .map(|s| scan_alias(s).to_string())
        .collect();

    // Placement dependencies (ported from join_order's reorder): the leaves — other than a
    // step's own right leaf — its ON / residual exprs reference. A step may only be placed
    // once those are in the chain; an unresolvable or ambiguous column bails the rewrite.
    let mut deps: Vec<Vec<usize>> = Vec::with_capacity(steps.len());
    for (si, step) in steps.iter().enumerate() {
        let own_leaf = si + 1;
        let mut step_deps = Vec::new();
        let mut exprs: Vec<&Expr> = step.keys.iter().flat_map(|(l, r)| [l, r]).collect();
        if let Some(f) = &step.residual_filter {
            exprs.push(f);
        }
        for expr in exprs {
            for col in expr.column_refs() {
                let rel = col.relation.as_ref()?.table();
                let i = chain_leaf_index(rel, &leaf_scans)?;
                if i != own_leaf && !step_deps.contains(&i) {
                    step_deps.push(i);
                }
            }
        }
        deps.push(step_deps);
    }

    // Greedy, deterministic placement: step 0 fixed; repeatedly place the smallest
    // remaining step (measured right-leaf rows) whose dependencies are all placed
    // (ties: original order).
    let mut placed: Vec<usize> = vec![0, 1];
    let mut remaining: Vec<usize> = (1..steps.len()).collect();
    let mut new_order: Vec<usize> = vec![0];
    while !remaining.is_empty() {
        let mut best: Option<usize> = None; // position within `remaining`
        for (pos, &si) in remaining.iter().enumerate() {
            if !deps[si].iter().all(|d| placed.contains(d)) {
                continue;
            }
            let better = match best {
                None => true,
                Some(b) => (leaf_rows[si + 1], si) < (leaf_rows[remaining[b] + 1], remaining[b]),
            };
            if better {
                best = Some(pos);
            }
        }
        let pos = best?; // dependency cycle — keep the original plan
        let si = remaining.remove(pos);
        placed.push(si + 1);
        new_order.push(si);
    }
    if new_order.iter().copied().eq(0..steps.len()) {
        return None;
    }

    let mut slots: Vec<Option<ChainStep>> = steps.into_iter().map(Some).collect();
    let permuted: Vec<ChainStep> = new_order
        .iter()
        .map(|&i| slots[i].take().expect("each step placed exactly once"))
        .collect();
    let dq = build_chain(&p, &sharded, replicated, leftmost, &permuted, false).ok()?;
    // A scalar-token plan's positional literal-substitution pipeline must not be re-planned.
    if dq.stages.iter().any(|s| {
        s.sql.contains(crate::driver::SCALAR_TOKEN) || s.sql.contains("__OXIDANT_SCALAR_STAGE_")
    }) {
        return None;
    }

    let measured: Vec<String> = leaf_names
        .iter()
        .zip(leaf_rows.iter())
        .map(|(n, r)| format!("{n}={r}"))
        .collect();
    let chosen: Vec<&str> = new_order
        .iter()
        .map(|&i| leaf_names[i + 1].as_str())
        .collect();
    let detail = format!(
        "measured leaf rows [{}]; tail join order after fixed first join: [{}]",
        measured.join(", "),
        chosen.join(", ")
    );
    tracing::info!(
        target: "oxidant.reopt",
        leaf_rows = ?leaf_rows,
        original_order = ?(0..new_order.len()).collect::<Vec<_>>(),
        chosen_order = ?new_order,
        "re-optimized shuffle-join tail by measured leaf cardinality"
    );
    Some(ReplannedTail {
        stages: dq.stages,
        detail,
    })
}

/// The chain leaf (0 = leftmost, i+1 = `steps[i].right`) a qualified relation resolves to,
/// or `None` when zero or several leaves match — ambiguity bails the rewrite rather than
/// guessing, as in `join_order`.
fn chain_leaf_index(rel: &str, leaves: &[&SimpleScan<'_>]) -> Option<usize> {
    let mut found = None;
    for (idx, scan) in leaves.iter().enumerate() {
        if scan.table == rel || scan.alias == Some(rel) {
            if found.is_some() {
                return None;
            }
            found = Some(idx);
        }
    }
    found
}

/// KAN-26: normalize `Filter → CROSS JOIN` (a SQL comma-join) into the inner equijoin shape the
/// broadcast / shuffle-join planners already understand.
///
/// `FROM orders, lineitem WHERE o_orderkey = l_orderkey AND <preds>` (TPC-H Q12) reaches the
/// planner as a `Cross Join` with the whole WHERE parked in a `Filter` above it, and the chain /
/// two-table paths require their equijoin keys in `on` / `join.filter` — so these queries were
/// rejected with "shuffle join needs an equijoin key". The rewrite is semantics-preserving:
/// cross-table conjuncts move into the join filter ([`collect_equijoin_keys`] promotes the
/// equalities to hash keys, the rest stays residual), and single-table conjuncts push down to a
/// `Filter` over that side. Applied bottom-up, so a multi-table comma join converges across
/// repeated calls (each call normalizes the outermost level; pushed-down filters re-expose the
/// next inner cross join).
///
/// Returns `None` when the plan has no `Filter → cross join` with at least one usable
/// cross-table column equality — the caller then keeps the original error path.
pub(crate) fn rewrite_comma_join_filters(lp: &LogicalPlan) -> Option<LogicalPlan> {
    let (plan, changed) = rewrite_comma_join_node(lp);
    changed.then_some(plan)
}

fn rewrite_comma_join_node(lp: &LogicalPlan) -> (LogicalPlan, bool) {
    let mut changed = false;
    let mut new_inputs = Vec::with_capacity(lp.inputs().len());
    for input in lp.inputs() {
        let (rewritten, child_changed) = rewrite_comma_join_node(input);
        changed |= child_changed;
        new_inputs.push(rewritten);
    }
    let mut node = if changed {
        match lp.with_new_exprs(lp.expressions(), new_inputs) {
            Ok(n) => n,
            Err(_) => return (lp.clone(), false),
        }
    } else {
        lp.clone()
    };
    if let LogicalPlan::Filter(f) = &node {
        if let LogicalPlan::Join(j) = f.input.as_ref() {
            if j.join_type == JoinType::Inner && j.on.is_empty() {
                if let Some(rewritten) = convert_filter_cross_join(&f.predicate, j) {
                    node = rewritten;
                    changed = true;
                }
            }
        }
    }
    (node, changed)
}

/// The relations (qualifiers) and field names one join side brings into scope, used to classify
/// which side of the cross join a filter conjunct's columns come from.
pub(crate) struct JoinSideScope {
    relations: std::collections::HashSet<String>,
    fields: std::collections::HashSet<String>,
}

impl JoinSideScope {
    pub(crate) fn of(lp: &LogicalPlan) -> Self {
        let mut relations = std::collections::HashSet::new();
        let mut fields = std::collections::HashSet::new();
        for (qualifier, field) in lp.schema().iter() {
            if let Some(q) = qualifier {
                relations.insert(q.table().to_string());
            }
            fields.insert(field.name().clone());
        }
        JoinSideScope { relations, fields }
    }

    pub(crate) fn contains(&self, c: &datafusion::common::Column) -> bool {
        match &c.relation {
            Some(r) => self.relations.contains(r.table()),
            None => self.fields.contains(&c.name),
        }
    }
}

pub(crate) enum ConjunctSide {
    Left,
    Right,
    Cross,
    /// A column neither side owns (or an unqualified name both sides own) — do not rewrite.
    Unknown,
}

pub(crate) fn conjunct_side(
    conjunct: &Expr,
    left: &JoinSideScope,
    right: &JoinSideScope,
) -> ConjunctSide {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut saw_left = false;
    let mut saw_right = false;
    let mut unknown = false;
    let _ = conjunct.apply(|node| {
        if let Expr::Column(c) = node {
            match (left.contains(c), right.contains(c)) {
                // Unqualified columns present on both sides are ambiguous — bail.
                (true, true) if c.relation.is_none() => unknown = true,
                (true, _) => saw_left = true,
                (_, true) => saw_right = true,
                _ => unknown = true,
            }
            if unknown {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    match (unknown, saw_left, saw_right) {
        (true, _, _) => ConjunctSide::Unknown,
        (_, true, true) => ConjunctSide::Cross,
        (_, true, false) => ConjunctSide::Left,
        (_, false, true) => ConjunctSide::Right,
        // No columns at all (a constant predicate): treat as left-side, it filters rows either way.
        (_, false, false) => ConjunctSide::Left,
    }
}

/// `a.k = b.k` with plain columns on both sides — promotable to a shuffle hash key.
fn is_column_equality(e: &Expr) -> bool {
    matches!(
        e,
        Expr::BinaryExpr(b)
            if b.op == datafusion::logical_expr::Operator::Eq
                && matches!(b.left.as_ref(), Expr::Column(_))
                && matches!(b.right.as_ref(), Expr::Column(_))
    )
}

fn convert_filter_cross_join(
    predicate: &Expr,
    join: &datafusion::logical_expr::Join,
) -> Option<LogicalPlan> {
    use datafusion::logical_expr::LogicalPlanBuilder;

    let left_scope = JoinSideScope::of(&join.left);
    let right_scope = JoinSideScope::of(&join.right);

    let mut conjuncts = Vec::new();
    super::stage_planner::flatten_and_conjuncts(predicate, &mut conjuncts);
    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut cross_preds = Vec::new();
    for conjunct in conjuncts {
        match conjunct_side(&conjunct, &left_scope, &right_scope) {
            ConjunctSide::Left => left_preds.push(conjunct),
            ConjunctSide::Right => right_preds.push(conjunct),
            ConjunctSide::Cross => cross_preds.push(conjunct),
            ConjunctSide::Unknown => return None,
        }
    }
    // Only rewrite when at least one cross-table equality can become a shuffle key; otherwise
    // the conversion buys nothing and the original error path is clearer.
    if !cross_preds.iter().any(is_column_equality) {
        return None;
    }

    let push_down = |side: &LogicalPlan, preds: Vec<Expr>| -> Option<LogicalPlan> {
        if preds.is_empty() {
            return Some(side.clone());
        }
        let combined = preds.into_iter().reduce(Expr::and)?;
        LogicalPlanBuilder::from(side.clone())
            .filter(combined)
            .ok()?
            .build()
            .ok()
    };
    let new_left = push_down(&join.left, left_preds)?;
    let new_right = push_down(&join.right, right_preds)?;

    if let Some(existing) = &join.filter {
        cross_preds.push(existing.clone());
    }
    let join_filter = cross_preds.into_iter().reduce(Expr::and)?;
    LogicalPlanBuilder::from(new_left)
        .join(
            new_right,
            JoinType::Inner,
            (
                Vec::<datafusion::common::Column>::new(),
                Vec::<datafusion::common::Column>::new(),
            ),
            Some(join_filter),
        )
        .ok()?
        .build()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::Int64Array;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::shuffle::partition::hash_partition;
    use oxidant_loom::arrow::array::Array;

    #[test]
    fn sql_join_keyword_emits_spark_valid_outer_and_semi() {
        assert_eq!(sql_join_keyword(JoinType::Left).unwrap(), "LEFT JOIN");
        assert_eq!(sql_join_keyword(JoinType::Right).unwrap(), "RIGHT JOIN");
        assert_eq!(sql_join_keyword(JoinType::Full).unwrap(), "FULL OUTER JOIN");
        assert_eq!(
            sql_join_keyword(JoinType::LeftSemi).unwrap(),
            "LEFT SEMI JOIN"
        );
        assert_eq!(
            sql_join_keyword(JoinType::LeftAnti).unwrap(),
            "LEFT ANTI JOIN"
        );
        assert!(projects_right_side(JoinType::Left));
        assert!(!projects_right_side(JoinType::LeftSemi));
    }

    #[test]
    fn right_semi_anti_are_rejected_not_emitted() {
        let err = supported_shuffle_join_type(JoinType::RightSemi).unwrap_err();
        assert!(err.to_string().contains("RIGHT SEMI"), "got: {err}");
        let err = sql_join_keyword(JoinType::RightAnti).unwrap_err();
        assert!(
            err.to_string().contains("RIGHT") && err.to_string().contains("ANTI"),
            "got: {err}"
        );
    }

    #[test]
    fn null_join_keys_are_hashed_not_dropped() {
        // Arrow row-format FNV hashing must keep NULL-key rows (required for LEFT/FULL/ANTI).
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(2), None])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();
        let parts = hash_partition(&[batch], &[0], 3).unwrap();
        let got: usize = parts
            .iter()
            .flat_map(|p| p.iter())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(got, 4, "NULL-key rows must survive hash_partition");
        // Both NULL keys co-locate in the same bucket.
        let mut null_buckets = 0;
        for p in &parts {
            let has_null = p.iter().any(|b| {
                let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..k.len()).any(|i| k.is_null(i))
            });
            if has_null {
                null_buckets += 1;
            }
        }
        assert_eq!(null_buckets, 1, "all NULL keys must hash to one bucket");
    }

    #[test]
    fn flat_col_uses_double_underscore_separator() {
        assert_eq!(flat_col("lineitem", "l_orderkey"), "lineitem__l_orderkey");
        assert_eq!(flat_col("o", "orderkey"), "o__orderkey");
    }

    #[test]
    fn flat_key_index_finds_projected_join_key() {
        let flats = vec![
            "orders__o_orderkey".into(),
            "orders__o_custkey".into(),
            "lineitem__l_orderkey".into(),
        ];
        assert_eq!(flat_key_index(&flats, "lineitem", "l_orderkey").unwrap(), 2);
        assert_eq!(flat_key_index(&flats, "orders", "o_orderkey").unwrap(), 0);
    }

    #[test]
    fn flat_key_index_errors_when_join_key_missing() {
        let flats = vec!["orders__o_orderkey".into()];
        let err = flat_key_index(&flats, "lineitem", "l_orderkey").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("lineitem__l_orderkey") && msg.contains("missing"),
            "got: {msg}"
        );
    }

    /// A derived leg whose recursive plan outputs via a `Forward` exchange must refuse: a
    /// single-worker forward would read only one worker's local shard, so the leg's input
    /// must flow through hash-shuffled stages. No SQL-reachable chain shape reaches this
    /// guard (a derived leg is admitted only when it scans a sharded table, and every such
    /// subplan needs a shuffle), so it is pinned here with a hand-built leg: `SELECT k
    /// FROM t WHERE k > 0` over a replicated MemTable plans as a lone Forward stage
    /// (`try_non_aggregate`).
    #[tokio::test]
    async fn derived_leg_with_forward_output_declines() {
        let engine = oxidant_loom::Engine::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(vec![10, 20])) as Arc<dyn Array>,
            ],
        )
        .unwrap();
        engine.register_batches("t", vec![batch]).unwrap();
        let lp = engine
            .logical_plan("SELECT k FROM t WHERE k > 0")
            .await
            .unwrap();
        let leg = DerivedLeg {
            alias: "t",
            plan: &lp,
            schema: lp.schema().clone(),
        };
        let mut stages = Vec::new();
        let mut next_id = 0;
        let err =
            materialize_derived_leg(&leg, &["t"], &["k".to_string()], &mut stages, &mut next_id)
                .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Forward exchange"),
            "the refusal names the Forward-exchange cause: {msg}"
        );
    }

    // --- OXIDANT_REOPT_JOIN_ORDER: replan_chain_tail -------------------------------------

    /// Build the logical plan for a join-chain aggregate over small in-memory tables and
    /// derive its distributed stage DAG through the same chain planner the query would use.
    async fn chain_plan(sql: &str, tables: &[&str]) -> (LogicalPlan, DistributedQuery) {
        let engine = oxidant_loom::Engine::new();
        let int = DataType::Int64;
        for t in tables {
            let cols: &[(&str, DataType)] = if *t == "ta" {
                &[("k", int.clone()), ("g", int.clone())]
            } else {
                &[("k", int.clone())]
            };
            let schema = Arc::new(Schema::new(
                cols.iter()
                    .map(|(n, t)| Field::new(*n, t.clone(), true))
                    .collect::<Vec<_>>(),
            ));
            let batch = RecordBatch::try_new(
                schema,
                cols.iter()
                    .map(|_| Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>)
                    .collect(),
            )
            .unwrap();
            engine.register_batches(t, vec![batch]).unwrap();
        }
        let lp = engine.logical_plan(sql).await.unwrap();
        let p = super::super::stage_planner::peel(&lp).unwrap();
        let dq = plan_shuffle_join_chain(&p, tables, &[]).unwrap();
        (lp, dq)
    }

    /// The stage SQL of the leaf scanning `tbl` (leaf SQL is the only stage SQL naming a
    /// base table — join stages read `shuffle_input_*`).
    fn leaf_sql(dq: &DistributedQuery, tbl: &str) -> String {
        dq.stages
            .iter()
            .find(|s| s.upstream_stage_ids.is_empty() && s.sql.contains(&format!("FROM {tbl}")))
            .unwrap_or_else(|| panic!("no leaf stage for {tbl}"))
            .sql
            .clone()
    }

    /// Barrier-measured row counts for the leaves named in `rows_by_table`.
    fn measured(dq: &DistributedQuery, rows_by_table: &[(&str, u64)]) -> HashMap<u32, Vec<u64>> {
        let mut m = HashMap::new();
        for (tbl, rows) in rows_by_table {
            let stage = dq
                .stages
                .iter()
                .find(|s| s.upstream_stage_ids.is_empty() && s.sql.contains(&format!("FROM {tbl}")))
                .unwrap();
            m.insert(stage.stage_id, vec![*rows]);
        }
        m
    }

    const CHAIN4: &str = "SELECT ta.g, COUNT(*) AS c FROM ta \
         JOIN tb ON ta.k = tb.k JOIN tc ON tb.k = tc.k JOIN td ON ta.k = td.k \
         GROUP BY ta.g";
    const TABLES4: [&str; 4] = ["ta", "tb", "tc", "td"];

    #[tokio::test]
    async fn replan_returns_none_when_sizes_confirm_current_order() {
        let (lp, dq) = chain_plan(CHAIN4, &TABLES4).await;
        // Tail right leaves already ascending (tc < td): nothing to gain.
        let rows = measured(&dq, &[("ta", 100), ("tb", 100), ("tc", 200), ("td", 300)]);
        assert!(replan_chain_tail(&lp, &[], &dq.stages, &rows).is_none());
    }

    #[tokio::test]
    async fn replan_permutes_tail_when_a_tail_leaf_is_smallest() {
        let (lp, dq) = chain_plan(CHAIN4, &TABLES4).await;
        // td (tiny) is written last but must join before tc (huge).
        let rows = measured(&dq, &[("ta", 100), ("tb", 100), ("tc", 5000), ("td", 5)]);
        let replanned = replan_chain_tail(&lp, &[], &dq.stages, &rows)
            .expect("a smaller tail leaf must trigger a re-plan");

        // Equal total stage count, and every dispatched leaf survives byte-identical
        // (SQL + hash keys) so the splice can keep its stage id.
        assert_eq!(replanned.stages.len(), dq.stages.len());
        for leaf in dq.stages.iter().filter(|s| s.upstream_stage_ids.is_empty()) {
            let twin = replanned
                .stages
                .iter()
                .find(|s| s.sql == leaf.sql)
                .unwrap_or_else(|| panic!("leaf lost in re-plan: {}", leaf.sql));
            assert_eq!(twin.hash_key_cols, leaf.hash_key_cols, "{}", leaf.sql);
            assert_eq!(twin.exchange, leaf.exchange, "{}", leaf.sql);
        }
        // The first tail leaf slot (chain position after the fixed first join) now holds
        // tiny td; huge tc moved last. build_chain emits leaf stages at positions 3 and 5.
        assert_eq!(replanned.stages[3].sql, leaf_sql(&dq, "td"));
        assert_eq!(replanned.stages[5].sql, leaf_sql(&dq, "tc"));
        assert!(replanned.detail.contains("td=5"), "{}", replanned.detail);
        assert!(replanned.detail.contains("tc=5000"), "{}", replanned.detail);
    }

    #[tokio::test]
    async fn replan_respects_on_dependencies() {
        // td's join key references tc, so td can never be hoisted ahead of tc no matter how
        // small it measures; te (tiny, depending only on ta) leapfrogs both.
        let sql = "SELECT ta.g, COUNT(*) AS c FROM ta \
                   JOIN tb ON ta.k = tb.k JOIN tc ON ta.k = tc.k \
                   JOIN td ON tc.k = td.k JOIN te ON ta.k = te.k \
                   GROUP BY ta.g";
        let tables = ["ta", "tb", "tc", "td", "te"];
        let (lp, dq) = chain_plan(sql, &tables).await;
        let rows = measured(
            &dq,
            &[
                ("ta", 100),
                ("tb", 100),
                ("tc", 500),
                ("td", 100),
                ("te", 5),
            ],
        );
        let replanned = replan_chain_tail(&lp, &[], &dq.stages, &rows)
            .expect("te must leapfrog the large tail leaves");
        // 5-table chain: leaf stages sit at positions 0,1,3,5,7 in build_chain emission
        // order; the first permutable slot (3) goes to the smallest placeable leaf, te.
        assert_eq!(replanned.stages[3].sql, leaf_sql(&dq, "te"));
        // td still follows tc — never hoisted ahead of the leaf its ON references.
        let pos = |tbl: &str| {
            replanned
                .stages
                .iter()
                .position(|s| s.sql == leaf_sql(&dq, tbl))
                .unwrap()
        };
        assert!(
            pos("tc") < pos("td"),
            "td must stay behind tc: {:?}",
            replanned
                .stages
                .iter()
                .map(|s| s.sql.clone())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn replan_bails_on_non_inner_short_chain_and_missing_measurement() {
        // Any non-inner join: no permutation.
        let left = "SELECT ta.g, COUNT(*) AS c FROM ta \
                    JOIN tb ON ta.k = tb.k LEFT JOIN tc ON tb.k = tc.k \
                    JOIN td ON ta.k = td.k GROUP BY ta.g";
        let (lp, dq) = chain_plan(left, &TABLES4).await;
        let rows = measured(&dq, &[("ta", 100), ("tb", 100), ("tc", 5000), ("td", 5)]);
        assert!(replan_chain_tail(&lp, &[], &dq.stages, &rows).is_none());

        // Two joins only: no permutable tail.
        let short = "SELECT ta.g, COUNT(*) AS c FROM ta \
                     JOIN tb ON ta.k = tb.k JOIN tc ON tb.k = tc.k GROUP BY ta.g";
        let (lp, dq) = chain_plan(short, &["ta", "tb", "tc"]).await;
        let rows = measured(&dq, &[("ta", 100), ("tb", 5000), ("tc", 5)]);
        assert!(replan_chain_tail(&lp, &[], &dq.stages, &rows).is_none());

        // A leaf without a barrier measurement: no re-optimization on partial data.
        let (lp, dq) = chain_plan(CHAIN4, &TABLES4).await;
        let rows = measured(&dq, &[("ta", 100), ("tb", 100), ("tc", 5000)]);
        assert!(replan_chain_tail(&lp, &[], &dq.stages, &rows).is_none());
    }

    // --- KAN-150: semi-join runtime filters injected into leaf scan stages ----------------

    /// Serializes the tests that mutate `OXIDANT_SEMI_JOIN_FILTERS` (process-global env).
    static SEMI_FILTER_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// [`chain_plan`] with distinct sharded / replicated sets: the sharded facts get
    /// `(k, g)` columns, every replicated dim gets `(k, d)` so a scan filter has something
    /// to reference. Dim filters ride a derived table (`JOIN (SELECT … WHERE …) AS …`) so
    /// the scan's `filter_sql` is structural, not optimizer-dependent.
    async fn chain_plan_mixed(
        sql: &str,
        sharded: &[&str],
        replicated: &[&str],
    ) -> DistributedQuery {
        let engine = oxidant_loom::Engine::new();
        let int = DataType::Int64;
        for t in sharded.iter().chain(replicated.iter()) {
            let cols: &[(&str, DataType)] = if sharded.contains(t) {
                &[("k", int.clone()), ("g", int.clone())]
            } else {
                &[("k", int.clone()), ("d", int.clone())]
            };
            let schema = Arc::new(Schema::new(
                cols.iter()
                    .map(|(n, t)| Field::new(*n, t.clone(), true))
                    .collect::<Vec<_>>(),
            ));
            let batch = RecordBatch::try_new(
                schema,
                cols.iter()
                    .map(|_| Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>)
                    .collect(),
            )
            .unwrap();
            engine.register_batches(t, vec![batch]).unwrap();
        }
        let lp = engine.logical_plan(sql).await.unwrap();
        let p = super::super::stage_planner::peel(&lp).unwrap();
        plan_shuffle_join_chain(&p, sharded, replicated)
            .unwrap_or_else(|e| panic!("chain planning failed for {sql}: {e}"))
    }

    /// Star chain with a filtered replicated dim on the leftmost fact: the dim's key set
    /// must filter the fact's leaf scan BEFORE the shuffle.
    #[tokio::test]
    async fn semi_filter_injected_into_leftmost_leaf_for_filtered_dim() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fa.k = dimf.k \
                   JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        let dq = chain_plan_mixed(sql, &["fa", "fb"], &["dim"]).await;
        let leaf = leaf_sql(&dq, "fa");
        assert!(
            leaf.contains("k IN (SELECT k FROM dim AS dimf WHERE (dim.d > 10))"),
            "the filtered dim must inject a semi-join filter into the fa leaf, got:\n{leaf}"
        );
        // The untouched sharded leaf carries no filter.
        assert!(
            !leaf_sql(&dq, "fb").contains(" IN (SELECT"),
            "fb joins no dim — its leaf must stay unfiltered"
        );
    }

    /// A dim keying on a sharded RIGHT leaf injects into that leaf — but only when the
    /// boundary consuming the leaf does not preserve it (INNER here; LEFT equally allowed).
    #[tokio::test]
    async fn semi_filter_injected_into_right_leaf_when_boundary_not_preserving() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   JOIN fb ON fa.k = fb.k \
                   JOIN fc ON fa.k = fc.k \
                   JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fb.k = dimf.k \
                   GROUP BY fa.g";
        let dq = chain_plan_mixed(sql, &["fa", "fb", "fc"], &["dim"]).await;
        assert!(
            leaf_sql(&dq, "fb").contains("k IN (SELECT k FROM dim AS dimf WHERE (dim.d > 10))"),
            "fb's leaf must carry the dim's semi-join filter, got:\n{}",
            leaf_sql(&dq, "fb")
        );
        assert!(
            !leaf_sql(&dq, "fa").contains(" IN (SELECT"),
            "fa keys no dim — its leaf must stay unfiltered"
        );
    }

    /// Exactness guard: when the boundary consuming the leaf PRESERVES the leaf's side
    /// (RIGHT JOIN — unmatched fb rows must null-extend into the output whatever the dim
    /// says), no filter may attach to that leaf.
    #[tokio::test]
    async fn semi_filter_absent_when_boundary_preserves_leaf() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   RIGHT JOIN fb ON fa.k = fb.k \
                   JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fb.g = dimf.k \
                   JOIN fc ON fa.k = fc.k \
                   GROUP BY fa.g";
        let dq = chain_plan_mixed(sql, &["fa", "fb", "fc"], &["dim"]).await;
        assert!(
            !leaf_sql(&dq, "fb").contains(" IN (SELECT"),
            "fb is preserved by the RIGHT boundary — its leaf must stay unfiltered, got:\n{}",
            leaf_sql(&dq, "fb")
        );
    }

    /// Admission guards (KAN-146 — provable selectivity only): an UNFILTERED dim injects
    /// nothing, and neither does an outer (non-INNER) dim join.
    #[tokio::test]
    async fn semi_filter_absent_for_unfiltered_or_outer_dim() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let unfiltered = "SELECT fa.g, COUNT(*) AS c FROM fa \
                          JOIN dim ON fa.k = dim.k \
                          JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        let dq = chain_plan_mixed(unfiltered, &["fa", "fb"], &["dim"]).await;
        assert!(
            !leaf_sql(&dq, "fa").contains(" IN (SELECT"),
            "an unfiltered dim admits ~every fact key — no injection, got:\n{}",
            leaf_sql(&dq, "fa")
        );

        let outer = "SELECT fa.g, COUNT(*) AS c FROM fa \
                     LEFT JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fa.k = dimf.k \
                     JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        let dq = chain_plan_mixed(outer, &["fa", "fb"], &["dim"]).await;
        assert!(
            !leaf_sql(&dq, "fa").contains(" IN (SELECT"),
            "a LEFT dim join null-extends non-matching facts — no injection, got:\n{}",
            leaf_sql(&dq, "fa")
        );
    }

    /// A volatile dim filter must never be duplicated into the leaf stage (the leaf's
    /// second evaluation could disagree with the join-stage fold's).
    #[tokio::test]
    async fn semi_filter_absent_for_volatile_dim_filter() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   JOIN (SELECT k FROM dim WHERE d > rand() * 10) AS dimf ON fa.k = dimf.k \
                   JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        let dq = chain_plan_mixed(sql, &["fa", "fb"], &["dim"]).await;
        assert!(
            !leaf_sql(&dq, "fa").contains(" IN (SELECT"),
            "a volatile dim filter must not be re-evaluated in the leaf, got:\n{}",
            leaf_sql(&dq, "fa")
        );
    }

    /// `OXIDANT_SEMI_JOIN_FILTERS=0` disables the injection (byte-identical legacy plan).
    #[tokio::test]
    async fn semi_filter_env_kill_switch() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fa.k = dimf.k \
                   JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        std::env::set_var("OXIDANT_SEMI_JOIN_FILTERS", "0");
        let dq = chain_plan_mixed(sql, &["fa", "fb"], &["dim"]).await;
        std::env::remove_var("OXIDANT_SEMI_JOIN_FILTERS");
        assert!(
            !leaf_sql(&dq, "fa").contains(" IN (SELECT"),
            "the kill switch must restore the unfiltered leaf, got:\n{}",
            leaf_sql(&dq, "fa")
        );
    }

    // --- KAN-160: stats-gated semi-join filter admission --------------------------------

    /// A `MemTable` wrapper whose logical-plan statistics are overridden, so the KAN-160
    /// admission gate can be exercised without parquet footers. `None` keeps the provider's
    /// default unknown-statistics shape.
    #[derive(Debug)]
    struct StatsMemTable {
        inner: Arc<datafusion::datasource::MemTable>,
        stats: Option<datafusion::common::Statistics>,
    }

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for StatsMemTable {
        fn schema(&self) -> oxidant_loom::arrow::datatypes::SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> datafusion::logical_expr::TableType {
            datafusion::logical_expr::TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }

        fn statistics(&self) -> Option<datafusion::common::Statistics> {
            self.stats.clone()
        }
    }

    /// [`chain_plan_mixed`] with the dim registered through [`StatsMemTable`] carrying the
    /// given `num_rows` precision (`Absent` = no statistics override). Sharded tables stay on
    /// plain `MemTable` (statistics `Absent`).
    async fn chain_plan_dim_stats(
        sql: &str,
        sharded: &[&str],
        dim: &str,
        dim_rows: datafusion::common::stats::Precision<usize>,
    ) -> DistributedQuery {
        let engine = oxidant_loom::Engine::new();
        let int = DataType::Int64;
        for t in sharded {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", int.clone(), true),
                Field::new("g", int.clone(), true),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
                    Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
                ],
            )
            .unwrap();
            engine.register_batches(t, vec![batch]).unwrap();
        }
        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("k", int.clone(), true),
            Field::new("d", int.clone(), true),
        ]));
        let dim_batch = RecordBatch::try_new(
            dim_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
            ],
        )
        .unwrap();
        let mem =
            datafusion::datasource::MemTable::try_new(dim_schema.clone(), vec![vec![dim_batch]])
                .unwrap();
        let stats = match dim_rows {
            datafusion::common::stats::Precision::Absent => None,
            num_rows => Some(
                datafusion::common::Statistics::new_unknown(&dim_schema).with_num_rows(num_rows),
            ),
        };
        let table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(StatsMemTable {
            inner: Arc::new(mem),
            stats,
        });
        engine.ctx().register_table(dim, table).unwrap();
        let lp = engine.logical_plan(sql).await.unwrap();
        let p = super::super::stage_planner::peel(&lp).unwrap();
        plan_shuffle_join_chain(&p, sharded, &[dim])
            .unwrap_or_else(|e| panic!("chain planning failed for {sql}: {e}"))
    }

    /// A dim whose EXACT statistics row count exceeds
    /// `OXIDANT_SEMI_JOIN_FILTER_MAX_DIM_ROWS` (default 1M) must NOT inject — the leaf
    /// stages would hash-build a near-fact-sized key set for no proven selectivity.
    #[tokio::test]
    async fn semi_filter_rejected_when_dim_exact_rows_exceed_cap() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fa.k = dimf.k \
                   JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        let dq = chain_plan_dim_stats(
            sql,
            &["fa", "fb"],
            "dim",
            datafusion::common::stats::Precision::Exact(2_000_000),
        )
        .await;
        assert!(
            !leaf_sql(&dq, "fa").contains(" IN (SELECT"),
            "a 2M-row dim exceeds the 1M admission cap — no injection, got:\n{}",
            leaf_sql(&dq, "fa")
        );
    }

    /// A dim whose EXACT statistics row count is within the cap injects as before.
    #[tokio::test]
    async fn semi_filter_injected_when_dim_exact_rows_within_cap() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fa.k = dimf.k \
                   JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        let dq = chain_plan_dim_stats(
            sql,
            &["fa", "fb"],
            "dim",
            datafusion::common::stats::Precision::Exact(1_000_000),
        )
        .await;
        assert!(
            leaf_sql(&dq, "fa").contains("k IN (SELECT k FROM dim AS dimf WHERE (dim.d > 10))"),
            "a dim exactly at the 1M cap must still inject, got:\n{}",
            leaf_sql(&dq, "fa")
        );
    }

    /// A dim with ABSENT statistics fails open: providers without statistics keep the
    /// KAN-150 injection (the `MemTable`-backed plans stay byte-identical).
    #[tokio::test]
    async fn semi_filter_injected_when_dim_stats_absent() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fa.k = dimf.k \
                   JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        let dq = chain_plan_dim_stats(
            sql,
            &["fa", "fb"],
            "dim",
            datafusion::common::stats::Precision::Absent,
        )
        .await;
        assert!(
            leaf_sql(&dq, "fa").contains("k IN (SELECT k FROM dim AS dimf WHERE (dim.d > 10))"),
            "absent dim statistics must fail open and inject, got:\n{}",
            leaf_sql(&dq, "fa")
        );
    }

    /// An INEXACT statistics row count is rejected outright, even within the cap — the
    /// KAN-146 provable-admission discipline admits only exact counts (or absent-statistics
    /// fail-open).
    #[tokio::test]
    async fn semi_filter_rejected_when_dim_stats_inexact() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fa.k = dimf.k \
                   JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        let dq = chain_plan_dim_stats(
            sql,
            &["fa", "fb"],
            "dim",
            datafusion::common::stats::Precision::Inexact(100),
        )
        .await;
        assert!(
            !leaf_sql(&dq, "fa").contains(" IN (SELECT"),
            "an inexact dim row count must reject the injection even within the cap, got:\n{}",
            leaf_sql(&dq, "fa")
        );
    }

    /// `OXIDANT_SEMI_JOIN_FILTER_MAX_DIM_ROWS` overrides the 1M default cap: a dim whose
    /// EXACT count exceeds the default but fits the override must inject.
    #[tokio::test]
    async fn semi_filter_env_cap_override_honored() {
        let _env = SEMI_FILTER_ENV_LOCK.lock().await;
        let sql = "SELECT fa.g, COUNT(*) AS c FROM fa \
                   JOIN (SELECT k FROM dim WHERE d > 10) AS dimf ON fa.k = dimf.k \
                   JOIN fb ON fa.k = fb.k GROUP BY fa.g";
        std::env::set_var("OXIDANT_SEMI_JOIN_FILTER_MAX_DIM_ROWS", "4000000");
        let dq = chain_plan_dim_stats(
            sql,
            &["fa", "fb"],
            "dim",
            datafusion::common::stats::Precision::Exact(2_000_000),
        )
        .await;
        std::env::remove_var("OXIDANT_SEMI_JOIN_FILTER_MAX_DIM_ROWS");
        assert!(
            leaf_sql(&dq, "fa").contains("k IN (SELECT k FROM dim AS dimf WHERE (dim.d > 10))"),
            "a 2M-row dim fits the 4M override cap — the injection must be admitted, got:\n{}",
            leaf_sql(&dq, "fa")
        );
    }
}
