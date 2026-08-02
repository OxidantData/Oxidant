//! Recursive branch-aware lowering for plans whose aggregates live below a `CrossJoin` (or a
//! `LEFT`/`RIGHT`/`FULL` join chaining several independently-aggregated branches together).
//!
//! The core splitter's [`super::stage_planner::peel`] is deliberately a fast linear walk to one
//! aggregate. CTE-heavy plans instead have a tree of independently distributable aggregate
//! branches under one or more joins. This module materializes those sharded branches as stage
//! sub-DAGs, replaces them with the worker's `shuffle_input[_i]` tables, and unparses the
//! remaining outer plan as one gathered output stage.
//!
//! Two branch refinements (KAN-41, TPC-DS Q11/Q58/Q78 at SF10):
//!
//! - **Replicated-only aggregate branches are materialized, not inlined.** A branch whose
//!   inputs are all replicated (Q58's `cs_items`/`ws_items`, Q78's `cs`/`ws` arms) used to stay
//!   inline in the gathered outer stage — which runs once per shuffle partition, so the full
//!   replicated-fact scan + aggregate was recomputed `WEFT_SHUFFLE_PARTITIONS` times (16× at
//!   SF10) with only partition 0's copy ever used. Such a branch now becomes a single
//!   `Forward` stage: computed exactly once on one worker (every worker holds the replicated
//!   inputs), its exact output gathered to partition 0 like any sharded branch's.
//! - **Identical branches are planned once.** A CTE self-joined N times (Q11's `year_total` ×4)
//!   inlines as N structurally identical subtrees; deduplicating by plan fingerprint (volatile
//!   expressions excluded — they must re-evaluate per reference) leaves one sub-DAG whose
//!   shuffle output every outer placeholder pulls.
//! - **Branches sharing one scan merge into one sub-DAG.** TPC-DS Q88's eight time-bucket
//!   `count(*)` aggregates differ only in their `time_dim` predicates, so the identical-branch
//!   fingerprint keeps them distinct and each plans its own sharded scan + aggregate sub-DAG
//!   (17 stages, eight full fact scans). Branches whose peeled aggregate inputs are structurally
//!   identical *modulo their filter predicates* now plan together: one leaf stage scans the
//!   shared tail once, computing every branch's partial aggregates as
//!   `agg(…) FILTER (WHERE <branch predicate>)`, and one combine stage emits all branches'
//!   values as columns. `COUNT(DISTINCT)`-carrying branches (TPC-DS Q28) can't compute in a
//!   FILTER-merged leaf — they keep their own sub-DAGs.
//!
//! ## Why any join type at the outer skeleton is safe
//!
//! Each materialized branch's final stage always writes with empty `hash_key_cols`, so — per
//! [`crate::shuffle::partition::hash_partition`]'s "empty key list = global gather" rule — its
//! *entire* output lands in partition 0 and every other rendezvous partition of the gathered
//! outer stage sees zero rows from it. A `CROSS`/`INNER` join of such branches is therefore
//! trivially empty on partitions `1..w` (whichever side is empty makes an inner-style join
//! empty), which is why the original cross-join-only admission never needed to check anything
//! beyond "at least one branch exists". A `LEFT`/`RIGHT`/`FULL` join is *not* automatically safe:
//! it must additionally have its *preserved* side(s) gated to zero on `1..w`, or a replicated
//! (never-empty) preserved side would re-emit its full contents once per worker. See
//! [`is_branch_gated`], which [`try_branch_dag`] enforces before accepting a non-cross outer
//! skeleton.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};

use super::shape_extensions::ensure_subquery_tables_replicated;
use super::stage_planner::{
    base_tables, count_table_scans, expr_sql, extract_from_tail, flatten_and_conjuncts,
    partial_combine_sql, peel, plan_contains_distinct, plan_distributed_logical,
    reject_unsafe_broadcast_shapes, sanitize_generated_sql, strip_alias, AggSpec, DistributedQuery,
};
use crate::driver::{ExchangeMode, StageDef};

/// Try to lower a plan containing a cross-join tree into independent branch sub-DAGs followed by
/// one gathered outer stage. Returns `Ok(None)` when no cross join (or no sharded branch) exists.
pub(crate) fn try_branch_dag(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    // Only split when the first branching node under the outer unary chain is a combinable join
    // (CROSS/INNER, or a LEFT/RIGHT/FULL chaining several aggregate branches — e.g. TPC-DS Q78's
    // `ss LEFT JOIN ws LEFT JOIN cs`). A join buried inside an aggregate or UNION arm belongs to
    // that branch's own planner; lifting it would either gather raw fact rows or duplicate
    // replicated UNION arms.
    if !first_branching_node(lp).is_some_and(is_combinable_join) {
        return Ok(None);
    }

    let mut branches = Vec::new();
    collect_sharded_branches(lp, replicated, &mut branches);
    // At least one genuinely sharded branch must exist; a skeleton whose branches are all
    // replicated-only belongs to the replicated-table paths, not to this splitter.
    if !branches.iter().any(|b| b.kind == BranchKind::Sharded) {
        return Ok(None);
    }

    let branch_count = branches.len();
    let branch_by_node: HashMap<usize, usize> = branches
        .iter()
        .enumerate()
        .map(|(i, b)| (node_id(b.node), i))
        .collect();

    // KAN-41 (TPC-DS Q11): one CTE self-joined N times inlines as N structurally identical
    // branch subtrees. Plan each distinct branch ONCE and point every occurrence's outer
    // placeholder at the same shuffle output; without this the fact scans + aggregates of the
    // CTE run N times over the full sub-DAG. Only deterministic branches (no volatile
    // expressions) may share an evaluation — a `rand()`-bearing branch is re-evaluated per
    // occurrence single-node, so deduplicating it would change results.
    let mut rep_of: Vec<usize> = (0..branch_count).collect();
    let mut fp_to_rep: HashMap<String, usize> = HashMap::new();
    for (i, b) in branches.iter().enumerate() {
        if let Some(fp) = branch_fingerprint(b.node) {
            if let Some(&r) = fp_to_rep.get(&fp) {
                rep_of[i] = r;
            } else {
                fp_to_rep.insert(fp, i);
            }
        }
    }
    let mut reps: Vec<usize> = Vec::new();
    for &r in &rep_of {
        if !reps.contains(&r) {
            reps.push(r);
        }
    }

    for &r in &reps {
        reject_mixed_union_branch(branches[r].node, replicated)?;
    }

    // TPC-DS Q88: representatives whose peeled aggregate inputs are structurally identical
    // modulo their filter predicates would otherwise each plan their own scan + aggregate
    // sub-DAG (Q88: eight full store_sales scans, 17 stages). Group them into one shared-scan
    // sub-DAG — one leaf computing every branch's partials as
    // `agg(…) FILTER (WHERE <branch predicate>)`, one combine emitting all branches' columns —
    // and point every member's outer placeholder at the shared combine output. Ineligible
    // branches (DISTINCT-carrying, GROUP BY, HAVING, volatile, …) keep their own sub-DAGs.
    // Merging requires the outer skeleton to re-assert the original output columns explicitly:
    // a merged group's combine row carries every member branch's columns, and a wildcard outer
    // would expand them all at every placeholder position.
    let (merged_dqs, merged_members) = if skeleton_has_output_projection(lp) {
        plan_merge_groups(&branches, &reps, replicated)
    } else {
        (HashMap::new(), HashMap::new())
    };
    let merged: HashSet<usize> = merged_members.values().flatten().copied().collect();

    let mut rep_queries: HashMap<usize, DistributedQuery> = HashMap::with_capacity(reps.len());
    for (leader, dq) in merged_dqs {
        rep_queries.insert(leader, dq);
    }
    for &r in &reps {
        if merged.contains(&r) {
            continue;
        }
        let branch = &branches[r];
        let dq = match branch.kind {
            BranchKind::Sharded => {
                let dq = plan_branch(branch.node, replicated).map_err(|e| {
                    Error::Unsupported(format!(
                        "auto-distribute: branch-aware CrossJoin branch {r} is not distributable: {e}"
                    ))
                })?;
                // Only the branch's own *output* stage matters here: it is the one whose stage id
                // becomes the outer skeleton's upstream (via `append_branch`), so it alone must satisfy
                // the "empty hash key ⇒ everything gathers to partition 0" invariant the rest of this
                // module relies on (see the module doc). An *intermediate* Forward stage feeding that
                // output — e.g. a UNION arm scanning only replicated tables, run once via
                // `try_split_broadcast_union` / `plan_union`'s zero-sharded-arm path and then combined
                // with the sharded arm(s) inside this same sub-DAG — is exactly the safe "run once,
                // shuffle its (already-exact) contribution" pattern the driver already implements for
                // Forward producer stages, so it is not rejected here.
                if dq
                    .stages
                    .last()
                    .is_some_and(|s| s.exchange == ExchangeMode::Forward)
                {
                    return Err(Error::Unsupported(format!(
                        "auto-distribute: branch-aware CrossJoin branch {r} scans a sharded table \
                         but outputs via Forward exchange; its sharded input must flow through \
                         hash-shuffled stages, not a single-worker forward"
                    )));
                }
                dq
            }
            // KAN-41 (TPC-DS Q58/Q78): an aggregate branch over only *replicated* tables (e.g.
            // Q58's `cs_items` over catalog_sales) must NOT stay inline in the gathered outer
            // stage: that stage runs once per shuffle partition, so the full replicated-fact
            // scan + join + aggregate would be recomputed `WEFT_SHUFFLE_PARTITIONS` times
            // (16× at SF10) while only partition 0's copy is ever used. Materialize it as one
            // `Forward` stage instead — computed exactly once (the driver's Forward producers
            // run on a single worker), its already-exact output gathered to partition 0 like
            // any other branch output.
            BranchKind::ReplicatedAggregate => forward_branch_query(branch.node, r)?,
        };
        rep_queries.insert(r, dq);
    }

    let rewritten = replace_branches(lp, &branch_by_node, branch_count)?.0;
    if !is_branch_gated(&rewritten) {
        return Err(Error::Unsupported(
            "auto-distribute: branch-aware outer join is not guaranteed empty on non-driving \
             worker partitions (a replicated-only input sits on a LEFT/RIGHT/FULL join's \
             preserved side) — would duplicate rows once per worker"
                .into(),
        ));
    }
    reject_remaining_sharded_scans(&rewritten, replicated)?;
    let outer_sql = Unparser::default()
        .plan_to_sql(&rewritten)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse branch-aware CrossJoin outer plan: {e}"
            ))
        })?
        .to_string();

    let mut stages = Vec::new();
    let mut next_id = 0u32;
    let mut rep_output: HashMap<usize, u32> = HashMap::with_capacity(reps.len());
    for &r in &reps {
        if rep_output.contains_key(&r) {
            continue; // already emitted as part of a shared-scan merge group
        }
        let dq = rep_queries.remove(&r).expect("representative planned");
        let output = append_branch(&mut stages, &mut next_id, dq, r)?;
        rep_output.insert(r, output);
        if let Some(members) = merged_members.get(&r) {
            // Every member's placeholder points at the shared combine output, exactly like a
            // deduplicated identical branch's occurrences.
            for &m in members {
                rep_output.insert(m, output);
            }
        }
    }
    // One upstream per *occurrence* (positions map to `shuffle_input_{i}` names on the worker);
    // deduplicated occurrences repeat their representative's output stage id, which the worker
    // simply pulls once per position.
    let upstream_stage_ids: Vec<u32> = (0..branch_count).map(|i| rep_output[&rep_of[i]]).collect();

    stages.push(StageDef::new(
        next_id,
        sanitize_generated_sql(&outer_sql),
        upstream_stage_ids,
        vec![],
    ));
    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: None,
    }))
}

/// One collected branch of the outer join skeleton.
#[derive(Debug)]
struct Branch<'a> {
    node: &'a LogicalPlan,
    kind: BranchKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BranchKind {
    /// Scans at least one sharded table; planned through the shape planners into a sub-DAG.
    Sharded,
    /// An aggregate branch over only replicated tables; materialized as a single `Forward`
    /// stage so its full replicated-fact scan runs once, not once per shuffle partition.
    ReplicatedAggregate,
}

/// Materialize a replicated-only aggregate branch as one `Forward` stage: every worker holds
/// the full replicated inputs, so any single worker computes the exact branch output.
fn forward_branch_query(branch: &LogicalPlan, branch_i: usize) -> Result<DistributedQuery> {
    let sql = Unparser::default()
        .plan_to_sql(branch)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse replicated aggregate branch {branch_i}: {e}"
            ))
        })?
        .to_string();
    Ok(DistributedQuery {
        stages: vec![StageDef {
            stage_id: 0,
            sql: sanitize_generated_sql(&sql),
            upstream_stage_ids: vec![],
            hash_key_cols: vec![],
            exchange: ExchangeMode::Forward,
            plan_fragment: None,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
        }],
        finalize_sql: None,
    })
}

/// One branch representative eligible for shared-scan merging: a bare global aggregate (through
/// `SubqueryAlias` wrappers) over a tail structurally identical to other branches' tails once
/// every `Filter` is factored out (TPC-DS Q88's eight time-bucket counts over the same star
/// join). `None` from [`mergeable_branch`] for anything a FILTER-merged leaf cannot reproduce.
struct MergeableBranch {
    /// The branch's aggregates, classified (never DISTINCT).
    aggs: Vec<AggSpec>,
    /// SQL of the branch's full row predicate — the AND of every stripped `Filter` conjunct —
    /// or `None` when the branch carries no filter.
    predicate_sql: Option<String>,
    /// Output column names the shared combine stage re-aliases this branch's values to.
    out_names: Vec<String>,
    /// Fingerprint of the filter-stripped tail (the merge group key).
    tail_fp: String,
    /// The filter-stripped aggregate input shared by the whole group.
    tail: LogicalPlan,
}

/// Group merge-eligible representative branches by their shared filter-stripped tail and plan
/// one shared-scan sub-DAG per group of ≥2. Returns each merged sub-DAG keyed by its group's
/// first representative, plus that representative's full member list. A group whose merged
/// planning fails falls back to ordinary per-branch sub-DAGs.
fn plan_merge_groups(
    branches: &[Branch<'_>],
    reps: &[usize],
    replicated: &[&str],
) -> (HashMap<usize, DistributedQuery>, HashMap<usize, Vec<usize>>) {
    let mut mergeable: HashMap<usize, MergeableBranch> = HashMap::new();
    let mut fp_to_group: HashMap<String, Vec<usize>> = HashMap::new();
    for &r in reps {
        if branches[r].kind != BranchKind::Sharded {
            continue;
        }
        if let Some(m) = mergeable_branch(branches[r].node, replicated) {
            fp_to_group.entry(m.tail_fp.clone()).or_default().push(r);
            mergeable.insert(r, m);
        }
    }

    let mut dqs = HashMap::new();
    let mut members_of = HashMap::new();
    for group in fp_to_group.into_values() {
        if group.len() < 2 {
            continue;
        }
        // Every member's outputs share the one combine row; duplicate column names would
        // collide there, so such a group stays on per-branch sub-DAGs.
        let mut seen = HashSet::new();
        if !group
            .iter()
            .flat_map(|r| mergeable[r].out_names.iter())
            .all(|name| seen.insert(name))
        {
            continue;
        }
        let members: Vec<&MergeableBranch> = group.iter().map(|r| &mergeable[r]).collect();
        match merged_shared_scan_query(&members) {
            Ok(dq) => {
                dqs.insert(group[0], dq);
                members_of.insert(group[0], group);
            }
            Err(e) => {
                if std::env::var("WEFT_TPCDS_DEBUG").is_ok() {
                    eprintln!("[plan-debug] shared-scan branch merge declined: {e}");
                }
            }
        }
    }
    (dqs, members_of)
}

/// Classify a branch representative for shared-scan merging. `None` for anything a
/// FILTER-merged leaf cannot reproduce: a GROUP BY / HAVING / ORDER BY / LIMIT above the
/// aggregate, an output projection doing more than renaming the aggregate's outputs in order,
/// a DISTINCT aggregate (TPC-DS Q28 — it keeps its co-located shuffle sub-DAG), a volatile
/// expression, an unqualified or multiply-qualified output schema, or a tail that isn't a
/// single-scan broadcast shape over INNER joins only.
fn mergeable_branch(branch: &LogicalPlan, replicated: &[&str]) -> Option<MergeableBranch> {
    let p = peel(branch).ok()?;
    if p.sort.is_some()
        || p.limit.is_some()
        || !p.having.is_empty()
        || !p.alias_projections.is_empty()
        || !p.agg.group_expr.is_empty()
        || plan_contains_volatile(branch)
    {
        return None;
    }
    // The output projection may only rename the aggregate's outputs, in order (Q88's
    // `count(*) AS h8_30_to_9` aliasing) — the shared combine re-aliases each recombined value
    // to that name. Any other expression over the aggregate output belongs to the branch's own
    // sub-DAG.
    if let Some(exprs) = p.projection {
        let renames_in_order = exprs.len() == p.agg.aggr_expr.len()
            && exprs.iter().zip(&p.agg.aggr_expr).all(|(proj, agg)| {
                strip_alias(proj).schema_name().to_string()
                    == strip_alias(agg).schema_name().to_string()
            });
        if !renames_in_order {
            return None;
        }
    }
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()
        .ok()?;
    if aggs.iter().any(|a| a.distinct) {
        return None;
    }
    // The merged outer projection references branch outputs as `alias.column`: an unqualified
    // or multiply-qualified schema cannot be re-aliased unambiguously, and duplicate names
    // would collide on the shared combine row.
    let schema = branch.schema();
    let mut qualifier = None;
    let mut out_names = Vec::with_capacity(schema.fields().len());
    for (q, field) in schema.iter() {
        match (&qualifier, q) {
            (None, Some(q)) => qualifier = Some(q.clone()),
            (Some(prev), Some(q)) if prev == q => {}
            _ => return None,
        }
        out_names.push(field.name().clone());
    }
    {
        let mut seen = HashSet::new();
        if !out_names.iter().all(|name| seen.insert(name)) {
            return None;
        }
    }

    let (tail, conjuncts) = strip_filters(&p.agg.input)?;
    // The shared tail must broadcast like any single-sharded aggregate input: exactly one
    // sharded base table scanned once, only INNER joins (a predicate stripped off a preserved
    // outer-join side would not commute to a post-join FILTER), no DISTINCT, and no
    // sharded-table subqueries (checked on the original input, predicates included).
    let tables = base_tables(&tail);
    let sharded: Vec<&str> = tables
        .iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .map(String::as_str)
        .collect();
    if sharded.len() != 1
        || count_table_scans(&tail, sharded[0]) != 1
        || !only_inner_joins(&tail)
        || plan_contains_distinct(&tail)
        || reject_unsafe_broadcast_shapes(&tail, sharded[0]).is_err()
        || ensure_subquery_tables_replicated(&p.agg.input, &sharded, replicated).is_err()
    {
        return None;
    }

    let up = Unparser::default();
    let predicate_sql = if conjuncts.is_empty() {
        None
    } else {
        let mut parts = Vec::with_capacity(conjuncts.len());
        for c in &conjuncts {
            parts.push(format!("({})", expr_sql(&up, c).ok()?));
        }
        Some(parts.join(" AND "))
    };
    let tail_fp = format!("{tail}");
    Some(MergeableBranch {
        aggs,
        predicate_sql,
        out_names,
        tail_fp,
        tail,
    })
}

/// Remove every `Filter` node from `lp`, returning the stripped plan plus the removed conjuncts
/// (AND-able, in tree order). `None` when a node refuses to rebuild — the caller simply
/// declines the merge. The conjuncts are only sound to reapply as a post-join row predicate
/// when every join below is INNER; [`mergeable_branch`] checks that on the stripped tail.
fn strip_filters(lp: &LogicalPlan) -> Option<(LogicalPlan, Vec<Expr>)> {
    if let LogicalPlan::Filter(f) = lp {
        let (input, mut conjuncts) = strip_filters(&f.input)?;
        let mut mine = Vec::new();
        flatten_and_conjuncts(&f.predicate, &mut mine);
        mine.append(&mut conjuncts);
        return Some((input, mine));
    }
    let inputs = lp.inputs();
    if inputs.is_empty() {
        return Some((lp.clone(), Vec::new()));
    }
    let mut conjuncts = Vec::new();
    let mut stripped = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (plan, mut preds) = strip_filters(input)?;
        conjuncts.append(&mut preds);
        stripped.push(plan);
    }
    let plan = lp.with_new_exprs(lp.expressions(), stripped).ok()?;
    Some((plan, conjuncts))
}

/// Every join in `lp` is INNER, so a stripped row predicate commutes past it unchanged.
fn only_inner_joins(lp: &LogicalPlan) -> bool {
    let local = match lp {
        LogicalPlan::Join(join) => join.join_type == JoinType::Inner,
        _ => true,
    };
    local && lp.inputs().iter().all(|input| only_inner_joins(input))
}

/// Plan one shared sub-DAG for a merge group: a single leaf stage scans the group's
/// filter-stripped tail once, computing every member branch's partial aggregates as
/// `agg(…) FILTER (WHERE <branch predicate>)`, and one gather-combine stage recombines each
/// branch's partials into its output columns (aliased back to the branch's original output
/// names, which the outer placeholders resolve through their own aliases). The FILTER clause
/// round-trips through the workers' Databricks-dialect parser (covered end-to-end by the Q88
/// shape test). Both stages gather to partition 0 like any global aggregation.
fn merged_shared_scan_query(members: &[&MergeableBranch]) -> Result<DistributedQuery> {
    let input_sql = Unparser::default()
        .plan_to_sql(&members[0].tail)
        .map_err(|e| {
            Error::Unsupported(format!("auto-distribute: unparse shared branch tail: {e}"))
        })?
        .to_string();
    let tail = sanitize_generated_sql(&extract_from_tail(&input_sql)?);

    let mut psel = Vec::new();
    let mut combine = Vec::new();
    let mut n = 0usize;
    for member in members {
        let filter = member
            .predicate_sql
            .as_ref()
            .map(|p| format!(" FILTER (WHERE {p})"))
            .unwrap_or_default();
        for (a, out_name) in member.aggs.iter().zip(&member.out_names) {
            psel.extend(partial_filter_items(&a.func, n, &a.arg_sql, &filter)?);
            let (_sel, comb) = partial_combine_sql(&a.func, n, &a.arg_sql)?;
            let expr = comb.strip_suffix(&format!(" AS r{n}")).ok_or_else(|| {
                Error::Unsupported("auto-distribute: unexpected aggregate combine fragment".into())
            })?;
            combine.push(format!("{expr} AS \"{out_name}\""));
            n += 1;
        }
    }

    let leaf_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", psel.join(", ")));
    // The empty-bucket synthetic row reads as NULLs / zero counts; HAVING COUNT(*) > 0 keeps
    // only partition 0's real row (same guard as the single-branch global path).
    let combine_sql = sanitize_generated_sql(&format!(
        "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
        combine.join(", ")
    ));
    Ok(DistributedQuery {
        stages: vec![
            StageDef::new(0, leaf_sql, vec![], vec![]),
            StageDef::new(1, combine_sql, vec![0], vec![]),
        ],
        finalize_sql: None,
    })
}

/// [`partial_combine_sql`]'s partial SELECT fragments with an aggregate `FILTER (WHERE …)`
/// clause spliced onto each partial (a shared-scan leaf gates every branch's partials to that
/// branch's predicate). `n` is the flat aggregate position across the whole merge group.
fn partial_filter_items(func: &str, n: usize, arg_sql: &str, filter: &str) -> Result<Vec<String>> {
    match func {
        "sum" | "count" | "min" | "max" => Ok(vec![format!("{func}({arg_sql}){filter} AS a{n}")]),
        "avg" => Ok(vec![format!(
            "sum({arg_sql}){filter} AS a{n}s, count({arg_sql}){filter} AS a{n}c"
        )]),
        "stddev" | "var" | "stddev_pop" | "var_pop" => Ok(vec![format!(
            "sum({arg_sql}){filter} AS a{n}s, sum(({arg_sql})*({arg_sql})){filter} AS a{n}q, \
             count({arg_sql}){filter} AS a{n}c"
        )]),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: aggregate `{other}` not supported"
        ))),
    }
}

/// Whether the outer skeleton re-asserts the query's output columns in an explicit `Projection`
/// above the branching node (DataFusion expands `SELECT *` at plan time, so this is the norm).
/// Shared-scan merging requires it: a merged group's combine row carries every member branch's
/// columns, and a wildcard outer stage would over-expand that wider row at every placeholder.
fn skeleton_has_output_projection(lp: &LogicalPlan) -> bool {
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Projection(_) => return true,
            LogicalPlan::Limit(l) => node = &l.input,
            LogicalPlan::Sort(s) => node = &s.input,
            LogicalPlan::Filter(f) => node = f.input.as_ref(),
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            _ => return false,
        }
    }
}

/// Structural fingerprint for branch deduplication: the full plan tree below any top-level
/// `SubqueryAlias` wrappers (self-join aliases like `t_s_firstyear` / `t_s_secyear` must not
/// defeat identity). `None` for branches carrying volatile expressions — those may never
/// share an evaluation (see the dedup note in [`try_branch_dag`]).
fn branch_fingerprint(branch: &LogicalPlan) -> Option<String> {
    if plan_contains_volatile(branch) {
        return None;
    }
    let mut inner = branch;
    while let LogicalPlan::SubqueryAlias(alias) = inner {
        inner = alias.input.as_ref();
    }
    Some(format!("{inner}"))
}

/// Whether any expression in the plan (including subquery plans) contains a volatile scalar
/// function (`rand()`, `now()`, …) whose result may differ across evaluations.
pub(crate) fn plan_contains_volatile(lp: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

    let mut volatile = false;
    let _ = lp.apply(|node| {
        if volatile {
            return Ok(TreeNodeRecursion::Stop);
        }
        for expr in node.expressions() {
            let _ = expr.apply(|e| {
                if let Expr::ScalarFunction(f) = e {
                    if f.func.signature().volatility
                        == datafusion::logical_expr::Volatility::Volatile
                    {
                        volatile = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
        Ok(if volatile {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    volatile
}

/// A `UNION ALL` arm that scans only replicated tables is not itself unsafe to mix with a sharded
/// arm: [`plan_branch`] (via [`super::stage_planner::try_split_broadcast_union`] when the union
/// feeds a `GROUP BY`, or [`super::shape_extensions::try_union_all`]'s zero-sharded-arm handling
/// when the union sits at the branch's own top) places that arm on exactly one worker
/// (`ExchangeMode::Forward`) and shuffles its single, already-exact contribution alongside the
/// sharded arm's genuine per-worker partials — the same "run once, don't replicate" composition
/// [`is_branch_gated`]'s docs describe for a replicated `LEFT`/`RIGHT` join side. That combine is
/// only sound for `UNION ALL`: it never needs to notice a row that also appears in another arm.
///
/// A plain `UNION` (`DISTINCT`) is different — DataFusion lowers it to `Distinct` wrapping
/// `Union` — because deduplication is defined over *all* arms together. Splitting a mixed
/// sharded/replicated union into two independently-planned halves and only then `UNION ALL`-ing
/// their results (as the two paths above do) could keep a row that should have been deduplicated
/// against a row that landed in the other half. Keep that shape an honest rejection.
///
/// The one exception: a distinct union sitting **directly under an `Aggregate`** (through
/// `SubqueryAlias` wrappers) with raw row-source arms is *not* split by sharding at all — it is
/// planned whole by [`super::stage_planner::aggregate_over_distinct_union_stages`] (KAN-49a,
/// TPC-DS Q75), which co-locates identical rows on one partition *before* dedup, preserving
/// cross-arm deduplication exactly. That node (and only that node) skips the rejection.
fn reject_mixed_union_branch(branch: &LogicalPlan, replicated: &[&str]) -> Result<()> {
    reject_mixed_union(branch, replicated, false)
}

/// `at_aggregate_input` is true exactly when `lp` sits directly under an `Aggregate` through
/// `SubqueryAlias` wrappers only — the same chain
/// [`super::stage_planner::aggregate_over_distinct_union_stages`] strips before planning the
/// co-located dedup, so the guard skips precisely the nodes that composition handles.
fn reject_mixed_union(
    lp: &LogicalPlan,
    replicated: &[&str],
    at_aggregate_input: bool,
) -> Result<()> {
    if let LogicalPlan::Distinct(distinct) = lp {
        if let LogicalPlan::Union(union) = distinct.input().as_ref() {
            if union_has_mixed_sharding(union, replicated) {
                if at_aggregate_input && union_arms_are_raw_row_sources(union) {
                    return Ok(());
                }
                return Err(Error::Unsupported(
                    "auto-distribute: branch-aware CrossJoin UNION (DISTINCT) has a \
                     replicated-table-only arm plus a sharded-table arm; splitting a distinct \
                     union by sharding cannot preserve cross-arm deduplication"
                        .into(),
                ));
            }
        }
    }
    let child_at_aggregate_input = match lp {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::SubqueryAlias(_) => at_aggregate_input,
        _ => false,
    };
    for input in lp.inputs() {
        reject_mixed_union(input, replicated, child_at_aggregate_input)?;
    }
    Ok(())
}

/// Every leaf arm of the union is a raw row source (no aggregate / window / distinct / nested
/// union), the same condition [`super::stage_planner::aggregate_over_distinct_union_stages`]
/// requires before planning the co-located dedup. Nested distinct unions flatten first (dedup
/// is idempotent), mirroring the hook.
fn union_arms_are_raw_row_sources(union: &datafusion::logical_expr::Union) -> bool {
    fn is_raw(lp: &LogicalPlan) -> bool {
        match lp {
            LogicalPlan::Aggregate(_) | LogicalPlan::Window(_) => return false,
            LogicalPlan::Distinct(d) => return is_raw(d.input().as_ref()),
            LogicalPlan::Union(u) => return u.inputs.iter().all(|arm| is_raw(arm.as_ref())),
            _ => {}
        }
        lp.inputs().iter().all(|c| is_raw(c))
    }
    union.inputs.iter().all(|arm| is_raw(arm.as_ref()))
}

fn union_has_mixed_sharding(union: &datafusion::logical_expr::Union, replicated: &[&str]) -> bool {
    let mut has_sharded_arm = false;
    let mut has_replicated_arm = false;
    for arm in &union.inputs {
        let tables = base_tables(arm);
        if tables
            .iter()
            .any(|table| !replicated.contains(&table.as_str()))
        {
            has_sharded_arm = true;
        } else if !tables.is_empty() {
            has_replicated_arm = true;
        }
    }
    has_sharded_arm && has_replicated_arm
}

fn plan_branch(branch: &LogicalPlan, replicated: &[&str]) -> Result<DistributedQuery> {
    match plan_distributed_logical(branch, replicated) {
        ok @ Ok(_) => ok,
        Err(primary) => {
            // Set-op detection intentionally looks through only its own top-level wrappers. CTE
            // references add one or more SubqueryAlias nodes around the same schema; stripping
            // those is safe because qualifiers are restored on the outer placeholder.
            let mut inner = branch;
            while let LogicalPlan::SubqueryAlias(alias) = inner {
                inner = alias.input.as_ref();
            }
            if std::ptr::eq(inner, branch) {
                Err(primary)
            } else {
                plan_distributed_logical(inner, replicated)
            }
        }
    }
}

/// Walk down through plain pass-through wrappers (`Limit`/`Sort`/`Projection`/`Filter`/
/// `SubqueryAlias` — exactly the node types [`super::stage_planner::peel`] also tunnels through)
/// looking for the first multi-input node. Stops (returns `None`) at any *other* single-input node
/// — notably `Aggregate` / `Window` / `Distinct` — instead of tunneling through it: those are plan
/// boundaries whose own input belongs to that node's own branch planner, not to a join skeleton
/// several levels up. Blindly tunneling through them used to let [`collect_sharded_branches`] dig
/// past a branch's `WindowAggr` straight into its raw pre-aggregation join, silently dropping the
/// window computation and materializing a branch with the wrong (pre-window) schema.
fn first_branching_node(mut lp: &LogicalPlan) -> Option<&LogicalPlan> {
    loop {
        let is_wrapper = matches!(
            lp,
            LogicalPlan::Limit(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::SubqueryAlias(_)
        );
        let inputs = lp.inputs();
        match inputs.as_slice() {
            [] => return None,
            [input] if is_wrapper => lp = input,
            [_] => return None,
            _ => return Some(lp),
        }
    }
}

/// A join whose outer-skeleton use is (subject to [`is_branch_gated`] for the non-`Inner` cases)
/// safe to leave in the gathered outer stage: `CROSS`/`INNER` (the original shape this splitter
/// was built for) or a `LEFT`/`RIGHT`/`FULL` chaining independently-materialized branches (e.g.
/// TPC-DS Q78's `ss LEFT JOIN ws LEFT JOIN cs`, one arm per fact table).
fn is_combinable_join(lp: &LogicalPlan) -> bool {
    use datafusion::logical_expr::JoinType;
    matches!(
        lp,
        LogicalPlan::Join(join)
            if matches!(join.join_type, JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full)
    )
}

/// Whether `lp` — a node in the *rewritten* outer skeleton, after branches have been substituted
/// with `shuffle_input`/`shuffle_input_N` placeholders — is guaranteed to produce zero rows
/// whenever every placeholder is empty (see the module doc for why every non-zero placeholder
/// partition other than partition 0 needs this). `CROSS`/`INNER`/semi joins only need *either*
/// side gated (matches the pre-existing cross-join behavior, which never needed this check because
/// the non-empty `branch_nodes` guard above already guarantees at least one side of an all-`Inner`
/// join tree is a branch). `LEFT`/`RIGHT` additionally require the *preserved* side gated — the
/// non-preserved side may safely stay a fully replicated table computed fresh every partition,
/// since it never survives into the output once the preserved side is empty. `FULL` needs both.
fn is_branch_gated(lp: &LogicalPlan) -> bool {
    use datafusion::logical_expr::JoinType;
    match lp {
        LogicalPlan::TableScan(scan) => {
            let name = scan.table_name.table();
            name == "shuffle_input" || name.starts_with("shuffle_input_")
        }
        LogicalPlan::Join(join) => match join.join_type {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => {
                is_branch_gated(&join.left) || is_branch_gated(&join.right)
            }
            JoinType::Left | JoinType::LeftAnti | JoinType::LeftMark => is_branch_gated(&join.left),
            JoinType::Right | JoinType::RightAnti | JoinType::RightMark => {
                is_branch_gated(&join.right)
            }
            JoinType::Full => is_branch_gated(&join.left) && is_branch_gated(&join.right),
        },
        LogicalPlan::Union(union) => union.inputs.iter().all(|i| is_branch_gated(i)),
        LogicalPlan::Filter(f) => is_branch_gated(&f.input),
        LogicalPlan::Projection(p) => is_branch_gated(&p.input),
        LogicalPlan::SubqueryAlias(s) => is_branch_gated(&s.input),
        LogicalPlan::Limit(l) => is_branch_gated(&l.input),
        LogicalPlan::Sort(s) => is_branch_gated(&s.input),
        // Conservative default: an unrecognized node sitting where gating matters (e.g. an
        // Aggregate re-grouping a branch — a global `COUNT(*)` over zero rows is one row, not
        // zero) is treated as *not* gated rather than risk a false "safe".
        _ => false,
    }
}

/// Collect the maximal sharded subplans below the cross-join skeleton, plus replicated-only
/// *aggregate* branches (see [`BranchKind::ReplicatedAggregate`]). Unary nodes above or between
/// cross joins remain in the skeleton so their expressions retain the original branch qualifiers.
/// Replicated-only leaves that are cheap to re-evaluate per partition (plain dimension scans /
/// filters) also remain there: evaluating them beside the gathered sharded inputs is correct and
/// avoids a Forward round-trip for data that is already local.
fn collect_sharded_branches<'a>(
    lp: &'a LogicalPlan,
    replicated: &[&str],
    out: &mut Vec<Branch<'a>>,
) {
    // Stop at an aggregate branch the linear planner already understands. Its input often contains
    // fact/dimension cross joins of its own; descending through those would gather raw fact rows
    // before aggregating and defeat the purpose of this splitter.
    if peel(lp).is_ok() {
        // Scan counting must include expression subqueries: a branch whose *join inputs* are all
        // replicated but whose IN/EXISTS/scalar subquery reads a sharded table is NOT safe to
        // materialize as a single Forward stage — that stage runs on one worker and would read
        // only that worker's shard of the fact.
        let mut tables = HashSet::new();
        collect_tables_with_subqueries(lp, &mut tables);
        if tables
            .iter()
            .any(|table| !replicated.contains(&table.as_str()))
        {
            out.push(Branch {
                node: lp,
                kind: BranchKind::Sharded,
            });
        } else if !tables.is_empty() && !plan_contains_volatile(lp) {
            out.push(Branch {
                node: lp,
                kind: BranchKind::ReplicatedAggregate,
            });
        }
        return;
    }

    let inputs = lp.inputs();
    let unary_leads_to_join = matches!(
        inputs.as_slice(),
        [input] if first_branching_node(input).is_some_and(is_combinable_join)
    );
    if is_combinable_join(lp) || unary_leads_to_join {
        for input in inputs {
            collect_sharded_branches(input, replicated, out);
        }
        return;
    }

    let tables = base_tables(lp);
    if tables
        .iter()
        .any(|table| !replicated.contains(&table.as_str()))
    {
        out.push(Branch {
            node: lp,
            kind: BranchKind::Sharded,
        });
    }
}

pub(crate) fn node_id(lp: &LogicalPlan) -> usize {
    lp as *const LogicalPlan as usize
}

/// A materialized branch must remove every scan of its sharded fact from the outer SQL. Scans left
/// in expression subqueries are especially dangerous: they would read only partition 0's local
/// shard and silently return incomplete results.
fn reject_remaining_sharded_scans(lp: &LogicalPlan, replicated: &[&str]) -> Result<()> {
    let mut tables = HashSet::new();
    collect_tables_with_subqueries(lp, &mut tables);
    let mut sharded: Vec<_> = tables
        .into_iter()
        .filter(|table| {
            table != "shuffle_input"
                && !table.starts_with("shuffle_input_")
                && !replicated.contains(&table.as_str())
        })
        .collect();
    sharded.sort();
    if sharded.is_empty() {
        Ok(())
    } else {
        Err(Error::Unsupported(format!(
            "auto-distribute: branch-aware CrossJoin outer plan still scans unmaterialized \
             sharded table(s) {sharded:?} (possibly in a scalar/correlated subquery)"
        )))
    }
}

fn collect_tables_with_subqueries(lp: &LogicalPlan, out: &mut HashSet<String>) {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

    if let LogicalPlan::TableScan(scan) = lp {
        out.insert(scan.table_name.table().to_string());
    }
    for input in lp.inputs() {
        collect_tables_with_subqueries(input, out);
    }
    for expr in lp.expressions() {
        let _ = expr.apply(|node| {
            let subquery = match node {
                Expr::Exists(exists) => Some(&exists.subquery.subquery),
                Expr::InSubquery(in_subquery) => Some(&in_subquery.subquery.subquery),
                Expr::ScalarSubquery(scalar) => Some(&scalar.subquery),
                _ => None,
            };
            if let Some(plan) = subquery {
                collect_tables_with_subqueries(plan, out);
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
}

/// Replace selected branches while preserving every other node. The bool reports whether this
/// subtree changed, avoiding unnecessary reconstruction (and schema validation) of untouched
/// replicated branches.
pub(crate) fn replace_branches(
    lp: &LogicalPlan,
    branch_by_node: &HashMap<usize, usize>,
    branch_count: usize,
) -> Result<(LogicalPlan, bool)> {
    if let Some(&branch_i) = branch_by_node.get(&node_id(lp)) {
        return Ok((placeholder_plan(lp, branch_i, branch_count)?, true));
    }

    let inputs = lp.inputs();
    if inputs.is_empty() {
        return Ok((lp.clone(), false));
    }

    let mut changed = false;
    let mut rewritten_inputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (rewritten, input_changed) = replace_branches(input, branch_by_node, branch_count)?;
        changed |= input_changed;
        rewritten_inputs.push(rewritten);
    }
    if !changed {
        return Ok((lp.clone(), false));
    }

    let rebuilt = lp
        .with_new_exprs(lp.expressions(), rewritten_inputs)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: rebuild branch-aware CrossJoin outer plan: {e}"
            ))
        })?;
    Ok((rebuilt, true))
}

pub(crate) fn placeholder_plan(
    branch: &LogicalPlan,
    branch_i: usize,
    branch_count: usize,
) -> Result<LogicalPlan> {
    let table_name = if branch_count == 1 {
        "shuffle_input".to_string()
    } else {
        format!("shuffle_input_{branch_i}")
    };
    let source = Arc::new(LogicalTableSource::new(Arc::new(
        branch.schema().as_arrow().clone(),
    )));
    let scan = LogicalPlanBuilder::scan(table_name, source, None)
        .and_then(|builder| builder.build())
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: build CrossJoin branch {branch_i} placeholder: {e}"
            ))
        })?;

    // CTE / derived-table branches normally have one qualifier. Reapply it so expressions in the
    // untouched outer plan still resolve, while the unparser emits `shuffle_input_i AS old_alias`.
    let qualifiers: HashSet<_> = branch
        .schema()
        .iter()
        .filter_map(|(qualifier, _)| qualifier.cloned())
        .collect();
    match qualifiers.len() {
        0 => Ok(scan),
        1 => {
            let alias = qualifiers.into_iter().next().expect("one qualifier");
            LogicalPlanBuilder::from(scan)
                .alias(alias)
                .and_then(|builder| builder.build())
                .map_err(|e| {
                    Error::Unsupported(format!(
                        "auto-distribute: alias CrossJoin branch {branch_i} placeholder: {e}"
                    ))
                })
        }
        n => Err(Error::Unsupported(format!(
            "auto-distribute: CrossJoin branch {branch_i} output spans {n} qualifiers; \
             add a projection/alias before the branch boundary"
        ))),
    }
}

fn append_branch(
    stages: &mut Vec<StageDef>,
    next_id: &mut u32,
    dq: DistributedQuery,
    branch_i: usize,
) -> Result<u32> {
    if dq.stages.is_empty() {
        return Err(Error::Unsupported(format!(
            "auto-distribute: branch-aware CrossJoin branch {branch_i} produced no stages"
        )));
    }

    let id_offset = *next_id;
    let mut output_id = None;
    for stage in dq.stages {
        let new_id = stage.stage_id.checked_add(id_offset).ok_or_else(|| {
            Error::Unsupported("auto-distribute: branch stage id overflow".into())
        })?;
        let upstream_stage_ids = stage
            .upstream_stage_ids
            .into_iter()
            .map(|id| {
                id.checked_add(id_offset).ok_or_else(|| {
                    Error::Unsupported("auto-distribute: branch upstream id overflow".into())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        stages.push(StageDef {
            stage_id: new_id,
            sql: stage.sql,
            upstream_stage_ids,
            hash_key_cols: stage.hash_key_cols,
            exchange: stage.exchange,
            plan_fragment: stage.plan_fragment,
            lakehouse_snapshot_pins: stage.lakehouse_snapshot_pins,
            replicated_tables: String::new(),
        });
        output_id = Some(new_id);
        *next_id = (*next_id).max(new_id.checked_add(1).ok_or_else(|| {
            Error::Unsupported("auto-distribute: branch stage id overflow".into())
        })?);
    }

    let mut output_id = output_id.expect("non-empty stages");
    if let Some(finalize_sql) = dq.finalize_sql {
        let rewritten = finalize_sql.replacen("FROM result", "FROM shuffle_input", 1);
        if rewritten == finalize_sql {
            return Err(Error::Unsupported(format!(
                "auto-distribute: CrossJoin branch {branch_i} finalize does not read `result`"
            )));
        }
        let finalize_id = *next_id;
        stages.push(StageDef::new(
            finalize_id,
            sanitize_generated_sql(&rewritten),
            vec![output_id],
            vec![],
        ));
        output_id = finalize_id;
        *next_id = (*next_id).checked_add(1).ok_or_else(|| {
            Error::Unsupported("auto-distribute: branch finalize stage id overflow".into())
        })?;
    }
    Ok(output_id)
}

#[cfg(test)]
mod tests {
    use weft_loom::arrow::array::{Int64Array, RecordBatch, StringArray};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::Engine;

    use super::*;

    fn fact() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 0])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap()
    }

    fn dim() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dk", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1])),
                Arc::new(StringArray::from(vec!["zero", "one"])),
            ],
        )
        .unwrap()
    }

    async fn logical_plan(sql: &str) -> LogicalPlan {
        let engine = Engine::new();
        engine.register_batches("t", vec![fact()]).unwrap();
        engine.register_batches("d", vec![dim()]).unwrap();
        engine.logical_plan(sql).await.unwrap()
    }

    #[tokio::test]
    async fn two_aggregate_ctes_form_independent_sub_dags() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("public planner should split");

        assert_eq!(dq.stages.len(), 5);
        assert_eq!(dq.stages.last().unwrap().upstream_stage_ids, vec![1, 3]);
        let outer = &dq.stages.last().unwrap().sql;
        assert!(outer.contains("shuffle_input_0"), "{outer}");
        assert!(outer.contains("shuffle_input_1"), "{outer}");
        assert!(dq.finalize_sql.is_none());
    }

    #[tokio::test]
    async fn replicated_cross_join_leaf_stays_in_outer_stage() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT a.k, a.s, d.label FROM a CROSS JOIN d",
        )
        .await;
        let dq = try_branch_dag(&lp, &["d"])
            .expect("split")
            .expect("cross join plan");

        assert_eq!(dq.stages.len(), 3);
        assert_eq!(dq.stages.last().unwrap().upstream_stage_ids, vec![1]);
        let outer = &dq.stages.last().unwrap().sql;
        assert!(outer.contains("shuffle_input"), "{outer}");
        assert!(outer.contains(" d"), "{outer}");
    }

    #[tokio::test]
    async fn unsupported_branch_reports_its_index_and_cause() {
        // A branch that the *inner* shape planners reject for a reason the gather fallback also
        // declines (ROLLUP+UNION) must surface as a branch-indexed error from try_branch_dag —
        // not as a silent Ok(None) or an unindexed shape error.
        let lp = logical_plan(
            "WITH a AS (\
                 SELECT k, SUM(v) AS s FROM (\
                   SELECT k, v FROM t WHERE k = 1 \
                   UNION ALL \
                   SELECT k, v FROM t WHERE k = 2\
                 ) AS x GROUP BY ROLLUP(k)\
             ), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let err = match try_branch_dag(&lp, &[]) {
            Err(e) => e,
            Ok(None) => panic!("expected branch-dag to attempt and fail the ROLLUP+UNION branch"),
            Ok(Some(_)) => panic!("ROLLUP+UNION branch must not distribute via branch-dag"),
        };
        let msg = err.to_string();
        assert!(msg.contains("branch-aware CrossJoin branch"), "{msg}");
    }

    #[tokio::test]
    async fn count_distinct_cross_join_gathers_via_materialize_fallback() {
        // plan_distributed_logical must not leave COUNT(DISTINCT)×CrossJoin as a hard failure:
        // the gather fallback evaluates the full fact once on partition 0 (TPC-DS Q28).
        let lp = logical_plan(
            "WITH a AS (SELECT COUNT(DISTINCT v) AS n FROM t), \
             b AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT a.n, b.k, b.s FROM a CROSS JOIN b",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("gather fallback");
        assert!(
            dq.stages.len() >= 3,
            "expected gather/gate/eval stages, got {dq:?}"
        );
    }

    /// `UNION ALL` of two per-arm aggregates — one over the sharded table, one over a replicated
    /// table (TPC-DS Q4/Q11/Q74's `year_total` CTE in miniature) — is safe to split: the
    /// replicated arm has zero sharded tables in its own `GROUP BY` input, so
    /// [`super::super::shape_extensions::try_union_all`]'s zero-sharded-arm handling runs it once
    /// (`ExchangeMode::Forward`) and shuffles its (already-exact) output alongside the sharded
    /// arm's genuine per-worker partials. Only the branch's own *output* stage matters to the
    /// outer skeleton (see [`try_branch_dag`]'s Forward check), and that stage still gathers with
    /// an empty hash key like any other branch.
    #[tokio::test]
    async fn mixed_union_all_branch_is_split_by_sharding() {
        let lp = logical_plan(
            "WITH a AS (\
                 SELECT k, SUM(v) AS s FROM t GROUP BY k \
                 UNION ALL \
                 SELECT dk AS k, SUM(1) AS s FROM d GROUP BY dk\
             ), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let dq = try_branch_dag(&lp, &["d"])
            .expect("split")
            .expect("cross join plan");
        assert!(
            dq.stages
                .iter()
                .any(|s| s.exchange == ExchangeMode::Forward),
            "expected a Forward stage for the replicated-only UNION ALL arm: {dq:?}"
        );
        let branch_a_output = dq
            .stages
            .iter()
            .find(|s| s.sql.contains("shuffle_input_0") && s.sql.contains("shuffle_input_1"))
            .expect("branch a's UNION ALL gather stage");
        assert!(
            branch_a_output.hash_key_cols.is_empty(),
            "{branch_a_output:?}"
        );
    }

    /// A plain `UNION` (`DISTINCT`) mixing a sharded arm with a replicated-only arm used to be an
    /// honest rejection: DataFusion lowers it to `Distinct` wrapping `Union`, and splitting into
    /// two independently-planned halves combined with `UNION ALL` would not reproduce
    /// deduplication across the two halves. KAN-49a plans it exactly instead — every arm's raw
    /// rows are hash-shuffled on the full row so duplicates co-locate *before* the per-partition
    /// `DISTINCT` (see `aggregate_over_distinct_union_stages`).
    #[tokio::test]
    async fn mixed_union_distinct_branch_plans_co_located_dedup() {
        let lp = logical_plan(
            "WITH a AS (\
                 SELECT k, SUM(v) AS s \
                 FROM (\
                     SELECT k, v FROM t \
                     UNION \
                     SELECT dk AS k, 1 AS v FROM d\
                 ) mixed \
                 GROUP BY k\
             ), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let dq = try_branch_dag(&lp, &["d"])
            .expect("split")
            .expect("distinct union branch plans via the co-located dedup");
        assert!(
            dq.stages.iter().any(|s| s.sql.contains("SELECT DISTINCT")),
            "expected the co-located dedup stage: {dq:?}"
        );
        assert!(
            dq.stages
                .iter()
                .any(|s| s.exchange == ExchangeMode::Forward),
            "expected a Forward stage for the replicated-only UNION arm: {dq:?}"
        );
    }

    /// …but only while every arm is a raw row source. An arm carrying its own aggregate cannot
    /// use the co-located dedup, and splitting the distinct union by sharding stays rejected.
    #[tokio::test]
    async fn mixed_union_distinct_branch_with_aggregated_arm_is_rejected() {
        let lp = logical_plan(
            "WITH a AS (\
                 SELECT k, SUM(v) AS s \
                 FROM (\
                     SELECT k, SUM(v) AS v FROM t GROUP BY k \
                     UNION \
                     SELECT dk AS k, 1 AS v FROM d\
                 ) mixed \
                 GROUP BY k\
             ), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let err =
            try_branch_dag(&lp, &["d"]).expect_err("a non-raw distinct UNION arm can't be split");
        let msg = err.to_string();
        assert!(msg.contains("UNION (DISTINCT)"), "{msg}");
        assert!(msg.contains("cross-arm deduplication"), "{msg}");
    }

    #[tokio::test]
    async fn union_of_only_replicated_arms_is_not_rejected() {
        let lp = logical_plan(
            "SELECT dk AS k, 1 AS v FROM d \
             UNION ALL \
             SELECT dk AS k, 2 AS v FROM d",
        )
        .await;
        reject_mixed_union_branch(&lp, &["d"]).expect("both UNION arms are replicated-only");
    }

    #[tokio::test]
    async fn first_branching_node_stops_at_aggregate_boundary() {
        // Projection -> Aggregate -> Filter -> Join(t, d). The join sits *below* the aggregate, so
        // it belongs to the aggregate's own branch planner, not to a join skeleton several levels
        // up. Before the fix, `first_branching_node` treated every single-input node (including
        // `Aggregate`) as a pass-through wrapper and would tunnel straight through it to this join.
        let lp = logical_plan("SELECT k, SUM(v) AS s FROM t, d WHERE t.k = d.dk GROUP BY k").await;
        assert!(
            first_branching_node(&lp).is_none(),
            "must stop at the Aggregate boundary instead of tunneling into its input"
        );
    }

    #[tokio::test]
    async fn left_join_of_two_aggregate_branches_is_distributed() {
        // TPC-DS Q78's shape in miniature: two independently-aggregated branches over the same
        // sharded fact, chained with a LEFT JOIN instead of a CROSS JOIN.
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k), \
             b AS (SELECT k, COUNT(*) AS n FROM t GROUP BY k) \
             SELECT a.k, a.s, b.n FROM a LEFT JOIN b ON a.k = b.k",
        )
        .await;
        let dq =
            plan_distributed_logical(&lp, &[]).expect("left join of two branches should split");

        assert_eq!(dq.stages.len(), 5);
        let outer = &dq.stages.last().unwrap().sql;
        assert!(outer.to_uppercase().contains("LEFT OUTER JOIN"), "{outer}");
        assert!(outer.contains("shuffle_input_0"), "{outer}");
        assert!(outer.contains("shuffle_input_1"), "{outer}");
    }

    #[tokio::test]
    async fn left_join_with_branch_on_preserved_side_is_accepted() {
        // The aggregate branch sits on the LEFT (preserved) side and the replicated dimension sits
        // on the non-preserved side — safe, since the branch is gated to empty on partitions 1..w.
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT a.k, a.s, d.label FROM a LEFT JOIN d ON a.k = d.dk",
        )
        .await;
        let dq = try_branch_dag(&lp, &["d"])
            .expect("split")
            .expect("left join plan");

        assert_eq!(dq.stages.len(), 3);
        let outer = &dq.stages.last().unwrap().sql;
        assert!(outer.to_uppercase().contains("LEFT OUTER JOIN"), "{outer}");
        assert!(outer.contains("shuffle_input"), "{outer}");
        assert!(outer.contains(" d"), "{outer}");
    }

    #[tokio::test]
    async fn left_join_with_replicated_preserved_side_is_rejected() {
        // The replicated dimension sits on the LEFT (preserved) side; it is never empty, so every
        // worker would re-emit its full contents once per worker if this were accepted.
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT d.dk, a.s FROM d LEFT JOIN a ON d.dk = a.k",
        )
        .await;
        let err = try_branch_dag(&lp, &["d"])
            .expect_err("replicated preserved side must not be accepted as gated");
        let msg = err.to_string();
        assert!(msg.contains("preserved side"), "{msg}");
        assert!(msg.contains("duplicate rows once per worker"), "{msg}");
    }

    #[tokio::test]
    async fn remaining_sharded_scan_in_outer_plan_is_rejected() {
        // After materializing branches, any leftover scan of a sharded fact (including inside an
        // EXISTS/scalar subquery) would read only partition 0 and silently drop rows.
        let lp = logical_plan("SELECT k FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.k = t.k)")
            .await;
        let err = reject_remaining_sharded_scans(&lp, &[]).expect_err("sharded t still present");
        let msg = err.to_string();
        assert!(msg.contains("unmaterialized sharded table"), "{msg}");
        assert!(
            msg.contains("\"t\"") || msg.contains("`t`") || msg.contains("t"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn remaining_scan_of_only_replicated_tables_is_allowed() {
        let lp = logical_plan("SELECT dk FROM d").await;
        reject_remaining_sharded_scans(&lp, &["d"]).expect("replicated dim may remain in outer");
    }

    /// KAN-41 (TPC-DS Q11): self-join aliases (`a`/`b`) wrap structurally identical subtrees —
    /// their fingerprints must match so the branch is planned once.
    #[tokio::test]
    async fn fingerprint_ignores_top_level_alias() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT x.k, x.s, y.s FROM a x JOIN a y ON x.k = y.k",
        )
        .await;
        let mut branches = Vec::new();
        collect_sharded_branches(&lp, &[], &mut branches);
        assert_eq!(branches.len(), 2, "two CTE references: {branches:?}");
        let f0 = branch_fingerprint(branches[0].node);
        let f1 = branch_fingerprint(branches[1].node);
        assert!(f0.is_some() && f0 == f1, "aliases must not defeat identity");
    }

    /// A branch carrying a volatile expression may never share one evaluation across its
    /// occurrences — single-node re-evaluates it per reference, so deduplication would change
    /// results. Its fingerprint is `None`, keeping every occurrence its own sub-DAG.
    #[tokio::test]
    async fn volatile_branch_has_no_fingerprint() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) + random() * 0 AS s FROM t GROUP BY k) \
             SELECT x.k, x.s, y.s FROM a x JOIN a y ON x.k = y.k",
        )
        .await;
        let mut branches = Vec::new();
        collect_sharded_branches(&lp, &[], &mut branches);
        assert_eq!(branches.len(), 2, "two CTE references: {branches:?}");
        for b in &branches {
            assert!(
                branch_fingerprint(b.node).is_none(),
                "volatile branch must not be deduplicated"
            );
        }
        // ...and the plan keeps one sub-DAG per occurrence (partial+combine each, plus outer).
        let dq = plan_distributed_logical(&lp, &[]).expect("volatile self-join still plans");
        assert_eq!(
            dq.stages.len(),
            5,
            "volatile branches are not deduplicated: {dq:?}"
        );
    }

    /// KAN-41 (TPC-DS Q58): an aggregate branch over only replicated tables is collected for
    /// Forward materialization; a plain replicated scan stays inline in the outer stage.
    #[tokio::test]
    async fn replicated_aggregate_branch_is_collected_but_plain_scan_is_not() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k), \
             b AS (SELECT dk AS k, SUM(1) AS n FROM d GROUP BY dk) \
             SELECT a.k, a.s, b.n, d.label FROM a JOIN b ON a.k = b.k CROSS JOIN d",
        )
        .await;
        let mut branches = Vec::new();
        collect_sharded_branches(&lp, &["d"], &mut branches);
        let kinds: Vec<_> = branches.iter().map(|b| b.kind).collect();
        assert!(
            kinds.contains(&BranchKind::Sharded),
            "sharded aggregate branch: {kinds:?}"
        );
        assert!(
            kinds.contains(&BranchKind::ReplicatedAggregate),
            "replicated aggregate branch: {kinds:?}"
        );
        assert_eq!(
            branches.len(),
            2,
            "the plain `d` scan stays in the outer stage: {kinds:?}"
        );
    }

    /// A branch whose *join inputs* are replicated but whose IN subquery reads the sharded fact
    /// must NOT be Forward-materialized: the Forward stage runs on one worker and would read
    /// only that worker's shard of `t`. It classifies as sharded, so the shape planners either
    /// distribute it honestly or decline the query to the gather fallback.
    #[tokio::test]
    async fn replicated_branch_with_sharded_subquery_is_not_forward_materialized() {
        let lp = logical_plan(
            "WITH a AS (SELECT dk AS k, SUM(1) AS n FROM d WHERE dk IN (SELECT k FROM t) GROUP BY dk), \
             b AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT a.k, a.n, b.s FROM a JOIN b ON a.k = b.k",
        )
        .await;
        let mut branches = Vec::new();
        collect_sharded_branches(&lp, &["d"], &mut branches);
        assert!(
            branches.iter().all(|b| b.kind == BranchKind::Sharded),
            "no branch may be classified replicated-aggregate: {branches:?}"
        );
    }
}
