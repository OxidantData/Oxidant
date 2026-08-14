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
//!   replicated-fact scan + aggregate was recomputed `OXIDANT_SHUFFLE_PARTITIONS` times (16× at
//!   SF10) with only partition 0's copy ever used. Such a branch now becomes a single
//!   `Forward` stage: computed exactly once on one worker (every worker holds the replicated
//!   inputs), its exact output gathered to partition 0 like any sharded branch's. On a
//!   multi-worker cluster the branch instead fans out (KAN-156): a per-worker partial
//!   aggregate over a disjoint 1/W file slice of the branch's anchor table, hash-shuffled by
//!   the group key, plus the ordinary recombine — same exactness argument as a sharded fact's
//!   partial, since the anchor's slice alone partitions the branch's joined rows.
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
//!   values as columns. `COUNT(DISTINCT)`-carrying branches (TPC-DS Q28's six bucket aggregates
//!   over `store_sales`) merge the same way when every branch's DISTINCT argument is the *same
//!   column*: the shared leaf GROUPs BY that argument (one scan, narrowed to the OR of the
//!   branch predicates), carrying each branch's FILTER'd partials plus a per-branch predicate
//!   marker, and the co-located distinct machinery recomputes every branch's exact distinct
//!   count from the deduped groups. Branches whose DISTINCT arguments differ (or whose DISTINCT
//!   aggregate isn't a count) would need a GROUP BY over the argument product — cardinality
//!   multiplication — so they keep their own sub-DAGs.
//!
//! ## Why any join type at the outer skeleton is safe
//!
//! By default each materialized branch's final stage writes with empty `hash_key_cols`, so — per
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
//!
//! The one exception to the always-gather default is [`outer_keying`]: when the outer skeleton
//! is an equijoin tree over the branch outputs whose keys are all branch-output columns (TPC-DS
//! Q4/Q39/Q78), the branch outputs hash-shuffle by those keys instead and the outer stage runs
//! key-partitioned on every worker — the per-partition equijoin of co-located slices is exactly
//! the global equijoin restricted to that key bucket. The admission rule is deliberately
//! conservative; every non-admitted shape keeps the byte-identical gather plan above.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::unparser::Unparser;
use oxidant_common::{Error, Result};

use super::shape_extensions::{build_outer_finalize, ensure_subquery_tables_replicated};
use super::stage_planner::{
    base_tables, build_remap, count_table_scans, expr_sql, extract_from_tail,
    flatten_and_conjuncts, flattened_group_exprs, is_grouping_set, partial_combine_sql, peel,
    plan_contains_distinct, plan_distributed_logical, recombine_partial_state_sql,
    recombine_stage_sql, reject_unsafe_broadcast_shapes, sanitize_generated_sql,
    sliced_replicate_stamp, strip_alias, AggSpec, DistributedQuery,
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
    // branches (DISTINCT-incompatible, GROUP BY, HAVING, volatile, …) keep their own sub-DAGs.
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
            // scan + join + aggregate would be recomputed `OXIDANT_SHUFFLE_PARTITIONS` times
            // (16× at SF10) while only partition 0's copy is ever used. Materialize it as one
            // `Forward` stage instead — computed exactly once (the driver's Forward producers
            // run on a single worker), its already-exact output gathered to partition 0 like
            // any other branch output. KAN-156: on a multi-worker cluster the branch instead
            // fans out — per-worker partials over disjoint file slices of the branch's anchor
            // table plus the ordinary recombine — when the shape allows (see
            // [`sliced_branch_query`]).
            BranchKind::ReplicatedAggregate => replicated_branch_query(branch.node, replicated, r)?,
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
    // TPC-DS Q4/Q39/Q78: when the outer skeleton over the branch outputs is an equijoin tree
    // keyed on branch-output columns, hash-shuffle the branch outputs by those keys instead of
    // gathering each to partition 0, and run the outer stage key-partitioned on every worker.
    // `None` keeps the byte-identical gather plan (see [`outer_keying`]).
    let keying = outer_keying(&rewritten, &rep_of, &merged, &rep_queries);
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
        if let Some(keys) = keying.as_ref().and_then(|k| k.rep_keys.get(&r)) {
            // Re-target the branch's output shuffle at the skeleton's equijoin key columns.
            // The stage's output rows are the branch's output columns in schema order (the
            // same assumption the window-over-join planner's terminal retarget relies on).
            stages
                .iter_mut()
                .find(|s| s.stage_id == output)
                .expect("branch output stage appended")
                .hash_key_cols = keys.clone();
        }
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
        // The outer stage is the driver-pulled output stage (`produce = false`): its own
        // `hash_key_cols` is never used to partition anything. What matters is that its
        // *upstreams* hash-shuffled by the join keys (stamped above), so each rendezvous
        // partition of this stage already sees a co-located key slice of every branch.
        vec![],
    ));
    Ok(Some(DistributedQuery {
        stages,
        // Keyed admission with an outer `ORDER BY`/`LIMIT`: each partition's copy of the
        // outer stage keeps its own top-k, and the driver-side finalize merges them (the
        // standard two-phase TopK — `build_outer_finalize` re-sorts/re-limits the
        // concatenation). The gather plan keeps `None`: partition 0 held every row, so the
        // in-stage `ORDER BY`/`LIMIT` was already global.
        finalize_sql: keying.and_then(|k| k.finalize),
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
    /// stage so its full replicated-fact scan runs once, not once per shuffle partition — or,
    /// on a multi-worker cluster, as a sliced per-worker partial + recombine (KAN-156, see
    /// [`sliced_branch_query`]).
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

/// KAN-156: fan a replicated-only aggregate branch out across workers when the shape allows,
/// falling back to the single-`Forward` materialization otherwise.
fn replicated_branch_query(
    branch: &LogicalPlan,
    replicated: &[&str],
    branch_i: usize,
) -> Result<DistributedQuery> {
    if let Some(dq) = sliced_branch_query(branch, replicated) {
        return Ok(dq);
    }
    forward_branch_query(branch, branch_i)
}

/// The fanned-out form of a replicated-only aggregate branch (TPC-DS Q78's `ws`/`cs` arms,
/// Q58's per-channel item arms — SF100 profile: 292 s of single-`Forward`-task work on the cs
/// arm alone): a per-worker **partial** aggregate over a disjoint 1/W file slice of the
/// branch's anchor table, hash-shuffled by the group key, then the ordinary recombine — the
/// same stage pair [`recombine_stage_sql`] builds for a sharded fact. The anchor's slice alone
/// partitions the branch's joined rows (every other replicated table stays whole on each
/// worker, so the arm's joins — including a `LEFT ANTI` whose preserved side is the anchor —
/// stay co-located within the slice), and the per-slice partials recombine associatively, so
/// the output is byte-identical in semantics to the single-`Forward` branch.
///
/// `None` — the caller keeps `Forward` — for anything the two-stage form cannot reproduce
/// exactly: not a peel-able plain grouped aggregate, a `DISTINCT` aggregate, grouping sets
/// (their gather/two-phase variants are not composed here), an `ORDER BY`/`LIMIT` on the
/// branch, an intervening alias projection, no safe slice anchor, or a degenerate all-sliced
/// stamp (see [`sliced_replicate_stamp`]). The branch's combine stage is the sub-DAG output:
/// [`outer_keying`] may still re-target its hash key at the skeleton's equijoin columns, and
/// its gather default matches the `Forward` placement it replaces.
fn sliced_branch_query(branch: &LogicalPlan, replicated: &[&str]) -> Option<DistributedQuery> {
    fn build(branch: &LogicalPlan, replicated: &[&str]) -> Result<DistributedQuery> {
        let p = peel(branch)?;
        if p.sort.is_some() || p.limit.is_some() || !p.alias_projections.is_empty() {
            return Err(Error::Unsupported("branch top not plain".into()));
        }
        let agg = p.agg;
        if agg.group_expr.is_empty() || is_grouping_set(&agg.group_expr) {
            return Err(Error::Unsupported(
                "branch aggregate not a plain group-by".into(),
            ));
        }
        let aggs = agg
            .aggr_expr
            .iter()
            .map(AggSpec::classify)
            .collect::<Result<Vec<_>>>()?;
        if aggs.iter().any(|a| a.distinct) {
            return Err(Error::Unsupported("DISTINCT aggregate".into()));
        }
        let stamp = sliced_replicate_stamp(&agg.input, replicated)
            .ok_or_else(|| Error::Unsupported("no safe slice anchor".into()))?;
        let up = Unparser::default();
        let group_sql: Vec<String> = flattened_group_exprs(&agg.group_expr)
            .into_iter()
            .map(|g| expr_sql(&up, g))
            .collect::<Result<_>>()?;
        let input_sql = up
            .plan_to_sql(&agg.input)
            .map_err(|e| Error::Unsupported(format!("unparse branch input: {e}")))?
            .to_string();
        let tail = sanitize_generated_sql(&extract_from_tail(&input_sql)?);
        let remap = build_remap(&p);
        let (partial_sql, final_sql) = recombine_stage_sql(&p, &group_sql, &aggs, &tail, &remap)?;
        let hash_key_cols: Vec<u32> = (0..group_sql.len() as u32).collect();
        let mut partial = StageDef::new(0, partial_sql, vec![], hash_key_cols);
        partial.replicated_tables = stamp;
        let combine = StageDef::new(1, final_sql, vec![0], vec![]);
        Ok(DistributedQuery {
            stages: vec![partial, combine],
            finalize_sql: None,
        })
    }
    build(branch, replicated).ok()
}

/// One branch representative eligible for shared-scan merging: a bare global aggregate (through
/// `SubqueryAlias` wrappers) over a tail structurally identical to other branches' tails once
/// every `Filter` is factored out (TPC-DS Q88's eight time-bucket counts over the same star
/// join). `None` from [`mergeable_branch`] for anything a FILTER-merged leaf cannot reproduce.
struct MergeableBranch {
    /// The branch's aggregates, classified.
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
    /// `Some(arg)` when the branch carries `COUNT(DISTINCT arg)` aggregates — all of them over
    /// this one argument (TPC-DS Q28). Distinct-carrying branches merge only with siblings
    /// sharing the argument: the merged leaf GROUPs BY it, so branches with different arguments
    /// (or non-count DISTINCT aggregates) stay on their own sub-DAGs. Part of the group key.
    distinct_arg: Option<String>,
}

/// Group merge-eligible representative branches by their shared filter-stripped tail (and, for
/// `COUNT(DISTINCT)`-carrying branches, their shared DISTINCT argument) and plan one
/// shared-scan sub-DAG per group of ≥2. Returns each merged sub-DAG keyed by its group's
/// first representative, plus that representative's full member list. A group whose merged
/// planning fails falls back to ordinary per-branch sub-DAGs.
fn plan_merge_groups(
    branches: &[Branch<'_>],
    reps: &[usize],
    replicated: &[&str],
) -> (HashMap<usize, DistributedQuery>, HashMap<usize, Vec<usize>>) {
    let mut mergeable: HashMap<usize, MergeableBranch> = HashMap::new();
    let mut fp_to_group: HashMap<(String, Option<String>), Vec<usize>> = HashMap::new();
    for &r in reps {
        if branches[r].kind != BranchKind::Sharded {
            continue;
        }
        if let Some(m) = mergeable_branch(branches[r].node, replicated) {
            fp_to_group
                .entry((m.tail_fp.clone(), m.distinct_arg.clone()))
                .or_default()
                .push(r);
            mergeable.insert(r, m);
        }
    }

    let mut dqs = HashMap::new();
    let mut members_of = HashMap::new();
    for ((_tail_fp, distinct_arg), group) in fp_to_group {
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
        let planned = match &distinct_arg {
            Some(arg) => merged_shared_distinct_scan_query(&members, arg),
            None => merged_shared_scan_query(&members),
        };
        match planned {
            Ok(dq) => {
                dqs.insert(group[0], dq);
                members_of.insert(group[0], group);
            }
            Err(e) => {
                if std::env::var("OXIDANT_TPCDS_DEBUG").is_ok() {
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
/// a volatile expression, an unqualified or multiply-qualified output schema, a tail that isn't
/// a single-scan broadcast shape over INNER joins only, or DISTINCT aggregates the merged leaf
/// cannot recombine — only `COUNT(DISTINCT arg)` aggregates all sharing one argument (TPC-DS
/// Q28) are mergeable, and only with siblings carrying the same argument.
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
    // A DISTINCT-carrying branch (TPC-DS Q28) merges only through the co-located dedup leaf,
    // which GROUPs BY one shared argument: every DISTINCT aggregate must be a count over that
    // one argument (the same restriction [`super::stage_planner::global_distinct_aggregation_stages`]
    // applies to a lone branch). Mixed arguments would multiply the grouping cardinality;
    // non-count DISTINCT cannot recombine from per-group markers.
    let first_distinct = aggs.iter().find(|a| a.distinct);
    let distinct_arg = match first_distinct {
        None => None,
        Some(first) => {
            if aggs
                .iter()
                .any(|a| a.distinct && (a.func != "count" || a.arg_sql != first.arg_sql))
            {
                return None;
            }
            Some(first.arg_sql.clone())
        }
    };
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
        distinct_arg,
    })
}

/// Remove every `Filter` node from `lp`, returning the stripped plan plus the removed conjuncts
/// (AND-able, in tree order). `None` when a node refuses to rebuild — the caller simply
/// declines the merge. The conjuncts are only sound to reapply as a post-join row predicate
/// when every join below is INNER; [`mergeable_branch`] checks that on the stripped tail.
///
/// KAN-158 (`gather_shapes` Q23 shared-scan CSE) also uses this to compare an unrestricted
/// outer aggregate body against a filter-restricted sibling (sq2) without re-planning either.
pub(crate) fn strip_filters(lp: &LogicalPlan) -> Option<(LogicalPlan, Vec<Expr>)> {
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
pub(crate) fn only_inner_joins(lp: &LogicalPlan) -> bool {
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

/// Plan one shared sub-DAG for a merge group whose members carry `COUNT(DISTINCT <arg>)` over a
/// single shared argument (TPC-DS Q28's six bucket aggregates over `store_sales`). The group
/// reuses the lone-branch co-located machinery's three-stage shape (see
/// [`super::stage_planner::global_distinct_aggregation_stages`]) with **one** scan of the tail:
///
/// 1. **Partial dedup** (`stage 0`, hash-shuffled by the argument): scans the shared tail once —
///    narrowed to the OR of every member's row predicate (a row matching no member contributes
///    to no aggregate) — and GROUPs BY the shared DISTINCT argument. Per member it carries the
///    recombinable partial state of the member's plain aggregates as
///    `agg(…) FILTER (WHERE <member predicate>)`, plus one marker
///    `count(*) FILTER (WHERE <member predicate>) AS f{k}` recording whether the group holds any
///    row the member's predicate matched.
/// 2. **Per-partition aggregate** (`stage 1`): `count(DISTINCT CASE WHEN f{k} > 0 THEN c END)`
///    is exact by co-location — a group contributes to member `k`'s distinct count iff any
///    co-located row matched that member's predicate — plus a recombine of the partial state.
/// 3. **Gather-combine** (`stage 2`): `sum(d{k})` per distinct aggregate and the plain
///    recombines, aliased to each member's original output names; `HAVING COUNT(*) > 0`
///    suppresses the synthetic empty-partition row, same as the other global paths.
fn merged_shared_distinct_scan_query(
    members: &[&MergeableBranch],
    distinct_arg: &str,
) -> Result<DistributedQuery> {
    let input_sql = Unparser::default()
        .plan_to_sql(&members[0].tail)
        .map_err(|e| {
            Error::Unsupported(format!("auto-distribute: unparse shared branch tail: {e}"))
        })?
        .to_string();
    let tail = sanitize_generated_sql(&extract_from_tail(&input_sql)?);

    // The leaf's select list leads with the DISTINCT argument (hash key position 0), then
    // carries each member's FILTER'd partial state and predicate marker. `n` is the flat
    // non-DISTINCT partial position across the whole group; `k` is the member position.
    let mut psel = vec![format!("{distinct_arg} AS c")];
    let mut mid_sel = Vec::new();
    let mut combine = Vec::new();
    let mut n = 0usize;
    for (k, member) in members.iter().enumerate() {
        let filter = member
            .predicate_sql
            .as_ref()
            .map(|p| format!(" FILTER (WHERE {p})"))
            .unwrap_or_default();
        psel.push(format!("count(*){filter} AS f{k}"));
        mid_sel.push(format!(
            "count(DISTINCT CASE WHEN f{k} > 0 THEN c END) AS d{k}"
        ));
        for (a, out_name) in member.aggs.iter().zip(&member.out_names) {
            if a.distinct {
                combine.push(format!("sum(d{k}) AS \"{out_name}\""));
                continue;
            }
            psel.extend(partial_filter_items(&a.func, n, &a.arg_sql, &filter)?);
            mid_sel.extend(recombine_partial_state_sql(&a.func, n)?);
            let (_sel, comb) = partial_combine_sql(&a.func, n, &a.arg_sql)?;
            let expr = comb.strip_suffix(&format!(" AS r{n}")).ok_or_else(|| {
                Error::Unsupported("auto-distribute: unexpected aggregate combine fragment".into())
            })?;
            combine.push(format!("{expr} AS \"{out_name}\""));
            n += 1;
        }
    }

    // Narrow the scan to the union of the members' predicates — only when every member carries
    // one (a predicate-less member's aggregates must see every row) and the tail accepts a
    // trailing WHERE. Without it the FILTER'd aggregates are still exact over the full tail.
    let union_pred = if members.iter().all(|m| m.predicate_sql.is_some())
        && tail_allows_trailing_where(&members[0].tail)
    {
        let parts: Vec<String> = members
            .iter()
            .map(|m| {
                format!(
                    "({})",
                    m.predicate_sql.as_deref().expect("predicate checked above")
                )
            })
            .collect();
        format!(" WHERE {}", parts.join(" OR "))
    } else {
        String::new()
    };
    let leaf_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail}{union_pred} GROUP BY {distinct_arg}",
        psel.join(", ")
    ));
    let mid_sql =
        sanitize_generated_sql(&format!("SELECT {} FROM shuffle_input", mid_sel.join(", ")));
    // The empty-bucket synthetic row reads as NULLs / zero counts; HAVING COUNT(*) > 0 keeps
    // only partition 0's real row (same guard as the single-branch global path).
    let combine_sql = sanitize_generated_sql(&format!(
        "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
        combine.join(", ")
    ));
    Ok(DistributedQuery {
        stages: vec![
            // Equal argument values must co-locate: hash-shuffle by the leading argument column.
            StageDef::new(0, leaf_sql, vec![], vec![0]),
            StageDef::new(1, mid_sql, vec![0], vec![]),
            StageDef::new(2, combine_sql, vec![1], vec![]),
        ],
        finalize_sql: None,
    })
}

/// Whether a `WHERE …` clause can be spliced between the shared tail's `FROM …` and the merged
/// leaf's `GROUP BY`: only scan / projection / join shapes unparse as a bare `FROM …` clause
/// with no trailing clauses of their own (a `LIMIT` / `ORDER BY` / grouping in the tail would
/// invalidate the splice). A `SubqueryAlias` unparses as a self-contained derived table, so its
/// contents don't matter here.
fn tail_allows_trailing_where(lp: &LogicalPlan) -> bool {
    match lp {
        LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_) => true,
        LogicalPlan::Projection(p) => tail_allows_trailing_where(p.input.as_ref()),
        LogicalPlan::Join(j) => {
            tail_allows_trailing_where(j.left.as_ref())
                && tail_allows_trailing_where(j.right.as_ref())
        }
        _ => false,
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

/// Keyed-execution admission for the branch-DAG outer skeleton (TPC-DS Q4/Q39/Q78).
///
/// With the default gather, every branch output lands whole in partition 0 and the outer join
/// stage — which runs once per shuffle partition — computes the entire skeleton on one core
/// while the other partitions see empty inputs. When the skeleton is an *equijoin tree* over
/// the branch outputs whose join keys are all branch-output columns, the branch outputs can
/// hash-shuffle by those keys instead and the outer stage runs key-partitioned on every
/// worker: [`crate::shuffle::partition::hash_partition`] is deterministic (fnv1a over the
/// order-faithful row encoding), so every row tuple that could join lands in one bucket, and
/// the per-partition join of the co-located slices is exactly the global join restricted to
/// that bucket.
///
/// [`outer_keying`] derives the per-branch key columns (and the driver-side TopK merge) or
/// returns `None`; every `None` keeps the byte-identical gather plan.
struct OuterKeying {
    /// Hash-key column indices into each representative branch's output schema — one entry per
    /// deduplicated representative (every occurrence of a representative resolves to the same
    /// columns, or keying is declined). Same class order for every branch, so equal composite
    /// keys hash identically across stages.
    rep_keys: HashMap<usize, Vec<u32>>,
    /// `ORDER BY`/`LIMIT` over the driver-concatenated result when the outer plan carries
    /// either: each partition's copy of the outer stage applies the same TopK locally, and
    /// this finalize merges the per-partition winners (two-phase TopK).
    finalize: Option<String>,
}

/// One leaf of the outer join tree: a materialized branch placeholder (`shuffle_input[_i]`,
/// occurrence index `Some(i)`) or a replicated-only subplan (`None`) re-evaluated in full on
/// every partition — always co-located, never a shuffle-key constraint.
struct OuterLeaf<'a> {
    occurrence: Option<usize>,
    schema: &'a datafusion::common::DFSchema,
}

/// One join node of the outer skeleton plus the leaf spans of its two inputs (contiguous
/// in-order ranges into the walk's leaf vector).
struct OuterJoinEdge<'a> {
    join: &'a datafusion::logical_expr::Join,
    left_span: (usize, usize),
    right_span: (usize, usize),
}

/// One placeholder leaf's output column in the outer join tree: `(leaf index, column index)`.
type LeafColumn = (usize, usize);
/// One equi pair of leaf columns.
type EquiPair = (LeafColumn, LeafColumn);

/// Where a join-condition column resolves inside the outer join tree.
#[derive(Clone, Copy)]
enum OuterColumn {
    /// A column of a placeholder leaf, at that leaf's output index.
    Placeholder { leaf: usize, col: usize },
    /// A column of a replicated-only leaf: present in full on every partition, so it never
    /// constrains co-location.
    Replicated,
}

/// Derive the keyed execution plan for an admissible outer skeleton, or `None` to keep the
/// gather. The admission rule, each step conservative (any doubt → `None`):
///
/// 1. **Top chain**: only `Projection`/`Filter`/`SubqueryAlias` above the join-tree root, with
///    an optional plain `Sort` and/or literal `Limit` at the very top. `LIMIT` without
///    `ORDER BY` declines (per-partition top-k is not single-node-equivalent), as do
///    `OFFSET` and fused `Sort.fetch` (neither composes through the two-phase finalize), and
///    any volatile expression anywhere in the skeleton (single-node evaluates it once; keying
///    would re-evaluate per partition).
/// 2. **Join tree**: every join is `INNER`, `LEFT`, or `RIGHT`. `LEFT` requires a placeholder
///    on its preserved (left) side, `RIGHT` on its right side — a replicated preserved side
///    would re-emit its unmatched rows once per partition. (`FULL`/semi/anti decline.) Leaves
///    are placeholders or replicated-only subplans; anything else (mid-tree aggregate, union,
///    window, sort, …) declines.
/// 3. **Co-location**: placeholder-to-placeholder equi conditions union the referenced
///    (leaf, column) pairs into equivalence classes. Every class must hold *exactly one
///    column of every placeholder leaf* (so one composite key — one column per class, same
///    class order everywhere — co-locates every joined row), all columns of a class must
///    share one data type (the hash encodes typed key bytes), and every join between two
///    placeholder-bearing sides must equate the FULL composite key — a partially-covered edge
///    would let matched rows land on different partitions.
/// 4. **LEFT/RIGHT null-extension safety**: the preserved side is placeholder-keyed and the
///    non-preserved side either co-locates by the same key (placeholder) or is present in
///    full (replicated), so a preserved-side row finds all of its matches on its own key
///    partition and is null-extended there exactly when no global match exists — the same
///    argument `join_chain.rs` makes for key-partitioned outer joins. NULL keys never satisfy
///    the equi predicate on any partition, so their null-extension is partition-local too.
/// 5. **Stamplable branches**: no occurrence may belong to a shared-scan *merge* group (a
///    merged combine row carries several branches' columns, not one branch's schema), every
///    occurrence of one representative must resolve to the same key columns, and the
///    representative's output stage must exchange by `Hash` or `Forward`.
fn outer_keying(
    rewritten: &LogicalPlan,
    rep_of: &[usize],
    merged: &HashSet<usize>,
    rep_queries: &HashMap<usize, DistributedQuery>,
) -> Option<OuterKeying> {
    if plan_contains_volatile(rewritten) {
        return None;
    }
    let mut predicates: Vec<&Expr> = Vec::new();
    let (join_root, sort, limit) = strip_keyable_top(rewritten, &mut predicates)?;

    let mut leaves = Vec::new();
    let mut edges = Vec::new();
    let mut leaf_by_ptr = HashMap::new();
    collect_outer_joins(
        join_root,
        &mut leaves,
        &mut edges,
        &mut leaf_by_ptr,
        &mut predicates,
    )?;
    let placeholder_leaves: Vec<usize> = (0..leaves.len())
        .filter(|&i| leaves[i].occurrence.is_some())
        .collect();
    // Keying needs at least two co-located placeholders; a lone branch joined only against
    // replicated tables gains nothing and keeps the gather.
    if placeholder_leaves.len() < 2 {
        return None;
    }
    let span_has_placeholder =
        |span: (usize, usize)| (span.0..span.0 + span.1).any(|i| leaves[i].occurrence.is_some());

    // Join-type admission.
    for edge in &edges {
        let ph_left = span_has_placeholder(edge.left_span);
        let ph_right = span_has_placeholder(edge.right_span);
        match edge.join.join_type {
            JoinType::Inner => {}
            JoinType::Left if ph_left => {}
            JoinType::Right if ph_right => {}
            _ => return None,
        }
    }

    // Union every placeholder-to-placeholder equi pair into key classes. A pair comes from a
    // join's `on` (assigned to that edge) or from a plain `col = col` conjunct of any `Filter`
    // in the skeleton — including Q39's/Q4's comma-join `WHERE` floating above a
    // predicate-less cross join — assigned to the deepest edge whose two inputs separate the
    // pair's leaves. Everything else in those predicates (same-side conjuncts, cross-side
    // residuals like Q4's ratio CASE) is row-local and preserved verbatim in the outer stage
    // SQL; the per-edge coverage check below separately guarantees every keyed edge equates
    // the full composite key.
    let mut parent: HashMap<LeafColumn, LeafColumn> = HashMap::new();
    let mut edge_pairs: Vec<Vec<EquiPair>> = vec![Vec::new(); edges.len()];
    for (i, edge) in edges.iter().enumerate() {
        for (le, re) in &edge.join.on {
            record_equi_pair(
                le,
                re,
                Some(i),
                join_root,
                &edges,
                &leaves,
                &leaf_by_ptr,
                &mut parent,
                &mut edge_pairs,
            )?;
        }
    }
    let mut conjuncts: Vec<Expr> = Vec::new();
    for pred in &predicates {
        flatten_and_conjuncts(pred, &mut conjuncts);
    }
    for edge in &edges {
        if let Some(f) = &edge.join.filter {
            flatten_and_conjuncts(f, &mut conjuncts);
        }
    }
    for c in &conjuncts {
        let Expr::BinaryExpr(be) = c else {
            continue;
        };
        if be.op != datafusion::logical_expr::Operator::Eq {
            continue;
        }
        record_equi_pair(
            be.left.as_ref(),
            be.right.as_ref(),
            None,
            join_root,
            &edges,
            &leaves,
            &leaf_by_ptr,
            &mut parent,
            &mut edge_pairs,
        )?;
    }

    let members: Vec<LeafColumn> = parent.keys().copied().collect();
    let mut classes: Vec<Vec<LeafColumn>> = Vec::new();
    {
        let mut by_root: HashMap<LeafColumn, usize> = HashMap::new();
        for member in members {
            let root = uf_find(&mut parent, member);
            match by_root.get(&root) {
                Some(&i) => classes[i].push(member),
                None => {
                    by_root.insert(root, classes.len());
                    classes.push(vec![member]);
                }
            }
        }
    }
    // No placeholder-placeholder equi key anywhere (cross/scalar skeleton): keep the gather.
    if classes.is_empty() {
        return None;
    }
    for class in &classes {
        // Exactly one column of every placeholder leaf per class.
        if class.len() != placeholder_leaves.len() {
            return None;
        }
        let mut seen = HashSet::new();
        if !class.iter().all(|(leaf, _)| seen.insert(*leaf)) {
            return None;
        }
        // One shared type per class: the shuffle hash encodes typed key bytes, so equal
        // logical keys with different physical types could land in different buckets.
        let (l0, c0) = class[0];
        let dt = leaves[l0].schema.field(c0).data_type();
        if class
            .iter()
            .any(|&(l, c)| leaves[l].schema.field(c).data_type() != dt)
        {
            return None;
        }
    }
    // Deterministic class order — every leaf's key column list must follow the same order or
    // the composite hashes would not line up.
    classes.sort_by_key(|class| class.iter().copied().min().unwrap_or((usize::MAX, 0)));

    // Every join between two placeholder-bearing sides must equate the full composite key.
    for (edge, pairs) in edges.iter().zip(&edge_pairs) {
        if !(span_has_placeholder(edge.left_span) && span_has_placeholder(edge.right_span)) {
            continue;
        }
        let mut covered = HashSet::new();
        for (a, b) in pairs {
            covered.insert(uf_find(&mut parent, *a));
            covered.insert(uf_find(&mut parent, *b));
        }
        if covered.len() != classes.len() {
            return None;
        }
    }

    // One ordered key-column list per leaf, then per representative branch.
    let mut leaf_keys: Vec<Vec<u32>> = vec![Vec::new(); leaves.len()];
    for class in &classes {
        for &(leaf, col) in class {
            leaf_keys[leaf].push(col as u32);
        }
    }
    let mut rep_keys: HashMap<usize, Vec<u32>> = HashMap::new();
    for &i in &placeholder_leaves {
        let occurrence = leaves[i].occurrence.expect("placeholder leaf");
        let rep = *rep_of.get(occurrence)?;
        // A merged shared-scan combine carries every member branch's columns on one row, so
        // a single branch's schema indices do not address it.
        if merged.contains(&rep) {
            return None;
        }
        let dq = rep_queries.get(&rep)?;
        let terminal = dq.stages.last()?;
        if !matches!(
            terminal.exchange,
            ExchangeMode::Hash | ExchangeMode::Forward
        ) {
            return None;
        }
        let keys = &leaf_keys[i];
        match rep_keys.get(&rep) {
            Some(prev) if prev != keys => return None,
            Some(_) => {}
            None => {
                rep_keys.insert(rep, keys.clone());
            }
        }
    }

    let finalize = build_outer_finalize(sort, limit).ok()?;
    Some(OuterKeying { rep_keys, finalize })
}

/// The peeled top of an admissible outer skeleton: the join-tree root plus the `ORDER BY` /
/// `LIMIT` the driver-side finalize must reproduce.
type KeyableTop<'a> = (
    &'a LogicalPlan,
    Option<&'a [datafusion::logical_expr::SortExpr]>,
    Option<usize>,
);

/// Validate the unary chain above the outer join-tree root and peel any top `Sort`/`Limit`
/// for the driver-side finalize, recording every `Filter` predicate it passes (those may
/// carry the comma-join equi conditions — see [`outer_keying`]). `None` for anything that is
/// not a row-local pass-through (see [`outer_keying`] step 1).
fn strip_keyable_top<'a>(
    lp: &'a LogicalPlan,
    predicates: &mut Vec<&'a Expr>,
) -> Option<KeyableTop<'a>> {
    let mut sort: Option<&[datafusion::logical_expr::SortExpr]> = None;
    let mut limit: Option<usize> = None;
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Sort(s) => {
                // A fused TopK (`ORDER BY … FETCH`) cannot merge per-partition results
                // through the plain finalize; only unbounded sorts admit keying.
                if s.fetch.is_some() || sort.is_some() {
                    return None;
                }
                sort = Some(s.expr.as_slice());
                node = &s.input;
            }
            LogicalPlan::Limit(l) => {
                if limit.is_some() {
                    return None;
                }
                // OFFSET does not compose per-partition (each partition would have to keep
                // skip+fetch rows); keep the gather.
                match l.skip.as_deref() {
                    None => {}
                    Some(Expr::Literal(scalar, _)) if literal_usize(scalar) == Some(0) => {}
                    _ => return None,
                }
                match l.fetch.as_deref() {
                    None => {}
                    Some(Expr::Literal(scalar, _)) => limit = Some(literal_usize(scalar)?),
                    _ => return None,
                }
                node = &l.input;
            }
            LogicalPlan::Projection(p) => node = &p.input,
            LogicalPlan::Filter(f) => {
                predicates.push(&f.predicate);
                node = f.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            LogicalPlan::Join(_) => break,
            _ => return None,
        }
    }
    // LIMIT without ORDER BY picks an arbitrary subset; a per-partition top-k would not
    // reproduce single-node's choice.
    if limit.is_some() && sort.is_none() {
        return None;
    }
    Some((node, sort, limit))
}

/// Walk the outer join tree, collecting leaves (placeholders / replicated-only subplans),
/// join edges, and every `Filter` predicate along the way. `None` for any shape
/// [`outer_keying`] step 2 declines. Returns the subtree's leaf span as `(start, count)`
/// into `leaves` (contiguous, in-order).
fn collect_outer_joins<'a>(
    node: &'a LogicalPlan,
    leaves: &mut Vec<OuterLeaf<'a>>,
    edges: &mut Vec<OuterJoinEdge<'a>>,
    leaf_by_ptr: &mut HashMap<usize, usize>,
    predicates: &mut Vec<&'a Expr>,
) -> Option<(usize, usize)> {
    let mut n = node;
    while let LogicalPlan::SubqueryAlias(s) = n {
        n = s.input.as_ref();
    }
    if !plan_contains_placeholder(n) {
        let start = leaves.len();
        leaf_by_ptr.insert(node_id(n), start);
        leaves.push(OuterLeaf {
            occurrence: None,
            schema: n.schema(),
        });
        return Some((start, 1));
    }
    match n {
        LogicalPlan::TableScan(scan) => {
            let occurrence = placeholder_occurrence(scan.table_name.table())?;
            let start = leaves.len();
            leaf_by_ptr.insert(node_id(n), start);
            leaves.push(OuterLeaf {
                occurrence: Some(occurrence),
                schema: n.schema(),
            });
            Some((start, 1))
        }
        LogicalPlan::Join(join) => {
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right => {}
                _ => return None,
            }
            let left_span =
                collect_outer_joins(&join.left, leaves, edges, leaf_by_ptr, predicates)?;
            let right_span =
                collect_outer_joins(&join.right, leaves, edges, leaf_by_ptr, predicates)?;
            edges.push(OuterJoinEdge {
                join,
                left_span,
                right_span,
            });
            Some((left_span.0, left_span.1 + right_span.1))
        }
        // Row-local nodes between joins (a per-alias filter or renaming projection the
        // optimizer left around a placeholder) keep the same leaf span; the shuffle still
        // co-locates the underlying branch rows. A filter's conjuncts may carry the equi
        // conditions (comma-join `WHERE`), so record them for [`record_equi_pair`].
        LogicalPlan::Filter(f) => {
            predicates.push(&f.predicate);
            collect_outer_joins(&f.input, leaves, edges, leaf_by_ptr, predicates)
        }
        LogicalPlan::Projection(p) => {
            collect_outer_joins(&p.input, leaves, edges, leaf_by_ptr, predicates)
        }
        _ => None,
    }
}

/// Whether any `shuffle_input` placeholder scan sits below `lp`.
fn plan_contains_placeholder(lp: &LogicalPlan) -> bool {
    if let LogicalPlan::TableScan(scan) = lp {
        if placeholder_occurrence(scan.table_name.table()).is_some() {
            return true;
        }
    }
    lp.inputs().iter().any(|i| plan_contains_placeholder(i))
}

/// The branch occurrence a placeholder table name stands for (`shuffle_input` → 0,
/// `shuffle_input_{i}` → `i`), mirroring [`placeholder_plan`]'s naming.
fn placeholder_occurrence(table: &str) -> Option<usize> {
    if table == "shuffle_input" {
        return Some(0);
    }
    table
        .strip_prefix("shuffle_input_")
        .and_then(|suffix| suffix.parse::<usize>().ok())
}

/// Resolve a join-condition column through the pass-through nodes of a join input subtree to
/// the leaf column it reads. `None` when the reference does not bottom out at a collected
/// leaf (an expression projection, an ambiguous qualifier) — the caller declines keying.
fn resolve_outer_column(
    subtree: &LogicalPlan,
    col: &datafusion::common::Column,
    leaves: &[OuterLeaf],
    leaf_by_ptr: &HashMap<usize, usize>,
) -> Option<OuterColumn> {
    let mut idx = subtree.schema().index_of_column(col).ok()?;
    let mut node = subtree;
    loop {
        let mut n = node;
        while let LogicalPlan::SubqueryAlias(s) = n {
            n = s.input.as_ref();
        }
        match n {
            LogicalPlan::Join(join) => {
                let left_width = join.left.schema().fields().len();
                if idx < left_width {
                    node = &join.left;
                } else {
                    node = &join.right;
                    idx -= left_width;
                }
            }
            LogicalPlan::Filter(f) => node = f.input.as_ref(),
            LogicalPlan::Projection(p) => {
                let Expr::Column(c) = strip_alias(&p.expr[idx]) else {
                    return None;
                };
                idx = p.input.schema().index_of_column(c).ok()?;
                node = p.input.as_ref();
            }
            _ => {
                let &leaf = leaf_by_ptr.get(&node_id(n))?;
                return Some(match leaves[leaf].occurrence {
                    Some(_) => OuterColumn::Placeholder { leaf, col: idx },
                    None => OuterColumn::Replicated,
                });
            }
        }
    }
}

/// Process one candidate equi pair `(le, re)`: when both sides resolve to *different*
/// placeholder leaves, union their (leaf, column) classes and assign the pair to `forced_edge`
/// (a join's own `on` pair) or to the deepest edge straddling the two leaves (a floating
/// filter conjunct). Anything else — a non-column expression, a same-leaf equality, a
/// replicated side — is a row-local residual that needs no co-location. A column that fails
/// to resolve at all declines (`None`).
#[allow(clippy::too_many_arguments)]
fn record_equi_pair(
    le: &Expr,
    re: &Expr,
    forced_edge: Option<usize>,
    join_root: &LogicalPlan,
    edges: &[OuterJoinEdge],
    leaves: &[OuterLeaf],
    leaf_by_ptr: &HashMap<usize, usize>,
    parent: &mut HashMap<LeafColumn, LeafColumn>,
    edge_pairs: &mut [Vec<EquiPair>],
) -> Option<()> {
    let (Expr::Column(lc), Expr::Column(rc)) = (le, re) else {
        return Some(());
    };
    let lt = resolve_outer_column(join_root, lc, leaves, leaf_by_ptr)?;
    let rt = resolve_outer_column(join_root, rc, leaves, leaf_by_ptr)?;
    let (
        OuterColumn::Placeholder { leaf: lf, col: cf },
        OuterColumn::Placeholder { leaf: rg, col: cg },
    ) = (lt, rt)
    else {
        return Some(());
    };
    if lf == rg {
        return Some(());
    }
    uf_union(parent, (lf, cf), (rg, cg));
    let edge_i = match forced_edge {
        Some(i) => i,
        None => deepest_straddling_edge(edges, lf, rg)?,
    };
    edge_pairs[edge_i].push(((lf, cf), (rg, cg)));
    Some(())
}

/// The deepest (smallest-span) join edge whose two inputs separate leaves `a` and `b` — the
/// edge at which their rows first meet. `None` when no edge straddles the pair (the caller
/// declines keying).
fn deepest_straddling_edge(edges: &[OuterJoinEdge], a: usize, b: usize) -> Option<usize> {
    let in_span = |span: (usize, usize), leaf: usize| leaf >= span.0 && leaf < span.0 + span.1;
    let mut best: Option<usize> = None;
    for (i, e) in edges.iter().enumerate() {
        let straddles = (in_span(e.left_span, a) && in_span(e.right_span, b))
            || (in_span(e.right_span, a) && in_span(e.left_span, b));
        if !straddles {
            continue;
        }
        let len = e.left_span.1 + e.right_span.1;
        match best {
            Some(j) if edges[j].left_span.1 + edges[j].right_span.1 <= len => {}
            _ => best = Some(i),
        }
    }
    best
}

fn uf_find(parent: &mut HashMap<LeafColumn, LeafColumn>, x: LeafColumn) -> LeafColumn {
    let mut root = x;
    loop {
        let &p = parent.get(&root).unwrap_or(&root);
        if p == root {
            break;
        }
        root = p;
    }
    let mut cur = x;
    while let Some(&p) = parent.get(&cur) {
        if p == cur || p == root {
            break;
        }
        parent.insert(cur, root);
        cur = p;
    }
    root
}

fn uf_union(parent: &mut HashMap<LeafColumn, LeafColumn>, a: LeafColumn, b: LeafColumn) {
    parent.entry(a).or_insert(a);
    parent.entry(b).or_insert(b);
    let ra = uf_find(parent, a);
    let rb = uf_find(parent, b);
    if ra != rb {
        parent.insert(ra, rb);
    }
}

/// Literal `LIMIT`/`OFFSET` value, or `None` for any non-literal or negative expression.
fn literal_usize(s: &datafusion::scalar::ScalarValue) -> Option<usize> {
    use datafusion::scalar::ScalarValue::*;
    match s {
        Int64(Some(v)) if *v >= 0 => Some(*v as usize),
        Int32(Some(v)) if *v >= 0 => Some(*v as usize),
        UInt64(Some(v)) => Some(*v as usize),
        UInt32(Some(v)) => Some(*v as usize),
        _ => None,
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

/// Splice a branch's sub-DAG into the outer stage list (ids re-based past `next_id`,
/// a `finalize_sql` appended as one more `shuffle_input` stage), returning the branch
/// output's stage id. Used by the branch-aware CrossJoin splitter and by
/// [`super::join_chain`]'s opaque derived legs (KAN-162 q54/q64).
pub(crate) fn append_branch(
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
            // Preserve a deliberate per-stage replicate stamp (a sliced replicated producer's
            // reduced stamp drops its anchor tables so the workers' file sharder slices those
            // scans for this stage only); an empty stamp is filled by the outer
            // `stamp_replicated_tables` as before.
            replicated_tables: stage.replicated_tables,
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
    use oxidant_loom::arrow::array::{Int64Array, RecordBatch, StringArray};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::Engine;

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

    // ---- Miniature TPC-DS schemas for the keyed-outer (Q4/Q39/Q78) shapes ----

    const Q4: &str = include_str!("../../../../bench/tpcds/queries/q4.sql");
    const Q39: &str = include_str!("../../../../bench/tpcds/queries/q39.sql");
    const Q78: &str = include_str!("../../../../bench/tpcds/queries/q78.sql");

    fn i64f(name: &str) -> Field {
        Field::new(name, DataType::Int64, false)
    }

    fn strf(name: &str) -> Field {
        Field::new(name, DataType::Utf8, false)
    }

    /// An all-Int64 single-batch table from row-major values (plan-shape tests never read the
    /// rows, but the tables must exist for `logical_plan`).
    fn i64_table(cols: &[&str], rows: &[&[i64]]) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            cols.iter().map(|c| i64f(c)).collect::<Vec<_>>(),
        ));
        let arrays: Vec<oxidant_loom::arrow::array::ArrayRef> = (0..cols.len())
            .map(|c| {
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r[c]).collect::<Vec<i64>>(),
                )) as oxidant_loom::arrow::array::ArrayRef
            })
            .collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    fn customer() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                i64f("c_customer_sk"),
                strf("c_customer_id"),
                strf("c_first_name"),
                strf("c_last_name"),
                strf("c_preferred_cust_flag"),
                strf("c_birth_country"),
                strf("c_login"),
                strf("c_email_address"),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["cust1", "cust2"])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(StringArray::from(vec!["Y", "N"])),
                Arc::new(StringArray::from(vec!["US", "CA"])),
                Arc::new(StringArray::from(vec!["l1", "l2"])),
                Arc::new(StringArray::from(vec!["e1", "e2"])),
            ],
        )
        .unwrap()
    }

    fn warehouse() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                i64f("w_warehouse_sk"),
                strf("w_warehouse_name"),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["w1", "w2"])),
            ],
        )
        .unwrap()
    }

    /// One engine holding every table the three queries touch (a few rows each).
    async fn tpcds_engine() -> Engine {
        let engine = Engine::new();
        engine
            .register_batches("customer", vec![customer()])
            .unwrap();
        engine
            .register_batches("warehouse", vec![warehouse()])
            .unwrap();
        engine
            .register_batches(
                "store_sales",
                vec![i64_table(
                    &[
                        "ss_customer_sk",
                        "ss_sold_date_sk",
                        "ss_item_sk",
                        "ss_ticket_number",
                        "ss_quantity",
                        "ss_wholesale_cost",
                        "ss_sales_price",
                        "ss_ext_list_price",
                        "ss_ext_wholesale_cost",
                        "ss_ext_discount_amt",
                        "ss_ext_sales_price",
                    ],
                    &[&[1, 10, 1, 100, 2, 5, 10, 12, 6, 1, 11]],
                )],
            )
            .unwrap();
        engine
            .register_batches(
                "catalog_sales",
                vec![i64_table(
                    &[
                        "cs_bill_customer_sk",
                        "cs_sold_date_sk",
                        "cs_item_sk",
                        "cs_order_number",
                        "cs_quantity",
                        "cs_wholesale_cost",
                        "cs_sales_price",
                        "cs_ext_list_price",
                        "cs_ext_wholesale_cost",
                        "cs_ext_discount_amt",
                        "cs_ext_sales_price",
                    ],
                    &[&[1, 10, 1, 200, 3, 4, 9, 12, 5, 1, 10]],
                )],
            )
            .unwrap();
        engine
            .register_batches(
                "web_sales",
                vec![i64_table(
                    &[
                        "ws_bill_customer_sk",
                        "ws_sold_date_sk",
                        "ws_item_sk",
                        "ws_order_number",
                        "ws_quantity",
                        "ws_wholesale_cost",
                        "ws_sales_price",
                        "ws_ext_list_price",
                        "ws_ext_wholesale_cost",
                        "ws_ext_discount_amt",
                        "ws_ext_sales_price",
                    ],
                    &[&[2, 11, 2, 300, 4, 3, 8, 12, 4, 1, 9]],
                )],
            )
            .unwrap();
        engine
            .register_batches(
                "store_returns",
                vec![i64_table(&["sr_ticket_number", "sr_item_sk"], &[&[100, 1]])],
            )
            .unwrap();
        engine
            .register_batches(
                "catalog_returns",
                vec![i64_table(&["cr_order_number", "cr_item_sk"], &[&[200, 1]])],
            )
            .unwrap();
        engine
            .register_batches(
                "web_returns",
                vec![i64_table(&["wr_order_number", "wr_item_sk"], &[&[300, 2]])],
            )
            .unwrap();
        engine
            .register_batches(
                "date_dim",
                vec![i64_table(
                    &["d_date_sk", "d_year", "d_moy"],
                    &[&[10, 2001, 1]],
                )],
            )
            .unwrap();
        engine
            .register_batches(
                "inventory",
                vec![i64_table(
                    &[
                        "inv_item_sk",
                        "inv_warehouse_sk",
                        "inv_date_sk",
                        "inv_quantity_on_hand",
                    ],
                    &[&[1, 1, 10, 50]],
                )],
            )
            .unwrap();
        engine
            .register_batches("item", vec![i64_table(&["i_item_sk"], &[&[1]])])
            .unwrap();
        engine
    }

    async fn tpcds_plan(sql: &str) -> LogicalPlan {
        tpcds_engine().await.logical_plan(sql).await.unwrap()
    }

    fn stage_by_id(dq: &DistributedQuery, id: u32) -> &StageDef {
        dq.stages
            .iter()
            .find(|s| s.stage_id == id)
            .expect("stage id present")
    }

    /// TPC-DS Q4: the deduplicated `year_total` union branch feeds six self-join occurrences
    /// equijoined on `customer_id` (the branch's first output column). The branch output
    /// hash-shuffles by that column instead of gathering to partition 0, the outer stage keeps
    /// the ratio-CASE residuals, and the driver finalize merges the per-partition TopK.
    #[tokio::test]
    async fn q4_outer_self_join_keys_the_deduped_branch_output() {
        let lp = tpcds_plan(Q4).await;
        let dq = plan_distributed_logical(&lp, &["customer", "date_dim"]).expect("Q4 plans");
        let outer = dq.stages.last().unwrap();
        assert_eq!(
            outer.upstream_stage_ids.len(),
            6,
            "six year_total occurrences: {dq:?}"
        );
        assert!(
            outer
                .upstream_stage_ids
                .iter()
                .all(|&id| id == outer.upstream_stage_ids[0]),
            "identical occurrences dedup into one sub-DAG: {dq:?}"
        );
        let branch_out = stage_by_id(&dq, outer.upstream_stage_ids[0]);
        assert_eq!(
            branch_out.hash_key_cols,
            vec![0],
            "hash-shuffle the branch output by customer_id: {branch_out:?}"
        );
        assert!(
            outer.sql.to_uppercase().contains("CASE"),
            "ratio join-condition residuals stay in the outer stage: {}",
            outer.sql
        );
        let finalize = dq.finalize_sql.expect("two-phase TopK finalize");
        assert!(
            finalize.contains("ORDER BY") && finalize.contains("LIMIT 100"),
            "{finalize}"
        );
    }

    /// KAN-2 throughput residual: Q4's real driver path pre-optimizes the plan before the
    /// split (`Engine::optimize_logical_plan` — union-extended rules). The six `year_total`
    /// occurrences prune to single-fact slices (the contradictory `sale_type` arms fold
    /// away, the `dyear` predicates reach the `date_dim` scans) and the rewritten plan must
    /// still split: six distinct slice sub-DAGs feed the keyed outer stage — not the v12
    /// 66-stage explosion that failed workers with do_get transport errors.
    #[tokio::test]
    async fn q4_optimized_plan_splits_into_per_slice_subdags() {
        let engine = tpcds_engine().await;
        let lp = engine.logical_plan(Q4).await.unwrap();
        let opt = engine.optimize_logical_plan(lp).unwrap();
        let display = format!("{}", opt.display_indent());
        assert!(
            !display.contains("Union"),
            "every year_total occurrence prunes to its sale_type slice: {display}"
        );
        let dq = plan_distributed_logical(&opt, &["customer", "date_dim"])
            .expect("optimized Q4 still plans distributed");
        assert!(
            dq.stages.len() <= 30,
            "slice sub-DAGs, not the 66-stage explosion: {} stages\n{dq:?}",
            dq.stages.len()
        );
        let outer = dq.stages.last().unwrap();
        assert_eq!(
            outer.upstream_stage_ids.len(),
            6,
            "six pruned year_total slices feed the outer stage: {dq:?}"
        );
        assert!(
            !outer
                .upstream_stage_ids
                .iter()
                .all(|&id| id == outer.upstream_stage_ids[0]),
            "distinct slices no longer dedup to one sub-DAG: {dq:?}"
        );
        // Each of the three fact tables is scanned by exactly two slices (firstyear +
        // secyear) — the unoptimized plan scanned each fact once for all six occurrences.
        let scans = |fact: &str| {
            dq.stages
                .iter()
                .filter(|s| s.sql.contains(&format!("JOIN {fact} ON")))
                .count()
        };
        assert_eq!(
            (
                scans("store_sales"),
                scans("catalog_sales"),
                scans("web_sales")
            ),
            (2, 2, 2),
            "one scan per (fact, year) slice:\n{dq:?}"
        );
        assert!(
            outer.sql.to_uppercase().contains("CASE"),
            "ratio-CASE residuals stay in the outer stage: {}",
            outer.sql
        );
        let finalize = dq.finalize_sql.expect("two-phase TopK finalize");
        assert!(
            finalize.contains("ORDER BY") && finalize.contains("LIMIT 100"),
            "{finalize}"
        );
    }

    /// TPC-DS Q39: the `inv` aggregate branch self-joins on the two-column key
    /// `(i_item_sk, w_warehouse_sk)` with `d_moy` residuals and an `ORDER BY` (no LIMIT). The
    /// branch output keys on both group columns — class order follows the branch schema
    /// (`w_warehouse_sk` at index 1, `i_item_sk` at 2).
    #[tokio::test]
    async fn q39_self_join_keys_on_item_and_warehouse() {
        let lp = tpcds_plan(Q39).await;
        let dq =
            plan_distributed_logical(&lp, &["item", "warehouse", "date_dim"]).expect("Q39 plans");
        let outer = dq.stages.last().unwrap();
        assert_eq!(outer.upstream_stage_ids.len(), 2, "inv self-join: {dq:?}");
        for &id in &outer.upstream_stage_ids {
            assert_eq!(
                stage_by_id(&dq, id).hash_key_cols,
                vec![1, 2],
                "composite key (w_warehouse_sk, i_item_sk): {dq:?}"
            );
        }
        assert!(
            outer.sql.contains("d_moy"),
            "d_moy residuals preserved: {}",
            outer.sql
        );
        let finalize = dq.finalize_sql.expect("ORDER BY finalize");
        assert!(
            finalize.contains("ORDER BY") && !finalize.contains("LIMIT"),
            "{finalize}"
        );
    }

    /// TPC-DS Q78: `ss LEFT JOIN ws LEFT JOIN cs` on (sold_year, item, customer). The sharded
    /// `ss` branch's combine output keys on [0,1,2]; the replicated-only `ws`/`cs` aggregate
    /// arms materialize as `Forward` stages that hash-partition by the same key instead of
    /// gathering to partition 0 — LEFT null-extension is key-local.
    #[tokio::test]
    async fn q78_left_chain_keys_branch_and_forward_arms() {
        let lp = tpcds_plan(Q78).await;
        let replicated = [
            "date_dim",
            "store_returns",
            "web_sales",
            "web_returns",
            "catalog_sales",
            "catalog_returns",
        ];
        let dq = plan_distributed_logical(&lp, &replicated).expect("Q78 plans");
        let outer = dq.stages.last().unwrap();
        assert_eq!(outer.upstream_stage_ids.len(), 3, "ss/ws/cs: {dq:?}");
        let mut forwards = 0;
        for &id in &outer.upstream_stage_ids {
            let s = stage_by_id(&dq, id);
            assert_eq!(
                s.hash_key_cols,
                vec![0, 1, 2],
                "(sold_year, item, customer) key: {s:?}"
            );
            if s.exchange == ExchangeMode::Forward {
                forwards += 1;
            }
        }
        assert_eq!(forwards, 2, "ws/cs are keyed Forward arms: {dq:?}");
        assert!(
            outer.sql.to_uppercase().contains("LEFT OUTER JOIN"),
            "outer chain preserved: {}",
            outer.sql
        );
        let finalize = dq.finalize_sql.expect("two-phase TopK finalize");
        assert!(
            finalize.contains("ORDER BY") && finalize.contains("LIMIT 100"),
            "{finalize}"
        );
    }

    /// The plain equijoin skeleton (no TopK) keys both branch outputs; with neither `ORDER BY`
    /// nor `LIMIT` the driver concatenation is the full result, so no finalize is added.
    #[tokio::test]
    async fn inner_equijoin_branches_key_without_finalize() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k), \
                 b AS (SELECT k, COUNT(*) AS n FROM t GROUP BY k) \
             SELECT a.k, a.s, b.n FROM a JOIN b ON a.k = b.k",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("join of two branches plans");
        let outer = dq.stages.last().unwrap();
        assert_eq!(outer.upstream_stage_ids.len(), 2, "{dq:?}");
        for &id in &outer.upstream_stage_ids {
            assert_eq!(
                stage_by_id(&dq, id).hash_key_cols,
                vec![0],
                "both branches keyed by k: {dq:?}"
            );
        }
        assert!(
            dq.finalize_sql.is_none(),
            "no ORDER BY/LIMIT → no finalize: {dq:?}"
        );
    }

    /// A RIGHT join keys exactly like LEFT when the placeholder sits on the preserved side
    /// (Q78 with the sides flipped).
    #[tokio::test]
    async fn right_join_keys_when_placeholder_is_preserved() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k), \
                 b AS (SELECT k, COUNT(*) AS n FROM t GROUP BY k) \
             SELECT a.k, a.s, b.n FROM a RIGHT JOIN b ON a.k = b.k",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("right join plans");
        let outer = dq.stages.last().unwrap();
        assert_eq!(outer.upstream_stage_ids.len(), 2, "{dq:?}");
        for &id in &outer.upstream_stage_ids {
            assert_eq!(stage_by_id(&dq, id).hash_key_cols, vec![0], "{dq:?}");
        }
    }

    /// Assert the gather invariant on the stages that matter: every branch *output* stage
    /// (the outer stage's upstreams) has an empty hash key, so all rows gather to partition 0.
    fn assert_branch_outputs_gather(dq: &DistributedQuery) {
        let outer = dq.stages.last().unwrap();
        for &id in &outer.upstream_stage_ids {
            assert!(
                stage_by_id(dq, id).hash_key_cols.is_empty(),
                "branch output stage {id} must gather: {dq:?}"
            );
        }
    }

    /// `LIMIT` without `ORDER BY` picks an arbitrary subset; a per-partition top-k would not
    /// reproduce single-node's choice, so the equijoin keeps the byte-identical gather.
    #[tokio::test]
    async fn limit_without_order_by_keeps_the_gather() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k), \
                 b AS (SELECT k, COUNT(*) AS n FROM t GROUP BY k) \
             SELECT a.k, a.s, b.n FROM a JOIN b ON a.k = b.k LIMIT 5",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("join of two branches plans");
        assert_branch_outputs_gather(&dq);
        assert!(dq.finalize_sql.is_none(), "{dq:?}");
    }

    /// A FULL join null-extends both sides; admission declines it conservatively and the
    /// skeleton keeps the gather.
    #[tokio::test]
    async fn full_join_of_branches_keeps_the_gather() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k), \
                 b AS (SELECT k, COUNT(*) AS n FROM t GROUP BY k) \
             SELECT a.k, a.s, b.n FROM a FULL JOIN b ON a.k = b.k",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("full join of two branches plans");
        assert_branch_outputs_gather(&dq);
        assert!(dq.finalize_sql.is_none(), "{dq:?}");
    }

    /// Two independent key families (`a⋈b` on `k`, `a⋈c` on `v2`) cannot co-locate both joins
    /// with one shuffle key: the one-column-per-leaf-per-class check declines the keying.
    #[tokio::test]
    async fn independent_key_families_keep_the_gather() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS v2 FROM t GROUP BY k), \
                 b AS (SELECT k, COUNT(*) AS n FROM t GROUP BY k), \
                 c AS (SELECT k, MAX(v) AS m FROM t GROUP BY k) \
             SELECT a.k, b.n, c.m FROM a JOIN b ON a.k = b.k JOIN c ON a.v2 = c.m",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("two key families still plan");
        assert_branch_outputs_gather(&dq);
        assert!(dq.finalize_sql.is_none(), "{dq:?}");
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
        // ...and the plan keeps one volatile combine per occurrence (the volatile-free partial
        // scan may be shared by `cse_identical_stages` — the invariant is that `random()` is
        // still evaluated once per occurrence, in two distinct combine stages).
        let dq = plan_distributed_logical(&lp, &[]).expect("volatile self-join still plans");
        let outer = dq.stages.last().unwrap();
        let mut upstreams = outer.upstream_stage_ids.clone();
        upstreams.dedup();
        assert_eq!(
            upstreams.len(),
            2,
            "volatile branches are not deduplicated: {dq:?}"
        );
        for &id in &outer.upstream_stage_ids {
            let combine = dq
                .stages
                .iter()
                .find(|s| s.stage_id == id)
                .expect("combine stage");
            assert!(
                combine.sql.contains("random()"),
                "each occurrence re-evaluates the volatile expression: {dq:?}"
            );
        }
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

    /// TPC-DS Q28 in miniature: two branches carrying `COUNT(DISTINCT v)` over the same sharded
    /// scan — differing only in their row predicates — merge into ONE scan whose leaf GROUPs BY
    /// the shared DISTINCT argument, gates every per-branch partial and predicate marker with
    /// its own branch's `FILTER (WHERE …)`, and narrows the scan to the OR of the predicates.
    #[tokio::test]
    async fn distinct_branches_sharing_one_arg_merge_into_one_scan() {
        let lp = logical_plan(
            "SELECT * FROM \
               (SELECT avg(v) AS a1_avg, count(v) AS a1_cnt, count(DISTINCT v) AS a1_cntd \
                FROM t WHERE k BETWEEN 0 AND 5) a1, \
               (SELECT avg(v) AS a2_avg, count(v) AS a2_cnt, count(DISTINCT v) AS a2_cntd \
                FROM t WHERE k BETWEEN 6 AND 10) a2",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("compatible distinct branches plan");

        assert_eq!(
            dq.stages.len(),
            4,
            "leaf + per-partition distinct + gather-combine + outer: {dq:?}"
        );
        let leaves: Vec<_> = dq
            .stages
            .iter()
            .filter(|s| s.upstream_stage_ids.is_empty())
            .collect();
        assert_eq!(leaves.len(), 1, "one shared scan of t: {dq:?}");
        let leaf = &leaves[0];
        assert!(
            leaf.sql.contains("GROUP BY t.v"),
            "partial dedup groups by the shared DISTINCT argument: {}",
            leaf.sql
        );
        assert_eq!(
            leaf.hash_key_cols,
            vec![0],
            "shuffle by the DISTINCT argument column: {leaf:?}"
        );
        // FILTER predicates land on the right aggregates: each branch's marker, avg partials,
        // and count partial carry that branch's own predicate.
        assert_eq!(
            leaf.sql.matches("FILTER (WHERE").count(),
            8,
            "four gated partials per branch: {}",
            leaf.sql
        );
        assert!(
            leaf.sql
                .contains("count(*) FILTER (WHERE ((t.k BETWEEN 0 AND 5))) AS f0"),
            "branch 1's predicate marker: {}",
            leaf.sql
        );
        assert!(
            leaf.sql
                .contains("count(*) FILTER (WHERE ((t.k BETWEEN 6 AND 10))) AS f1"),
            "branch 2's predicate marker: {}",
            leaf.sql
        );
        assert!(
            leaf.sql
                .contains("sum(t.v) FILTER (WHERE ((t.k BETWEEN 0 AND 5))) AS a0s"),
            "branch 1's avg partial: {}",
            leaf.sql
        );
        assert!(
            leaf.sql
                .contains("sum(t.v) FILTER (WHERE ((t.k BETWEEN 6 AND 10))) AS a2s"),
            "branch 2's avg partial: {}",
            leaf.sql
        );
        assert!(
            leaf.sql
                .contains("WHERE (((t.k BETWEEN 0 AND 5))) OR (((t.k BETWEEN 6 AND 10)))"),
            "the scan is narrowed to the union of the branch predicates: {}",
            leaf.sql
        );
        // Per-partition exact distinct counts keyed off the markers, then the gather-combine
        // re-aliased to each branch's original output names.
        let mid = &dq.stages[1].sql;
        assert!(
            mid.contains("count(DISTINCT CASE WHEN f0 > 0 THEN c END) AS d0")
                && mid.contains("count(DISTINCT CASE WHEN f1 > 0 THEN c END) AS d1"),
            "{mid}"
        );
        let combine = &dq.stages[2].sql;
        assert!(
            combine.contains("sum(d0) AS \"a1_cntd\"")
                && combine.contains("sum(d1) AS \"a2_cntd\""),
            "{combine}"
        );
        assert!(
            combine.contains("(sum(a0s) / NULLIF(sum(a0c), 0)) AS \"a1_avg\""),
            "avg recombines from the FILTER'd per-branch partials: {combine}"
        );
        let outer = dq.stages.last().unwrap();
        assert!(
            outer.upstream_stage_ids.iter().all(|&id| id == 2),
            "both placeholders pull the shared combine output: {outer:?}"
        );
    }

    /// Branches whose DISTINCT arguments differ cannot share one GROUP BY — a single leaf
    /// grouping by both arguments would multiply cardinality — so each keeps its own
    /// co-located distinct sub-DAG (and its own scan).
    #[tokio::test]
    async fn distinct_branches_with_different_args_do_not_merge() {
        let lp = logical_plan(
            "SELECT * FROM \
               (SELECT count(DISTINCT v) AS d1 FROM t WHERE k > 0) a1, \
               (SELECT count(DISTINCT k) AS d2 FROM t WHERE k > 1) a2",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("incompatible branches still plan");

        assert_eq!(
            dq.stages.len(),
            7,
            "two 3-stage co-located sub-DAGs + outer: {dq:?}"
        );
        let leaves: Vec<_> = dq
            .stages
            .iter()
            .filter(|s| s.upstream_stage_ids.is_empty())
            .collect();
        assert_eq!(
            leaves.len(),
            2,
            "each DISTINCT argument keeps its own scan: {dq:?}"
        );
        assert!(
            leaves.iter().all(|s| s.sql.contains("GROUP BY")),
            "each branch's stage 0 is its own partial dedup: {leaves:?}"
        );
        assert!(
            !dq.stages.iter().any(|s| s.sql.contains("FILTER (WHERE")),
            "no FILTER-merged leaf: {dq:?}"
        );
    }

    /// Mergeability rules for DISTINCT-carrying branches: only `COUNT(DISTINCT arg)` aggregates
    /// all sharing one argument are eligible; mixed arguments or a non-count DISTINCT decline
    /// (they keep their own sub-DAGs, planned by the co-located machinery or honestly rejected).
    #[tokio::test]
    async fn distinct_branch_mergeability_rules() {
        // Mixed DISTINCT arguments in one branch: not mergeable.
        let lp = logical_plan(
            "SELECT * FROM \
               (SELECT count(DISTINCT v) AS dv, count(DISTINCT k) AS dk FROM t WHERE k > 0) a1, \
               (SELECT count(*) AS n FROM t) a2",
        )
        .await;
        let mut branches = Vec::new();
        collect_sharded_branches(&lp, &[], &mut branches);
        assert_eq!(branches.len(), 2);
        assert!(
            mergeable_branch(branches[0].node, &[]).is_none(),
            "mixed DISTINCT arguments must decline the merge"
        );
        let plain = mergeable_branch(branches[1].node, &[]).expect("plain branch merges");
        assert!(plain.distinct_arg.is_none());

        // A non-count DISTINCT aggregate: not mergeable.
        let lp = logical_plan(
            "SELECT * FROM \
               (SELECT sum(DISTINCT v) AS sv FROM t WHERE k > 0) a1, \
               (SELECT count(*) AS n FROM t) a2",
        )
        .await;
        let mut branches = Vec::new();
        collect_sharded_branches(&lp, &[], &mut branches);
        assert!(
            mergeable_branch(branches[0].node, &[]).is_none(),
            "sum(DISTINCT) must decline the merge"
        );

        // COUNT(DISTINCT) alongside plain aggregates: mergeable, keyed on the argument.
        let lp = logical_plan(
            "SELECT * FROM \
               (SELECT count(DISTINCT v) AS dv, sum(v) AS s FROM t WHERE k > 0) a1, \
               (SELECT count(*) AS n FROM t) a2",
        )
        .await;
        let mut branches = Vec::new();
        collect_sharded_branches(&lp, &[], &mut branches);
        let m = mergeable_branch(branches[0].node, &[]).expect("count-distinct branch merges");
        assert_eq!(m.distinct_arg.as_deref(), Some("t.v"));
    }
}
