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

use std::collections::HashMap;

use datafusion::logical_expr::{Expr, JoinType, LogicalPlan};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};

use super::stage_planner::{
    base_tables, build_finalize, build_remap, collect_equijoin_keys, column_name,
    distinct_stage_sql, expr_sql, recombine_stage_sql, sanitize_generated_sql,
    shuffle_join_two_tables, simple_table_scan, AggSpec, DistributedQuery, Peeled, SimpleScan,
};
use crate::driver::StageDef;

/// Plan a left-deep shuffle-join chain (+ grouped aggregation) over `sharded.len() >= 2` tables.
pub(crate) fn plan_shuffle_join_chain(
    p: &Peeled<'_>,
    sharded: &[&str],
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let (leftmost, steps) = extract_equijoin_chain(&p.agg.input)?;
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
        && sharded.contains(&leftmost.table)
        && sharded.contains(&steps[0].right.table)
    {
        return shuffle_join_two_tables(p, sharded);
    }

    for step in &steps {
        let t = step.right.table;
        if !sharded.contains(&t) && !replicated.contains(&t) {
            return Err(Error::Unsupported(format!(
                "auto-distribute: join chain table `{t}` is neither sharded nor replicated"
            )));
        }
    }
    if !sharded.contains(&leftmost.table) {
        return Err(Error::Unsupported(
            "auto-distribute: left-deep shuffle chain requires a sharded leftmost table".into(),
        ));
    }

    ensure_semi_anti_aggs_ok(p, &steps)?;

    build_chain(p, sharded, replicated, leftmost, &steps)
}

struct ChainStep<'a> {
    right: SimpleScan<'a>,
    /// Equijoin key pairs `(left, right)` — one or more for composite keys (KAN-10).
    keys: Vec<(Expr, Expr)>,
    residual_filter: Option<Expr>,
    join_type: JoinType,
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
            dropped_relations.push(step.right.table.to_string());
            if let Some(a) = step.right.alias {
                dropped_relations.push(a.to_string());
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

fn extract_equijoin_chain(lp: &LogicalPlan) -> Result<(SimpleScan<'_>, Vec<ChainStep<'_>>)> {
    fn walk(lp: &LogicalPlan) -> Result<(SimpleScan<'_>, Vec<ChainStep<'_>>)> {
        match lp {
            LogicalPlan::Projection(p) => walk(p.input.as_ref()),
            LogicalPlan::Filter(f) => walk(f.input.as_ref()),
            // KAN-11: CTE / subquery aliases wrap otherwise left-deep equijoin trees.
            LogicalPlan::SubqueryAlias(s) => walk(s.input.as_ref()),
            LogicalPlan::Sort(s) => walk(s.input.as_ref()),
            LogicalPlan::Limit(l) => walk(l.input.as_ref()),
            LogicalPlan::Distinct(d) => walk(d.input().as_ref()),
            LogicalPlan::Join(j) => {
                supported_shuffle_join_type(j.join_type)?;
                let (keys, residual_filter) = equijoin_keys(j)?;
                let right = simple_table_scan(j.right.as_ref())?;
                match simple_table_scan(j.left.as_ref()) {
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
                        let (leftmost, mut steps) = walk(j.left.as_ref())?;
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
    walk(lp)
}

/// Extract one or more equijoin key pairs plus any non-equality residual (KAN-10 / D-2.7 / D-2.9).
fn equijoin_keys(
    join: &datafusion::logical_expr::Join,
) -> Result<super::stage_planner::EquijoinKeys> {
    let (keys, residual) = collect_equijoin_keys(&join.on, join.filter.as_ref())?;
    if residual.is_some() && join.join_type != JoinType::Inner {
        return Err(Error::Unsupported(
            "auto-distribute: residual filters on outer/semi/anti shuffle joins are not supported"
                .into(),
        ));
    }
    Ok((keys, residual))
}

fn flat_col(alias: &str, col: &str) -> String {
    format!("{alias}__{col}")
}

fn scan_alias<'a>(scan: &SimpleScan<'a>) -> &'a str {
    scan.alias.unwrap_or(scan.table)
}

fn leaf_stage_sql(scan: &SimpleScan<'_>) -> (String, Vec<String>) {
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

fn flat_key_index(flats: &[String], alias: &str, col: &str) -> Result<u32> {
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

fn flatten_expr(e: &Expr, alias_by_relation: &HashMap<String, String>) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                if let Some(rel) = &c.relation {
                    let rname = rel.table();
                    if let Some(alias) = alias_by_relation.get(rname) {
                        return Ok(Transformed::yes(datafusion::prelude::col(flat_col(
                            alias, &c.name,
                        ))));
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
fn flatten_join_residual(
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

fn build_chain(
    p: &Peeled<'_>,
    sharded: &[&str],
    replicated: &[&str],
    leftmost: SimpleScan<'_>,
    steps: &[ChainStep<'_>],
) -> Result<DistributedQuery> {
    let mut alias_by_relation: HashMap<String, String> = HashMap::new();
    let left_alias = scan_alias(&leftmost).to_string();
    alias_by_relation.insert(leftmost.table.to_string(), left_alias.clone());
    alias_by_relation.insert(left_alias.clone(), left_alias.clone());

    let mut stages: Vec<StageDef> = Vec::new();
    let mut next_id: u32 = 0;

    enum LeftSide {
        Leaf,
        Stage { id: u32 },
    }
    let mut left_side = LeftSide::Leaf;
    let mut left_flats: Vec<String> = Vec::new();
    let mut pending_bcast: Vec<usize> = Vec::new(); // indices into steps

    let n = steps.len();
    for i in 0..n {
        let step = &steps[i];
        let right_alias = scan_alias(&step.right).to_string();
        alias_by_relation.insert(step.right.table.to_string(), right_alias.clone());
        alias_by_relation.insert(right_alias.clone(), right_alias.clone());

        let right_is_sharded = sharded.contains(&step.right.table);
        let is_last = i + 1 == n;

        if !right_is_sharded {
            if !replicated.contains(&step.right.table) {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: `{}` must be listed in replicated",
                    step.right.table
                )));
            }
            pending_bcast.push(i);
            if is_last {
                if matches!(left_side, LeftSide::Leaf) {
                    return Err(Error::Unsupported(
                        "auto-distribute: join chain has no sharded–sharded shuffle boundary"
                            .into(),
                    ));
                }
                // Trailing broadcasts: fold into final agg by synthesizing a no-op right?
                // Require the last step to be sharded for now.
                return Err(Error::Unsupported(
                    "auto-distribute: trailing replicated-only joins after the last sharded \
                     shuffle join are not yet folded — mark them replicated and keep a sharded \
                     table as the rightmost join, or use the broadcast (1-sharded) path"
                        .into(),
                ));
            }
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
        let left_stage_id = match &left_side {
            LeftSide::Leaf => {
                let (sql, flats) = leaf_stage_sql(&leftmost);
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
            LeftSide::Stage { id } => {
                for (alias, name) in &left_key_metas {
                    let _ = flat_key_index(&left_flats, alias, name)?;
                }
                *id
            }
        };

        let (right_sql, right_flats) = leaf_stage_sql(&step.right);
        let mut right_key_idxs = Vec::with_capacity(right_key_names.len());
        for name in &right_key_names {
            right_key_idxs.push(flat_key_index(&right_flats, &right_alias, name)?);
        }
        let right_id = next_id;
        next_id += 1;
        stages.push(StageDef::new(right_id, right_sql, vec![], right_key_idxs));

        let on_sql = left_key_metas
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
        let join_kw = sql_join_keyword(step.join_type)?;
        let mut join_from =
            format!("FROM shuffle_input_0 AS l {join_kw} shuffle_input_1 AS r ON {on_sql}");
        let replicated_aliases: Vec<String> = pending_bcast
            .iter()
            .map(|&bi| scan_alias(&steps[bi].right).to_string())
            .collect();
        for &bi in &pending_bcast {
            let b = &steps[bi];
            let b_alias = scan_alias(&b.right);
            let mut on_parts = Vec::with_capacity(b.keys.len());
            for (b_left, b_right) in &b.keys {
                let b_left_col = column_name(b_left)?;
                let b_left_rel = relation_of(b_left)?;
                let b_left_alias = alias_by_relation
                    .get(&b_left_rel)
                    .cloned()
                    .unwrap_or(b_left_rel);
                let b_right_col = column_name(b_right)?;
                on_parts.push(format!(
                    "l.{} = {b_alias}.{b_right_col}",
                    flat_col(&b_left_alias, &b_left_col)
                ));
            }
            // Replicated dims are complete on every worker — always an inner JOIN fold.
            join_from.push_str(&format!(
                " JOIN {} AS {b_alias} ON {}",
                b.right.table,
                on_parts.join(" AND ")
            ));
            if let Some(pred) = &b.right.filter_sql {
                join_from.push_str(&format!(" AND ({pred})"));
            }
            alias_by_relation.insert(b.right.table.to_string(), b_alias.to_string());
            alias_by_relation.insert(b_alias.to_string(), b_alias.to_string());
        }

        let up = Unparser::default();
        let residual_sql = pending_bcast
            .iter()
            .filter_map(|&bi| steps[bi].residual_filter.as_ref())
            .chain(step.residual_filter.iter())
            .map(|residual| {
                expr_sql(
                    &up,
                    &flatten_join_residual(
                        residual,
                        &alias_by_relation,
                        &right_alias,
                        &replicated_aliases,
                    ),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if !residual_sql.is_empty() {
            join_from.push_str(&format!(" WHERE {}", residual_sql.join(" AND ")));
        }
        pending_bcast.clear();

        if is_last {
            return finish_with_aggregate(
                p,
                &alias_by_relation,
                &join_from,
                left_stage_id,
                right_id,
                &mut stages,
                &mut next_id,
            );
        }

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
        let hash_keys = next_sharded_left_keys(steps, i + 1, sharded, &alias_by_relation)
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

fn next_sharded_left_keys(
    steps: &[ChainStep<'_>],
    from: usize,
    sharded: &[&str],
    alias_by_relation: &HashMap<String, String>,
) -> Option<Vec<(String, String)>> {
    for step in steps.iter().skip(from) {
        if !sharded.contains(&step.right.table) {
            continue;
        }
        let mut out = Vec::with_capacity(step.keys.len());
        for (left_key, _) in &step.keys {
            let col = column_name(left_key).ok()?;
            let rel = relation_of(left_key).ok()?;
            let alias = alias_by_relation.get(&rel).cloned().unwrap_or(rel);
            out.push((alias, col));
        }
        return Some(out);
    }
    None
}

fn finish_with_aggregate(
    p: &Peeled<'_>,
    alias_by_relation: &HashMap<String, String>,
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
        .map(|g| expr_sql(&up, &flatten_expr(g, alias_by_relation)))
        .collect::<Result<_>>()?;
    let aggs: Vec<AggSpec> = p
        .agg
        .aggr_expr
        .iter()
        .map(|a| {
            let mut spec = AggSpec::classify(a)?;
            if let Expr::AggregateFunction(af) = a {
                if let Some(arg) = af.params.args.first() {
                    spec.arg_sql = expr_sql(&up, &flatten_expr(arg, alias_by_relation))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use weft_loom::arrow::array::Int64Array;
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::arrow::record_batch::RecordBatch;

    use crate::shuffle::partition::hash_partition;
    use weft_loom::arrow::array::Array;

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
}
