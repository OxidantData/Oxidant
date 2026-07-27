//! Left-deep shuffle-join chains for two or more sharded tables.
//!
//! A single equijoin between exactly two sharded leaf scans reuses the proven two-table path in
//! [`super::stage_planner`] (**inner only**). Longer chains — and any LEFT / RIGHT / FULL / SEMI /
//! ANTI equijoin — emit pairwise shuffle stages (intermediate consume+produce) with flattened
//! column names (`alias__col`), then partial/final aggregate.
//!
//! ## Outer / semi / anti correctness
//!
//! Both sides of a sharded–sharded equijoin are hash-partitioned on the join key so every key's
//! rows co-locate on one worker. That is sufficient for:
//! - **LEFT / RIGHT / FULL OUTER**: null-extension is local to the worker that owns the key;
//!   unmatched rows never need another worker's partition.
//! - **SEMI / ANTI**: matching is presence-only and likewise key-local.
//!
//! Replicated dimensions that appear in the chain fold into the next shuffle-join stage as
//! local broadcast joins (always `JOIN`, since the dim is complete on every worker).

use std::collections::HashMap;

use datafusion::logical_expr::{Expr, JoinType, LogicalPlan};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};

use super::stage_planner::{
    base_tables, build_finalize, column_name, distinct_stage_sql, equijoin_from_filter, expr_sql,
    recombine_stage_sql, sanitize_generated_sql, shuffle_join_two_tables, simple_table_scan,
    AggSpec, DistributedQuery, Peeled, SimpleScan,
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

    build_chain(p, sharded, replicated, leftmost, &steps)
}

struct ChainStep<'a> {
    right: SimpleScan<'a>,
    left_key: Expr,
    right_key: Expr,
    join_type: JoinType,
}

fn supported_shuffle_join_type(jt: JoinType) -> Result<()> {
    match jt {
        JoinType::Inner
        | JoinType::Left
        | JoinType::Right
        | JoinType::Full
        | JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::RightSemi
        | JoinType::RightAnti => Ok(()),
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
        JoinType::RightSemi => Ok("RIGHT SEMI JOIN"),
        JoinType::RightAnti => Ok("RIGHT ANTI JOIN"),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: cannot emit SQL for join type `{other}`"
        ))),
    }
}

/// SEMI / ANTI / mark joins project only one side.
fn projects_right_side(jt: JoinType) -> bool {
    matches!(
        jt,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
    )
}

fn extract_equijoin_chain(lp: &LogicalPlan) -> Result<(SimpleScan<'_>, Vec<ChainStep<'_>>)> {
    fn walk(lp: &LogicalPlan) -> Result<(SimpleScan<'_>, Vec<ChainStep<'_>>)> {
        match lp {
            LogicalPlan::Projection(p) => walk(p.input.as_ref()),
            LogicalPlan::Filter(f) => walk(f.input.as_ref()),
            LogicalPlan::Join(j) => {
                supported_shuffle_join_type(j.join_type)?;
                let (left_key, right_key) = single_equijoin_key(j)?;
                let right = simple_table_scan(j.right.as_ref())?;
                match simple_table_scan(j.left.as_ref()) {
                    Ok(leftmost) => Ok((
                        leftmost,
                        vec![ChainStep {
                            right,
                            left_key,
                            right_key,
                            join_type: j.join_type,
                        }],
                    )),
                    Err(_) => {
                        let (leftmost, mut steps) = walk(j.left.as_ref())?;
                        steps.push(ChainStep {
                            right,
                            left_key,
                            right_key,
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

fn single_equijoin_key(join: &datafusion::logical_expr::Join) -> Result<(Expr, Expr)> {
    match join.on.as_slice() {
        [(l, r)] => {
            if join.filter.is_some() {
                return Err(Error::Unsupported(
                    "auto-distribute: shuffle join with non-equi filter not yet supported".into(),
                ));
            }
            Ok((l.clone(), r.clone()))
        }
        [] => equijoin_from_filter(join.filter.as_ref()),
        _ => Err(Error::Unsupported(format!(
            "auto-distribute: shuffle join supports a single equijoin key, found {}",
            join.on.len()
        ))),
    }
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
        Some(pred) => format!("FROM {} WHERE {pred}", scan.table),
        None => format!("FROM {}", scan.table),
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

        let left_key_name = column_name(&step.left_key)?;
        let right_key_name = column_name(&step.right_key)?;
        let left_key_rel = relation_of(&step.left_key)?;
        let left_key_alias = alias_by_relation
            .get(&left_key_rel)
            .cloned()
            .unwrap_or(left_key_rel);

        let left_stage_id = match &left_side {
            LeftSide::Leaf => {
                let (sql, flats) = leaf_stage_sql(&leftmost);
                let key_idx = flat_key_index(&flats, &left_key_alias, &left_key_name)?;
                let id = next_id;
                next_id += 1;
                stages.push(StageDef::new(id, sql, vec![], vec![key_idx]));
                left_flats = flats;
                id
            }
            LeftSide::Stage { id } => {
                let _ = flat_key_index(&left_flats, &left_key_alias, &left_key_name)?;
                *id
            }
        };

        let (right_sql, right_flats) = leaf_stage_sql(&step.right);
        let right_key_idx = flat_key_index(&right_flats, &right_alias, &right_key_name)?;
        let right_id = next_id;
        next_id += 1;
        stages.push(StageDef::new(
            right_id,
            right_sql,
            vec![],
            vec![right_key_idx],
        ));

        let on_sql = format!(
            "l.{} = r.{}",
            flat_col(&left_key_alias, &left_key_name),
            flat_col(&right_alias, &right_key_name)
        );
        let join_kw = sql_join_keyword(step.join_type)?;
        let mut join_from =
            format!("FROM shuffle_input_0 AS l {join_kw} shuffle_input_1 AS r ON {on_sql}");
        for &bi in &pending_bcast {
            let b = &steps[bi];
            let b_alias = scan_alias(&b.right);
            let b_left_col = column_name(&b.left_key)?;
            let b_left_rel = relation_of(&b.left_key)?;
            let b_left_alias = alias_by_relation
                .get(&b_left_rel)
                .cloned()
                .unwrap_or(b_left_rel);
            let b_right_col = column_name(&b.right_key)?;
            // Replicated dims are complete on every worker — always an inner JOIN fold.
            join_from.push_str(&format!(
                " JOIN {} AS {b_alias} ON l.{} = {b_alias}.{b_right_col}",
                b.right.table,
                flat_col(&b_left_alias, &b_left_col)
            ));
            if let Some(pred) = &b.right.filter_sql {
                join_from.push_str(&format!(" AND ({pred})"));
            }
            alias_by_relation.insert(b.right.table.to_string(), b_alias.to_string());
            alias_by_relation.insert(b_alias.to_string(), b_alias.to_string());
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

        // Intermediate join output; hash by next sharded join's left key when possible.
        let mut proj = Vec::new();
        let mut new_flats = Vec::new();
        for c in &left_flats {
            proj.push(format!("l.{c} AS {c}"));
            new_flats.push(c.clone());
        }
        if projects_right_side(step.join_type) {
            for c in &right_flats {
                proj.push(format!("r.{c} AS {c}"));
                new_flats.push(c.clone());
            }
        } else if matches!(step.join_type, JoinType::RightSemi | JoinType::RightAnti) {
            // Right semi/anti keep the right schema only.
            proj.clear();
            new_flats.clear();
            for c in &right_flats {
                proj.push(format!("r.{c} AS {c}"));
                new_flats.push(c.clone());
            }
        }
        let (hash_alias, hash_col) =
            next_sharded_left_key(steps, i + 1, sharded, &alias_by_relation)
                .unwrap_or((left_key_alias.clone(), left_key_name.clone()));
        let hash_idx = flat_key_index(&new_flats, &hash_alias, &hash_col)?;
        let join_id = next_id;
        next_id += 1;
        stages.push(StageDef::new(
            join_id,
            sanitize_generated_sql(&format!("SELECT {} {join_from}", proj.join(", "))),
            vec![left_stage_id, right_id],
            vec![hash_idx],
        ));
        left_side = LeftSide::Stage { id: join_id };
        left_flats = new_flats;
    }

    Err(Error::Unsupported(
        "auto-distribute: shuffle join chain did not produce a final aggregate stage".into(),
    ))
}

fn next_sharded_left_key(
    steps: &[ChainStep<'_>],
    from: usize,
    sharded: &[&str],
    alias_by_relation: &HashMap<String, String>,
) -> Option<(String, String)> {
    for step in steps.iter().skip(from) {
        if !sharded.contains(&step.right.table) {
            continue;
        }
        let col = column_name(&step.left_key).ok()?;
        let rel = relation_of(&step.left_key).ok()?;
        let alias = alias_by_relation.get(&rel).cloned().unwrap_or(rel);
        return Some((alias, col));
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

    let mut remap: HashMap<String, String> = HashMap::new();
    for (j, g) in p.agg.group_expr.iter().enumerate() {
        remap.insert(g.schema_name().to_string(), format!("g{j}"));
    }
    for (i, a) in p.agg.aggr_expr.iter().enumerate() {
        remap.insert(a.schema_name().to_string(), format!("r{i}"));
    }

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

    #[test]
    fn sql_join_keyword_covers_outer_and_semi() {
        assert_eq!(sql_join_keyword(JoinType::Left).unwrap(), "LEFT JOIN");
        assert_eq!(sql_join_keyword(JoinType::Full).unwrap(), "FULL OUTER JOIN");
        assert_eq!(
            sql_join_keyword(JoinType::LeftAnti).unwrap(),
            "LEFT ANTI JOIN"
        );
        assert!(projects_right_side(JoinType::Left));
        assert!(!projects_right_side(JoinType::LeftSemi));
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
