//! Distributed shuffle: the control envelope carried in Flight tickets, hash partitioning of
//! stage output into per-downstream buckets, disk spill for large stage caches, and (gated)
//! serialized physical-plan fragments.
//!
//! The MVP shape is `partial-agg per worker → hash shuffle by key → re-aggregate per worker`,
//! which is the smallest real shuffle that proves the mechanism while only needing
//! SQL-expressible re-combinable aggregates (COUNT→SUM, SUM→SUM, MIN→MIN, MAX→MAX).

pub mod codec;
pub mod partition;
pub mod protocol;
pub mod spill;

pub use partition::hash_partition;
pub use protocol::{decode_ticket, ShuffleReadTicket, StageTicket, Ticket};
pub use spill::{estimated_batch_bytes, SpillStore};

/// The table name a stage's shuffle input is registered under before its SQL runs.
pub const SHUFFLE_INPUT_TABLE: &str = "shuffle_input";

/// Per-task shuffle-input table name: the planner's shared `shuffle_input` token scoped by
/// the consuming task's `(stage_id, partition_id)` (plus upstream index when a stage has
/// several inputs), so concurrent partition tasks of the SAME stage on one worker never
/// collide on register/deregister of the MemTable. The double underscore keeps these
/// names distinct from the planner's `shuffle_input_{i}` token pattern.
///
/// This is what lets the driver dispatch all of a stage's partition tasks to one worker
/// concurrently (F2); they previously had to be serialized per worker precisely because
/// every task registered the same `shuffle_input` table (KAN-32).
pub fn localized_shuffle_input_name(
    stage_id: u32,
    partition_id: u32,
    upstream_idx: Option<usize>,
) -> String {
    match upstream_idx {
        None => format!("{SHUFFLE_INPUT_TABLE}__s{stage_id}_p{partition_id}"),
        Some(i) => format!("{SHUFFLE_INPUT_TABLE}__s{stage_id}_p{partition_id}_{i}"),
    }
}

/// The upstream index of a planner `shuffle_input_{i}` token, or `None` when `ident` is
/// not exactly that pattern (bare `shuffle_input` included).
fn shuffle_input_token_index(ident: &str) -> Option<usize> {
    let suffix = ident.strip_prefix("shuffle_input_")?;
    if suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    suffix.parse().ok()
}

/// Rewrite the planner's shared `shuffle_input` / `shuffle_input_{i}` table tokens in a
/// stage's SQL to this task's localized names (see [`localized_shuffle_input_name`]) so
/// the worker can register this task's pulled inputs without racing sibling tasks.
///
/// Token-aware, not a blind string replace: whole identifiers only, and string literals /
/// quoted identifiers / comments are copied verbatim — a user literal like
/// `'shuffle_input'` or a comment mentioning it must not be renamed. A token
/// `shuffle_input_{i}` with `i >= upstreams` is left untouched on purpose: the worker
/// registers exactly one table per upstream, so a dangling token must keep failing as
/// "table not found" rather than be silently renamed onto a real table.
pub fn localize_shuffle_input_sql(
    sql: &str,
    stage_id: u32,
    partition_id: u32,
    upstreams: usize,
) -> String {
    if upstreams == 0 || !sql.contains(SHUFFLE_INPUT_TABLE) {
        return sql.to_string();
    }
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(sql.len() + 32);
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        // Quoted spans: 'string' / "identifier" / `identifier`, each self-escaped by doubling.
        if c == b'\'' || c == b'"' || c == b'`' {
            let quote = c;
            out.push(c);
            i += 1;
            while i < bytes.len() {
                out.push(bytes[i]);
                if bytes[i] == quote {
                    if i + 1 < bytes.len() && bytes[i + 1] == quote {
                        out.push(bytes[i + 1]);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Line comment: copy through end-of-line verbatim.
        if c == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(bytes[i]);
                i += 1;
            }
            continue;
        }
        // Block comment: copy through the closing marker verbatim.
        if c == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            out.push(b'/');
            out.push(b'*');
            i += 2;
            while i < bytes.len() {
                out.push(bytes[i]);
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    out.push(b'/');
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == b'_' || c.is_ascii_alphabetic() {
            let start = i;
            while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
                i += 1;
            }
            let ident = &sql[start..i];
            let replacement = if ident == SHUFFLE_INPUT_TABLE {
                Some(localized_shuffle_input_name(stage_id, partition_id, None))
            } else {
                shuffle_input_token_index(ident)
                    .filter(|idx| *idx < upstreams)
                    .map(|idx| localized_shuffle_input_name(stage_id, partition_id, Some(idx)))
            };
            match replacement {
                Some(name) => out.extend_from_slice(name.as_bytes()),
                None => out.extend_from_slice(ident.as_bytes()),
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    // Input was valid UTF-8; all inserted names are ASCII and quoted spans are copied whole.
    String::from_utf8(out).unwrap_or_else(|_| sql.to_string())
}

/// Producer scope used for stage-cache entries (and their spill files) written by push-based
/// `do_exchange`, where the producing task's partition id is not known to the receiver
/// (KAN-32). Pull-produced entries use the producing task's own partition id instead.
pub const PUSH_SRC: u32 = u32::MAX;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn localizes_bare_and_indexed_tokens() {
        let sql = "SELECT l.k0 FROM shuffle_input_0 AS l JOIN shuffle_input_1 AS r ON l.k0 = r.k0";
        let out = localize_shuffle_input_sql(sql, 7, 3, 2);
        assert_eq!(
            out,
            "SELECT l.k0 FROM shuffle_input__s7_p3_0 AS l JOIN shuffle_input__s7_p3_1 AS r ON l.k0 = r.k0"
        );
    }

    #[test]
    fn localizes_single_upstream_bare_token() {
        let sql = "SELECT k0, SUM(a0) FROM shuffle_input GROUP BY k0";
        let out = localize_shuffle_input_sql(sql, 2, 5, 1);
        assert_eq!(
            out,
            "SELECT k0, SUM(a0) FROM shuffle_input__s2_p5 GROUP BY k0"
        );
    }

    #[test]
    fn does_not_touch_literals_comments_or_longer_identifiers() {
        let sql = "SELECT 'shuffle_input' AS x, shuffle_input_01x.k0 FROM shuffle_input_0 -- shuffle_input here\n\
                   /* shuffle_input_1 */ WHERE shuffle_input_0.k0 = 1";
        let out = localize_shuffle_input_sql(sql, 1, 0, 1);
        // Literal, the over-long identifier (`shuffle_input_01x` is not `shuffle_input_<digits>`
        // boundary-wise but tokenizes as one identifier and is not a valid token), the
        // out-of-range index, and both comments stay verbatim; only real upstream-0 tokens move.
        assert!(out.contains("'shuffle_input'"), "{out}");
        assert!(out.contains("shuffle_input_01x.k0"), "{out}");
        assert!(out.contains("-- shuffle_input here"), "{out}");
        assert!(out.contains("/* shuffle_input_1 */"), "{out}");
        assert_eq!(out.matches("shuffle_input__s1_p0_0").count(), 2, "{out}");
        assert!(!out.contains("shuffle_input__s1_p0_1"), "{out}");
    }

    #[test]
    fn out_of_range_upstream_index_left_dangling() {
        // The worker registers one table per upstream; `shuffle_input_2` with only two
        // upstreams must stay as-is so it fails loudly as an unknown table.
        let sql = "SELECT * FROM shuffle_input_2";
        assert_eq!(localize_shuffle_input_sql(sql, 4, 1, 2), sql);
    }

    #[test]
    fn no_upstreams_or_no_token_is_identity() {
        assert_eq!(localize_shuffle_input_sql("SELECT 1", 1, 1, 0), "SELECT 1");
        assert_eq!(
            localize_shuffle_input_sql("SELECT 1 FROM t", 1, 1, 2),
            "SELECT 1 FROM t"
        );
    }

    #[test]
    fn doubled_quote_inside_literal_does_not_end_span() {
        let sql = "SELECT 'it''s shuffle_input' AS s, k0 FROM shuffle_input";
        let out = localize_shuffle_input_sql(sql, 3, 2, 1);
        assert!(out.contains("'it''s shuffle_input'"), "{out}");
        assert!(out.contains("FROM shuffle_input__s3_p2"), "{out}");
    }
}
