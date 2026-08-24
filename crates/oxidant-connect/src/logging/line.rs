//! One log line, in the three forms §6 requires to agree: the live tail's string, the text
//! file's line, and the Parquet row.
//!
//! The rolling writer is a `tracing` layer in its own right, not a re-serializer of
//! [`super::LogBuffer`] strings — but both read the same [`LogLine`], so the tail and the file
//! cannot drift apart on anything but dedup (§6, stated).
//!
//! `format_event` used to emit `[LEVEL] target - fields` with **no timestamp**. Converting that
//! to a columnar log would have produced a Parquet file with no usable time column, and §6b's
//! time-range filters would have had nothing to filter on. So the rendered line now carries an
//! RFC-3339 UTC timestamp prefix, and `GET /api/v1/logs` returns it — an announced change in
//! what that endpoint's strings look like (§8).

use chrono::{DateTime, Utc};

/// The rendered form's timestamp: RFC-3339, UTC, milliseconds. Same spelling as the journal's
/// `ts` field, so an operator correlating a statement record with a log line is comparing
/// like with like.
pub(crate) const TS_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3fZ";

/// A `tracing` event, decomposed into the columns the Parquet schema stores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LogLine {
    /// RFC-3339 UTC with milliseconds.
    pub ts: String,
    pub level: &'static str,
    pub target: String,
    /// `k=v, k2=v2` — the field list `format_event` has always produced.
    pub fields: String,
}

/// Escape the two bytes that would turn one event into two file lines.
///
/// **One line in, one line out, always.** The rendered format is newline-delimited with a fully
/// parseable prefix, so a newline inside a field value produces a second physical line that
/// `parse_line` accepts as a genuine event — with an attacker-chosen timestamp, level, target,
/// message and fields, indistinguishable in the Parquet from a real engine event. The values are
/// remote-controlled in practice: `flight.rs` logs `error = %status.message()` from a worker, and
/// `rest.rs`/`lib.rs`/`distributed.rs` all log `error = %e` from DataFusion and connector errors
/// that routinely embed multi-line plan text and remote messages.
///
/// It is a correctness problem before it is a security one. Without this, a routine multi-line
/// error became N physical lines and the continuation lines landed in the Parquet as
/// `message`-only rows with **null `ts` and null `level`** — so §6b's time-range and level
/// filters silently excluded exactly the error events an operator was searching for.
///
/// Escaping is idempotent: the two-character sequence `\n` this produces contains no newline, so
/// a value that has already been through `{:?}` (which is what `record_str` does) is unchanged.
pub(crate) fn escape_line_breaks(raw: &str) -> std::borrow::Cow<'_, str> {
    if !raw.contains(['\n', '\r']) {
        return std::borrow::Cow::Borrowed(raw);
    }
    let mut out = String::with_capacity(raw.len() + 8);
    for c in raw.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    std::borrow::Cow::Owned(out)
}

impl LogLine {
    /// `<ts> [LEVEL] target - fields`, or `<ts> [LEVEL] target` for an event with no fields.
    ///
    /// `target` and `fields` are escaped here as well as at the visitor that builds them: this is
    /// the last point before `writeln!`, and "one event is one line" has to hold for every
    /// [`LogLine`], not only for the ones `format_event` assembled.
    pub(crate) fn render(&self) -> String {
        let target = escape_line_breaks(&self.target);
        if self.fields.is_empty() {
            format!("{} [{}] {}", self.ts, self.level, target)
        } else {
            format!(
                "{} [{}] {} - {}",
                self.ts,
                self.level,
                target,
                escape_line_breaks(&self.fields)
            )
        }
    }

    /// Everything but the timestamp — the dedup key.
    ///
    /// Dedup **cannot** compare rendered lines: the timestamp differs on every event, so no two
    /// consecutive lines are ever byte-identical and a hot error loop would collapse to nothing.
    pub(crate) fn is_repeat_of(&self, other: &Self) -> bool {
        self.level == other.level && self.target == other.target && self.fields == other.fields
    }

    /// The `… repeated N times` summary a suppressed run flushes as, wearing the level and
    /// target of the line it summarises so a level filter still finds it (PR4).
    pub(crate) fn repeat_summary(&self, now: DateTime<Utc>, count: u64) -> Self {
        Self {
            ts: now.format(TS_FORMAT).to_string(),
            level: self.level,
            target: self.target.clone(),
            fields: format!("… repeated {count} times"),
        }
    }
}

/// A parsed line, ready for the Parquet writer.
///
/// The parse is **best-effort and says so**: the text file is authoritative (§6), and a line it
/// cannot decompose is preserved whole in `message` rather than dropped. Since
/// [`escape_line_breaks`] this happens only for a line some *other* producer wrote into the log
/// directory — the writer's own output is one physical line per event by construction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ParsedLine {
    /// Epoch milliseconds, or `None` when the line carried no parseable timestamp.
    pub ts_ms: Option<i64>,
    pub level: Option<String>,
    pub target: Option<String>,
    pub message: Option<String>,
    /// The non-`message` fields as a JSON object, or `None` when there were none.
    pub fields_json: Option<String>,
}

/// Decompose a rendered line back into columns.
pub(crate) fn parse_line(line: &str) -> ParsedLine {
    let whole = || ParsedLine {
        message: Some(line.to_string()),
        ..Default::default()
    };
    let Some((ts_raw, rest)) = line.split_once(' ') else {
        return whole();
    };
    let Ok(ts) = DateTime::parse_from_rfc3339(ts_raw) else {
        return whole();
    };
    let Some(rest) = rest.strip_prefix('[') else {
        return whole();
    };
    let Some((level, rest)) = rest.split_once(']') else {
        return whole();
    };
    let rest = rest.trim_start();
    let (target, fields) = match rest.split_once(" - ") {
        Some((target, fields)) => (target, fields),
        None => (rest, ""),
    };
    let (message, other) = split_fields(fields);
    ParsedLine {
        ts_ms: Some(ts.with_timezone(&Utc).timestamp_millis()),
        level: Some(level.to_string()),
        target: Some(target.to_string()),
        message,
        fields_json: (!other.is_empty()).then(|| {
            serde_json::Value::Object(
                other
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::String(v)))
                    .collect(),
            )
            .to_string()
        }),
    }
}

/// Split a `k=v, k2=v2` field list into `(message, other pairs)`.
///
/// Two hazards, both real in this tree's own log lines:
///
/// - a string field is rendered with `{:?}`, so its value is quoted and may itself contain
///   `", "` — the splitter tracks quoting and its backslash escape;
/// - `message` is rendered *unquoted* (it is `format_args!` under `{:?}`), so a message
///   containing `, ` splits into a fragment with no `=`. A fragment with no `=` is therefore
///   appended back to the previous value rather than discarded.
fn split_fields(fields: &str) -> (Option<String>, Vec<(String, String)>) {
    if fields.is_empty() {
        return (None, Vec::new());
    }
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut message = None;
    for token in split_top_level(fields) {
        match token.split_once('=') {
            // A key must look like one; `a=b` inside a prose fragment must not open a new field.
            Some((key, value)) if is_field_name(key) => {
                if key == "message" {
                    message = Some(value.to_string());
                } else {
                    pairs.push((key.to_string(), value.to_string()));
                }
            }
            _ => match (&mut message, pairs.last_mut()) {
                (_, Some((_, last))) => {
                    last.push_str(", ");
                    last.push_str(&token);
                }
                (Some(msg), None) => {
                    msg.push_str(", ");
                    msg.push_str(&token);
                }
                (None, None) => message = Some(token),
            },
        }
    }
    (message, pairs)
}

/// `tracing` field names are Rust identifiers plus `.`; anything else is prose.
fn is_field_name(raw: &str) -> bool {
    !raw.is_empty()
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
}

/// Split on `", "` outside double quotes.
fn split_top_level(fields: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escaped = false;
    let mut chars = fields.chars().peekable();
    while let Some(c) = chars.next() {
        if escaped {
            current.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_quotes => {
                current.push(c);
                escaped = true;
            }
            '"' => {
                in_quotes = !in_quotes;
                current.push(c);
            }
            ',' if !in_quotes && chars.peek() == Some(&' ') => {
                chars.next();
                out.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    out.push(current);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(fields: &str) -> LogLine {
        LogLine {
            ts: "2026-08-23T14:00:00.500Z".to_string(),
            level: "INFO",
            target: "oxidant_execution".to_string(),
            fields: fields.to_string(),
        }
    }

    /// The timestamp prefix is the whole reason a rolled log has a usable `ts` column.
    #[test]
    fn a_rendered_line_round_trips_through_the_parser() {
        let parsed = parse_line(&line("message=disk sweep, used_bytes=10").render());
        assert_eq!(parsed.level.as_deref(), Some("INFO"));
        assert_eq!(parsed.target.as_deref(), Some("oxidant_execution"));
        assert_eq!(parsed.message.as_deref(), Some("disk sweep"));
        assert_eq!(
            parsed.fields_json.as_deref(),
            Some(r#"{"used_bytes":"10"}"#)
        );
        let ts = parsed.ts_ms.expect("a usable time column");
        assert_eq!(
            chrono::DateTime::from_timestamp_millis(ts)
                .unwrap()
                .format(TS_FORMAT)
                .to_string(),
            "2026-08-23T14:00:00.500Z"
        );
    }

    #[test]
    fn an_event_with_no_fields_still_parses() {
        let parsed = parse_line(&line("").render());
        assert_eq!(parsed.target.as_deref(), Some("oxidant_execution"));
        assert_eq!(parsed.message, None);
        assert_eq!(parsed.fields_json, None);
        assert!(parsed.ts_ms.is_some());
    }

    /// A quoted string field may contain the very separator the splitter uses. Splitting
    /// naively on `", "` truncated the value and invented a field named after its tail.
    #[test]
    fn quoted_values_may_contain_the_separator() {
        let parsed = parse_line(&line(r#"error="a, b", count=2"#).render());
        assert_eq!(
            parsed.fields_json.as_deref(),
            Some(r#"{"count":"2","error":"\"a, b\""}"#)
        );
    }

    /// `message` is rendered unquoted, so a message with a comma splits. The fragment is put
    /// back rather than dropped — the file is authoritative and the Parquet must not lose text.
    #[test]
    fn an_unquoted_message_with_a_comma_is_rejoined() {
        let parsed = parse_line(&line("message=planned 3 stages, 2 replicated").render());
        assert_eq!(
            parsed.message.as_deref(),
            Some("planned 3 stages, 2 replicated")
        );
        assert_eq!(parsed.fields_json, None);
    }

    /// A line the parser cannot decompose keeps its text. A `tracing` field value carrying a
    /// newline is already two file lines by the time the converter sees it.
    #[test]
    fn an_undecomposable_line_is_preserved_whole() {
        let parsed = parse_line("    at oxidant_execution::plan::stage_planner");
        assert_eq!(parsed.ts_ms, None);
        assert_eq!(
            parsed.message.as_deref(),
            Some("    at oxidant_execution::plan::stage_planner")
        );
        assert_eq!(parsed.level, None);
    }

    /// The renderer is the last gate: whatever built the [`LogLine`], one event is one line.
    #[test]
    fn render_never_emits_a_second_physical_line() {
        let mut hostile = line("message=ok\nforged=1");
        hostile.target = "oxidant\rexecution".to_string();
        let rendered = hostile.render();
        assert_eq!(rendered.lines().count(), 1, "{rendered:?}");
        assert!(rendered.contains("message=ok\\nforged=1"), "{rendered:?}");
        assert!(rendered.contains("oxidant\\rexecution"), "{rendered:?}");
        // Idempotent: escaping an already-escaped value changes nothing.
        let mut again = hostile.clone();
        again.fields = escape_line_breaks(&hostile.fields).into_owned();
        assert_eq!(again.render(), rendered);
    }

    /// Dedup compares everything *but* the timestamp. Comparing rendered lines would have made
    /// every line unique and suppressed nothing.
    #[test]
    fn the_dedup_key_excludes_the_timestamp() {
        let a = line("message=pool exhausted");
        let mut b = a.clone();
        b.ts = "2026-08-23T14:00:09.999Z".to_string();
        assert!(a.is_repeat_of(&b));
        assert_ne!(a.render(), b.render(), "the rendered lines do differ");

        let mut c = a.clone();
        c.fields = "message=pool recovered".to_string();
        assert!(!a.is_repeat_of(&c));
    }

    #[test]
    fn the_repeat_summary_keeps_the_suppressed_line_s_level_and_target() {
        let held = line("message=pool exhausted");
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T14:00:05.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let summary = held.repeat_summary(now, 412);
        assert_eq!(
            summary.render(),
            "2026-08-23T14:00:05.000Z [INFO] oxidant_execution - … repeated 412 times"
        );
    }
}
