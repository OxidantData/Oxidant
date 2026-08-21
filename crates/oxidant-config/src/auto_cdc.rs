//! AUTO CDC (SCD Type 1) option validation shared by config load and SDP wiring.

use oxidant_common::{Error, Result};

use crate::AutoCdcConfig;

/// Resolve a key / `SEQUENCE BY` reference to the plain column name it names, if it is one.
///
/// Databricks allows a struct expression for `SEQUENCE BY`; we do not, because the merge has to
/// compare the batch's ordering value against the one already persisted in the target, and only
/// a stored column survives across micro-batches. Backticks are accepted and stripped so a
/// reserved word spelled the way the SQL surface spells it still resolves.
pub fn simple_column(expr: &str) -> Option<String> {
    let trimmed = expr.trim();
    let unquoted = trimmed
        .strip_prefix('`')
        .and_then(|r| r.strip_suffix('`'))
        .unwrap_or(trimmed);
    if unquoted.is_empty() || unquoted.contains('`') {
        return None;
    }
    let mut chars = unquoted.chars();
    let first = chars.next()?;
    if !(first.is_alphabetic() || first == '_') {
        return None;
    }
    if !chars.all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    Some(unquoted.to_string())
}

/// Validate AUTO CDC options for a pipeline table.
pub fn validate(config: &AutoCdcConfig, table_name: &str) -> Result<()> {
    if config.source.trim().is_empty() {
        return Err(Error::Io(format!(
            "table `{table_name}` AUTO CDC requires a non-empty `source`"
        )));
    }
    if config.keys.is_empty() {
        return Err(Error::Io(format!(
            "table `{table_name}` AUTO CDC requires at least one key column"
        )));
    }
    for key in &config.keys {
        if key.trim().is_empty() {
            return Err(Error::Io(format!(
                "table `{table_name}` AUTO CDC key expressions must not be empty"
            )));
        }
        // Checked here rather than only at merge-planning time: keys and the sequence column are
        // syntactically checkable without a schema, and finding out that `KEYS (lower(id))` is
        // unsupported only when the first micro-batch runs is a needlessly late failure.
        if simple_column(key).is_none() {
            return Err(Error::Io(format!(
                "table `{table_name}` AUTO CDC key must be a plain column name, got `{key}` — \
                 the merge compares it against the value stored in the target, so only a stored \
                 column works"
            )));
        }
    }
    if config.sequence_by.trim().is_empty() {
        return Err(Error::Io(format!(
            "table `{table_name}` AUTO CDC requires `sequence_by`"
        )));
    }
    if simple_column(&config.sequence_by).is_none() {
        return Err(Error::Io(format!(
            "table `{table_name}` AUTO CDC `sequence_by` must be a plain column name, got `{}` — \
             the merge compares it against the sequence stored in the target, so only a stored \
             column works",
            config.sequence_by
        )));
    }
    if config.column_list.is_some() && config.except_column_list.is_some() {
        return Err(Error::Io(format!(
            "table `{table_name}` AUTO CDC cannot set both column_list and except_column_list"
        )));
    }
    if config.ignore_null_updates_columns.is_some() && config.ignore_null_updates_except.is_some() {
        return Err(Error::Io(format!(
            "table `{table_name}` AUTO CDC cannot set both ignore_null_updates column lists"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> AutoCdcConfig {
        AutoCdcConfig {
            source: "src".into(),
            keys: vec!["id".into()],
            sequence_by: "seq".into(),
            apply_as_deletes: None,
            apply_as_truncates: None,
            column_list: None,
            except_column_list: None,
            ignore_null_updates_columns: None,
            ignore_null_updates_except: None,
        }
    }

    #[test]
    fn rejects_missing_keys_and_sequence_by() {
        let mut cfg = sample();
        cfg.keys.clear();
        validate(&cfg, "t").expect_err("keys required");

        cfg = sample();
        cfg.sequence_by = "  ".into();
        validate(&cfg, "t").expect_err("sequence_by required");
    }

    #[test]
    fn rejects_expression_keys_and_sequence_by_at_define_time() {
        let mut cfg = sample();
        cfg.keys = vec!["lower(id)".into()];
        let err = validate(&cfg, "t").expect_err("expression key").to_string();
        assert!(err.contains("plain column name"), "{err}");

        cfg = sample();
        cfg.sequence_by = "struct(seq, id)".into();
        let err = validate(&cfg, "t")
            .expect_err("expression sequence_by")
            .to_string();
        assert!(err.contains("plain column name"), "{err}");

        // Backticks are how the SQL surface spells a reserved word; they must still pass.
        cfg = sample();
        cfg.keys = vec!["`order`".into()];
        cfg.sequence_by = "`seq`".into();
        validate(&cfg, "t").expect("backticked identifiers are plain columns");
    }

    #[test]
    fn rejects_conflicting_column_lists() {
        let mut cfg = sample();
        cfg.column_list = Some(vec!["id".into()]);
        cfg.except_column_list = Some(vec!["op".into()]);
        validate(&cfg, "t").expect_err("conflicting column lists");
    }
}
