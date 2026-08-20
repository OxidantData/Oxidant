//! AUTO CDC (SCD Type 1) option validation shared by config load and SDP wiring.

use oxidant_common::{Error, Result};

use crate::AutoCdcConfig;

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
    }
    if config.sequence_by.trim().is_empty() {
        return Err(Error::Io(format!(
            "table `{table_name}` AUTO CDC requires `sequence_by`"
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
    fn rejects_conflicting_column_lists() {
        let mut cfg = sample();
        cfg.column_list = Some(vec!["id".into()]);
        cfg.except_column_list = Some(vec!["op".into()]);
        validate(&cfg, "t").expect_err("conflicting column lists");
    }
}
