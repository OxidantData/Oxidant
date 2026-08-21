//! `${NAME}` substitution in the config file.
//!
//! Paths in this config must be absolute ([`crate::OxidantConfig::resolve_paths`]), which is the
//! only rule with one unambiguous meaning — but hard-coding `/home/you/...` makes a config file
//! that cannot be committed, shared, or moved between a laptop and a container. Interpolation is
//! how a config stays portable *and* absolute: the variable supplies the absolute prefix.
//!
//! ```yaml
//! vars:
//!   DATA: /srv/oxidant          # overridable from the environment
//! catalogs:
//!   local:
//!     type: local
//!     warehouse: ${DATA}/warehouse
//!     tables:
//!       samples.nation: { format: delta, location: ${CONFIG_DIR}/../sample-data/delta/nation }
//! ```
//!
//! Substitution runs over the parsed YAML tree rather than the raw text. Textual substitution
//! would be one `#` or `:` inside a value away from silently reparsing as different YAML.
//!
//! One YAML wrinkle worth knowing: inside a **flow mapping** (`{ a: b }`), `{` and `}` are
//! structural, so a `${VAR}` value there must be quoted — `{ location: "${DATA}/t" }`. In block
//! style (one key per line) no quoting is needed.

use std::collections::BTreeMap;
use std::path::Path;

use oxidant_common::{Error, Result};
use serde::de::DeserializeOwned;
use serde_norway::Value;

/// Variables the config file cannot define or shadow, because they describe where the file *is*.
///
/// `CONFIG_DIR` is the one that makes a committed example runnable: it is the absolute directory
/// holding the config file, so `${CONFIG_DIR}/../data` means the same thing from any working
/// directory and on any machine.
const RESERVED: &[&str] = &["CONFIG_DIR", "PWD"];

/// Parse `text` as YAML, substitute every `${NAME}`, and deserialize the result.
pub(crate) fn interpolate_config<T: DeserializeOwned>(
    text: &str,
    source: Option<&Path>,
) -> Result<T> {
    let mut value: Value =
        serde_norway::from_str(text).map_err(|e| Error::Io(format!("parse error: {e}")))?;
    let table = variables(&value, source)?;
    substitute(&mut value, &table)?;
    serde_norway::from_value(value).map_err(|e| Error::Io(format!("parse error: {e}")))
}

/// A `vars:` value as text. Numbers and booleans are accepted so `PORT: 9092` need not be
/// quoted; anything structured (a list, a nested map) has no sensible textual form and is
/// skipped, which `deny_unknown_fields` on `vars: BTreeMap<String, String>` then reports.
fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

/// Build the lookup table: reserved names, then the environment, then the file's own `vars:`.
///
/// The environment beats `vars:` so a container can override a checked-in default without
/// editing the file — the same direction [`crate::OxidantConfig::apply_engine_env`] already
/// takes for `engine:`. Reserved names beat both, so `${CONFIG_DIR}` always means what it says.
fn variables(root: &Value, source: Option<&Path>) -> Result<BTreeMap<String, Option<String>>> {
    let mut table: BTreeMap<String, Option<String>> = BTreeMap::new();

    if let Some(Value::Mapping(vars)) = root.get("vars") {
        for (key, value) in vars {
            let (Some(key), Some(value)) = (key.as_str(), scalar_text(value)) else {
                continue;
            };
            if RESERVED.contains(&key) {
                return Err(Error::Io(format!(
                    "`vars.{key}` shadows a built-in variable; rename it (built-ins: {})",
                    RESERVED.join(", ")
                )));
            }
            table.insert(key.to_string(), Some(value));
        }
    }

    for (key, value) in std::env::vars() {
        if !RESERVED.contains(&key.as_str()) {
            table.insert(key, Some(value));
        }
    }

    // `None` marks a reserved name that exists but cannot be resolved here, so the error can say
    // *why* rather than "undefined variable" for something the docs promise is always available.
    table.insert(
        "CONFIG_DIR".to_string(),
        source.and_then(Path::parent).map(absolute).transpose()?,
    );
    table.insert(
        "PWD".to_string(),
        std::env::current_dir()
            .ok()
            .map(|dir| dir.to_string_lossy().into_owned()),
    );
    Ok(table)
}

/// The absolute form of `dir`, without touching the filesystem beyond reading the cwd.
///
/// A config named by a bare filename (`-c oxidant.yaml`) has an empty parent, and one named by a
/// relative path has a relative parent; `${CONFIG_DIR}` has to be absolute in both cases or it
/// would defeat the very rule it exists to serve.
fn absolute(dir: &Path) -> Result<String> {
    let joined = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| Error::Io(format!("resolve `${{CONFIG_DIR}}`: {e}")))?
            .join(dir)
    };
    Ok(crate::normalize(&joined).to_string_lossy().into_owned())
}

/// Recursively substitute into every string in `value`, keys included.
fn substitute(value: &mut Value, table: &BTreeMap<String, Option<String>>) -> Result<()> {
    match value {
        Value::String(text) => *text = expand(text, table)?,
        Value::Sequence(items) => {
            for item in items {
                substitute(item, table)?;
            }
        }
        Value::Mapping(mapping) => {
            let mut replacement = serde_norway::Mapping::with_capacity(mapping.len());
            for (key, item) in std::mem::take(mapping) {
                let mut key = key;
                let mut item = item;
                substitute(&mut key, table)?;
                substitute(&mut item, table)?;
                replacement.insert(key, item);
            }
            *mapping = replacement;
        }
        _ => {}
    }
    Ok(())
}

/// Expand every `${NAME}` in `text`. `$${` is the escape for a literal `${`.
///
/// An undefined name is an error naming it, never an empty string: a silently-empty `${DATA}`
/// turns `${DATA}/warehouse` into `/warehouse`, which is both absolute and completely wrong — it
/// would pass every check downstream and write to the root of the disk.
fn expand(text: &str, table: &BTreeMap<String, Option<String>>) -> Result<String> {
    if !text.contains('$') {
        return Ok(text.to_string());
    }
    let mut out = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'$' {
            let start = i;
            while i < bytes.len() && bytes[i] != b'$' {
                i += 1;
            }
            out.push_str(&text[start..i]);
            continue;
        }
        // `$${` — an escaped literal `${`.
        if text[i..].starts_with("$${") {
            out.push_str("${");
            i += 3;
            continue;
        }
        if !text[i..].starts_with("${") {
            // A lone `$` is ordinary text. Spark SQL is full of them (`get_json_object(v,
            // '$.id')`), and rewriting those would corrupt every JSON path in the file.
            out.push('$');
            i += 1;
            continue;
        }
        let Some(close) = text[i + 2..].find('}') else {
            return Err(Error::Io(format!(
                "unterminated `${{` in `{text}` — a literal `${{` is written `$${{`"
            )));
        };
        let name = &text[i + 2..i + 2 + close];
        out.push_str(&resolve(name, text, table)?);
        i += 2 + close + 1;
    }
    Ok(out)
}

/// Look one name up, with an error that says what to do about a miss.
fn resolve(name: &str, text: &str, table: &BTreeMap<String, Option<String>>) -> Result<String> {
    match table.get(name) {
        Some(Some(value)) => Ok(value.clone()),
        // A reserved name that could not be resolved in this context.
        Some(None) => Err(Error::Io(format!(
            "`${{{name}}}` in `{text}` is unavailable here — it is only defined when the config \
             is loaded from a file on disk"
        ))),
        None => Err(Error::Io(format!(
            "`${{{name}}}` in `{text}` is not defined. Set it under `vars:`, export it in the \
             environment, or use a built-in ({})",
            RESERVED.join(", ")
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(pairs: &[(&str, &str)]) -> BTreeMap<String, Option<String>> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), Some((*v).to_string())))
            .collect()
    }

    #[test]
    fn expands_references_and_leaves_bare_dollars_alone() {
        let vars = table(&[("DATA", "/srv/data")]);
        assert_eq!(
            expand("${DATA}/warehouse", &vars).unwrap(),
            "/srv/data/warehouse"
        );
        assert_eq!(
            expand("${DATA}/a/${DATA}/b", &vars).unwrap(),
            "/srv/data/a//srv/data/b"
        );
        // Spark SQL JSON paths are full of bare `$`, and rewriting one would corrupt the query.
        assert_eq!(
            expand("get_json_object(v, '$.order_id')", &vars).unwrap(),
            "get_json_object(v, '$.order_id')"
        );
        assert_eq!(expand("$${DATA}", &vars).unwrap(), "${DATA}");
        assert_eq!(expand("no references", &vars).unwrap(), "no references");
    }

    #[test]
    fn an_undefined_reference_is_an_error_not_an_empty_string() {
        // The failure this prevents: `${DATA}/warehouse` collapsing to `/warehouse`, which is
        // absolute, passes every downstream check, and writes to the root of the disk.
        let err = expand("${DATA}/warehouse", &table(&[])).expect_err("must not expand");
        assert!(err.to_string().contains("DATA"), "{err}");
        let err = expand("${UNCLOSED", &table(&[])).expect_err("must not expand");
        assert!(err.to_string().contains("unterminated"), "{err}");
    }

    #[test]
    fn the_environment_beats_vars_but_not_the_built_ins() {
        // Safe to set: prefixed so it cannot collide with a real variable in the test runner.
        std::env::set_var("OXIDANT_TEST_INTERP_DATA", "/from/env");
        let config: crate::OxidantConfig = interpolate_config(
            r#"
vars:
  OXIDANT_TEST_INTERP_DATA: /from/file
catalogs:
  local:
    type: local
    warehouse: ${OXIDANT_TEST_INTERP_DATA}/w
"#,
            None,
        )
        .expect("interpolates");
        assert_eq!(
            config.catalogs["local"].warehouse.as_deref(),
            Some("/from/env/w"),
            "the environment must override a checked-in default"
        );
        std::env::remove_var("OXIDANT_TEST_INTERP_DATA");
    }

    #[test]
    fn a_var_shadowing_a_built_in_is_rejected() {
        let err =
            interpolate_config::<crate::OxidantConfig>("vars:\n  CONFIG_DIR: /somewhere\n", None)
                .expect_err("CONFIG_DIR is reserved");
        assert!(err.to_string().contains("CONFIG_DIR"), "{err}");
    }

    #[test]
    fn config_dir_is_absolute_even_for_a_bare_filename() {
        // `-c oxidant.yaml` has an empty parent; `${CONFIG_DIR}` still has to be absolute or it
        // would defeat the rule it exists to serve.
        let config: crate::OxidantConfig = interpolate_config(
            "catalogs:\n  local:\n    type: local\n    warehouse: ${CONFIG_DIR}/w\n",
            Some(Path::new("oxidant.yaml")),
        )
        .expect("interpolates");
        let warehouse = config.catalogs["local"]
            .warehouse
            .clone()
            .expect("warehouse");
        assert!(
            Path::new(&warehouse).is_absolute(),
            "not absolute: {warehouse}"
        );
        assert!(warehouse.ends_with("/w"), "{warehouse}");
    }

    #[test]
    fn config_dir_says_why_it_is_unavailable_without_a_file() {
        let err = interpolate_config::<crate::OxidantConfig>(
            "catalogs:\n  local:\n    type: local\n    warehouse: ${CONFIG_DIR}/w\n",
            None,
        )
        .expect_err("no file to take a directory from");
        assert!(
            err.to_string().contains("loaded from a file"),
            "the error should explain, not just say undefined: {err}"
        );
    }
}
