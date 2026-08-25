//! The catalog rail's reserved-word list, checked against the parser it exists for.
//!
//! `crates/oxidant-ui-server/src/catalog_rail.js` backticks a name that would not parse bare,
//! and its list is deliberately *not* the whole grammar: a false positive there is what every
//! name the rail inserts looks like, so `default`, `schema` and `comment` — which Spark does
//! not reserve — have to stay bare. A **missing** word is the opposite kind of mistake and a
//! silent one: the rail lists a schema called `except`, the user clicks it, and the statement
//! it built is a syntax error about a set operator.
//!
//! A hand-maintained list drifts against the parser with nothing to say so, so this test asks
//! the parser. It lives here because this crate owns "what the Databricks dialect reserves"
//! (`dialect/intercept.rs` parses with the same dialect the engine does) and reads the rail's
//! source from the workspace — like `oxidant-ui-server`'s own connector-event test, a missing
//! sibling crate skips rather than fails, so either crate still builds packaged on its own.

use std::collections::BTreeSet;

use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::{
    ALL_KEYWORDS, RESERVED_FOR_COLUMN_ALIAS, RESERVED_FOR_IDENTIFIER, RESERVED_FOR_TABLE_ALIAS,
};
use datafusion::sql::sqlparser::parser::Parser;

/// The rail's `RESERVED`, read out of the shipped JavaScript.
///
/// The list is written as `('a b c ' + 'd e f').split(' ')` — one string per source line, so
/// the file stays inside its margin — so the quoted chunks are concatenated back together.
fn rail_reserved(source: &str) -> BTreeSet<String> {
    let (_, rest) = source
        .split_once("var RESERVED = (")
        .expect("`RESERVED` is gone from catalog_rail.js; nothing is quoted for being a keyword");
    let (list, _) = rest
        .split_once(").split(' ');")
        .expect("`RESERVED` is no longer a split string literal");
    let mut words = BTreeSet::new();
    let mut rest = list;
    while let Some((_, after)) = rest.split_once('\'') {
        let (chunk, tail) = after
            .split_once('\'')
            .expect("unterminated string in `RESERVED`");
        words.extend(chunk.split_whitespace().map(str::to_string));
        rest = tail;
    }
    assert!(!words.is_empty(), "`RESERVED` parsed as empty");
    words
}

/// Does `sql` parse, *and* does the name survive into the statement it parsed as?
///
/// Both halves matter. A word that fails to parse is a visible syntax error; a word that
/// parses with the name swallowed by a clause — `FROM cat.ns.limit` read as a `LIMIT` — is the
/// same bug wearing a wrong answer instead of a message.
fn parses_as_written(sql: &str, name: &str) -> bool {
    match Parser::parse_sql(&DatabricksDialect {}, sql) {
        Ok(stmts) => stmts.len() == 1 && stmts[0].to_string().contains(name),
        Err(_) => false,
    }
}

/// Every position the rail can put a name in: a catalog, schema or table lands in a `FROM`
/// (qualified when the tree inserted it, bare when a one-part suggestion did, and with the
/// `LIMIT` that `previewSql` appends), and a column lands in a select list.
fn bare_is_wrong(word: &str) -> bool {
    let qualified = format!("cat.ns.{word}");
    !parses_as_written(&format!("SELECT * FROM {word}"), word)
        || !parses_as_written(&format!("SELECT * FROM {qualified}"), &qualified)
        || !parses_as_written(&format!("SELECT * FROM {qualified} LIMIT 100"), &qualified)
        || !parses_as_written(
            &format!("SELECT * FROM {word}.ns.t"),
            &format!("{word}.ns.t"),
        )
        || !parses_as_written(
            &format!("SELECT * FROM cat.{word}.t"),
            &format!("cat.{word}.t"),
        )
        || !parses_as_written(&format!("SELECT {word} FROM t"), word)
        || !parses_as_written(&format!("SELECT a, {word} FROM t"), word)
}

#[test]
fn every_word_the_parser_rejects_bare_is_one_the_rail_quotes() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../oxidant-ui-server/src/catalog_rail.js");
    let Ok(source) = std::fs::read_to_string(&path) else {
        eprintln!("skipping: {} is not in this checkout", path.display());
        return;
    };
    let reserved = rail_reserved(&source);

    // **The dialect's own reserved sets.** These are exactly the words that cannot appear
    // where the rail puts a name: the table-factor and alias slots of a `FROM`, the select
    // list, and the identifier positions inside them. The rail inserts at a cursor in a query
    // it does not control, so the alias slot counts — `FROM orders except` is a set operator
    // with nothing on its right, not a table called `except`.
    let missing: Vec<String> = RESERVED_FOR_TABLE_ALIAS
        .iter()
        .chain(RESERVED_FOR_COLUMN_ALIAS)
        .chain(RESERVED_FOR_IDENTIFIER)
        .map(|kw| format!("{kw:?}").to_ascii_lowercase())
        .filter(|kw| !reserved.contains(kw))
        .collect();
    assert!(
        missing.is_empty(),
        "the Databricks dialect reserves these and the catalog rail inserts them bare — a \
         click on a name like this builds a statement that means something else. Add them to \
         `RESERVED` in crates/oxidant-ui-server/src/catalog_rail.js: {missing:?}"
    );

    // …and the belt to that pair of braces: every other keyword the parser knows, tried in the
    // shapes the rail actually builds. A word the lists do not cover but the parser still
    // rejects is the case a transcription would miss entirely.
    let rejected: Vec<&str> = ALL_KEYWORDS
        .iter()
        .copied()
        .filter(|kw| kw.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .filter(|kw| !reserved.contains(&kw.to_ascii_lowercase()))
        .filter(|kw| bare_is_wrong(&kw.to_ascii_lowercase()))
        .collect();
    assert!(
        rejected.is_empty(),
        "the parser will not take these bare in a `FROM` or a select list, and the rail \
         inserts them bare: {rejected:?}"
    );

    // The named hazards from the review, spelled out so a regression reads as itself rather
    // than as one word missing from a list of ninety.
    for word in [
        "except",
        "intersect",
        "minus",
        "anti",
        "semi",
        "natural",
        "lateral",
        "window",
    ] {
        assert!(
            reserved.contains(word),
            "`{word}` is a set operator or a join modifier; a schema called that inserts as \
             something other than a name"
        );
    }

    // And the other direction, which is what keeps the list from becoming the grammar: a word
    // the parser leaves alone must stay bare, or the rail spends its life backticking the
    // standard namespace.
    for word in ["default", "schema", "column", "comment", "date", "orders"] {
        assert!(
            !reserved.contains(word),
            "`{word}` is not reserved; quoting it puts backticks on nearly every name the rail \
             ever inserts"
        );
        assert!(!bare_is_wrong(word), "`{word}` no longer parses bare");
    }
}
