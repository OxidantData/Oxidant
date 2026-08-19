//! Parse Spark Declarative Pipelines SQL source files into pipeline element definitions.
//!
//! Pure parsing — no engine execution. Output types are the seam for Connect wiring (Phase 1A)
//! and graph→config conversion (Phase 2).

use std::collections::BTreeMap;

use datafusion::sql::sqlparser::ast::{CreateView, Statement};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use oxidant_common::{Error, Result};

/// Parsed contents of a `DefineSqlGraphElements.sql_text` payload.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqlGraphElements {
    pub outputs: Vec<ParsedOutput>,
    pub flows: Vec<ParsedFlow>,
    pub refreshes: Vec<String>,
}

/// Kind of pipeline output declared by CREATE STREAMING TABLE / MATERIALIZED VIEW / TEMPORARY VIEW.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputKind {
    Table,
    MaterializedView,
    TemporaryView,
}

/// A declared pipeline output (table, materialized view, or temporary view).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedOutput {
    pub name: String,
    pub kind: OutputKind,
    pub comment: Option<String>,
    pub partition_cols: Vec<String>,
    pub table_properties: BTreeMap<String, String>,
    pub format: Option<String>,
    /// Parenthesized column DDL when present, e.g. `(id INT, name STRING)`.
    pub schema: Option<String>,
    pub if_not_exists: bool,
    pub or_refresh: bool,
}

/// A flow that appends query results into a target output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedFlow {
    pub name: Option<String>,
    pub target: String,
    pub query_sql: String,
    pub once: bool,
    pub by_name: bool,
}

/// Parse multi-statement SDP SQL text into outputs, flows, and refresh requests.
pub fn parse(sql_text: &str, sql_file_path: Option<&str>) -> Result<SqlGraphElements> {
    parse_with_context(sql_text, sql_file_path)
}

enum ParsedStatement {
    Output(ParsedOutput),
    Flow(ParsedFlow),
    Refresh(String),
    OutputAndFlow {
        output: ParsedOutput,
        flow: ParsedFlow,
    },
}

fn parse_one_statement(stmt: &str) -> Result<ParsedStatement> {
    let stmt = strip_leading_comments(stmt.trim());
    if stmt.is_empty() {
        return Err(unsupported_kind(stmt, "empty statement"));
    }
    let upper = stmt.to_ascii_uppercase();
    if starts_with_keyword(&upper, "APPLY CHANGES INTO") {
        return Err(unsupported_kind(stmt, "AUTO CDC / APPLY CHANGES INTO"));
    }
    if contains_phrase_outside_literals(stmt, "AUTO CDC") {
        return Err(unsupported_kind(stmt, "AUTO CDC / APPLY CHANGES INTO"));
    }
    if starts_with_keyword(&upper, "CREATE SINK") {
        return Err(unsupported_kind(stmt, "CREATE SINK"));
    }
    if starts_with_keyword(&upper, "CREATE OR REFRESH STREAMING TABLE")
        || starts_with_keyword(&upper, "CREATE STREAMING TABLE")
    {
        return parse_create_streaming_table(stmt);
    }
    if starts_with_keyword(&upper, "CREATE OR REFRESH MATERIALIZED VIEW")
        || starts_with_keyword(&upper, "CREATE MATERIALIZED VIEW")
    {
        return parse_create_materialized_view(stmt);
    }
    if starts_with_keyword(&upper, "CREATE TEMPORARY VIEW") {
        return parse_create_temporary_view(stmt);
    }
    if starts_with_keyword(&upper, "CREATE ONCE FLOW") || starts_with_keyword(&upper, "CREATE FLOW")
    {
        return parse_create_flow(stmt);
    }
    if starts_with_keyword(&upper, "REFRESH MATERIALIZED VIEW") {
        return parse_refresh_materialized_view(stmt);
    }
    if starts_with_keyword(&upper, "SELECT")
        || starts_with_keyword(&upper, "INSERT")
        || starts_with_keyword(&upper, "WITH")
    {
        return Err(unsupported_kind(stmt, "SELECT / INSERT"));
    }
    Err(unsupported_kind(stmt, "unsupported statement"))
}

fn parse_create_streaming_table(stmt: &str) -> Result<ParsedStatement> {
    let mut cursor = Cursor::new(stmt);
    cursor.expect_keyword("CREATE")?;
    let or_refresh = match cursor.try_keyword("OR")? {
        true => {
            cursor.expect_keyword("REFRESH")?;
            true
        }
        false => false,
    };
    cursor.expect_keyword("STREAMING")?;
    cursor.expect_keyword("TABLE")?;
    let if_not_exists = match cursor.try_keyword("IF")? {
        true => {
            cursor.expect_keyword("NOT")?;
            cursor.expect_keyword("EXISTS")?;
            true
        }
        false => false,
    };
    let name = cursor.parse_identifier()?;
    let mut output = ParsedOutput {
        name: name.clone(),
        kind: OutputKind::Table,
        comment: None,
        partition_cols: Vec::new(),
        table_properties: BTreeMap::new(),
        format: None,
        schema: None,
        if_not_exists,
        or_refresh,
    };
    parse_table_clauses(&mut cursor, &mut output)?;
    if cursor.try_keyword("AS")? {
        let query = cursor.rest().trim().to_string();
        validate_query(&query)?;
        let flow = ParsedFlow {
            name: None,
            target: name,
            query_sql: query,
            once: false,
            by_name: false,
        };
        return Ok(ParsedStatement::OutputAndFlow { output, flow });
    }
    if cursor.try_keyword("FLOW")? {
        return Err(unsupported_kind(
            stmt,
            "inline FLOW clause on CREATE STREAMING TABLE",
        ));
    }
    if !cursor.rest().trim().is_empty() {
        return Err(plan_error(
            stmt,
            "unexpected tokens after CREATE STREAMING TABLE",
        ));
    }
    Ok(ParsedStatement::Output(output))
}

fn parse_create_materialized_view(stmt: &str) -> Result<ParsedStatement> {
    if let Ok(parsed) = try_parse_create_view(stmt) {
        if parsed.materialized {
            let name = object_name_to_string(&parsed.name);
            let comment = parsed.comment.clone();
            let output = ParsedOutput {
                name: name.clone(),
                kind: OutputKind::MaterializedView,
                comment,
                partition_cols: Vec::new(),
                table_properties: BTreeMap::new(),
                format: None,
                schema: None,
                if_not_exists: parsed.if_not_exists,
                or_refresh: parsed.or_replace,
            };
            let query = query_to_sql(&parsed.query)?;
            validate_query(&query)?;
            let flow = ParsedFlow {
                name: None,
                target: name,
                query_sql: query,
                once: false,
                by_name: false,
            };
            return Ok(ParsedStatement::OutputAndFlow { output, flow });
        }
    }
    hand_parse_materialized_view(stmt)
}

fn hand_parse_materialized_view(stmt: &str) -> Result<ParsedStatement> {
    let mut cursor = Cursor::new(stmt);
    cursor.expect_keyword("CREATE")?;
    let or_refresh = match cursor.try_keyword("OR")? {
        true => {
            cursor.expect_keyword("REFRESH")?;
            true
        }
        false => false,
    };
    cursor.expect_keyword("MATERIALIZED")?;
    cursor.expect_keyword("VIEW")?;
    let if_not_exists = match cursor.try_keyword("IF")? {
        true => {
            cursor.expect_keyword("NOT")?;
            cursor.expect_keyword("EXISTS")?;
            true
        }
        false => false,
    };
    let name = cursor.parse_identifier()?;
    let mut output = ParsedOutput {
        name: name.clone(),
        kind: OutputKind::MaterializedView,
        comment: None,
        partition_cols: Vec::new(),
        table_properties: BTreeMap::new(),
        format: None,
        schema: None,
        if_not_exists,
        or_refresh,
    };
    parse_view_clauses_before_as(&mut cursor, &mut output, stmt)?;
    cursor.expect_keyword("AS")?;
    let query = cursor.rest().trim().to_string();
    validate_query(&query)?;
    let flow = ParsedFlow {
        name: None,
        target: name,
        query_sql: query,
        once: false,
        by_name: false,
    };
    Ok(ParsedStatement::OutputAndFlow { output, flow })
}

fn parse_create_temporary_view(stmt: &str) -> Result<ParsedStatement> {
    if let Ok(parsed) = try_parse_create_view(stmt) {
        if parsed.temporary {
            let name = object_name_to_string(&parsed.name);
            let output = ParsedOutput {
                name: name.clone(),
                kind: OutputKind::TemporaryView,
                comment: parsed.comment.clone(),
                partition_cols: Vec::new(),
                table_properties: BTreeMap::new(),
                format: None,
                schema: None,
                if_not_exists: false,
                or_refresh: false,
            };
            let query = query_to_sql(&parsed.query)?;
            validate_query(&query)?;
            let flow = ParsedFlow {
                name: None,
                target: name,
                query_sql: query,
                once: false,
                by_name: false,
            };
            return Ok(ParsedStatement::OutputAndFlow { output, flow });
        }
    }
    hand_parse_temporary_view(stmt)
}

fn hand_parse_temporary_view(stmt: &str) -> Result<ParsedStatement> {
    let mut cursor = Cursor::new(stmt);
    cursor.expect_keyword("CREATE")?;
    cursor.expect_keyword("TEMPORARY")?;
    cursor.expect_keyword("VIEW")?;
    let name = cursor.parse_identifier()?;
    cursor.expect_keyword("AS")?;
    let query = cursor.rest().trim().to_string();
    validate_query(&query)?;
    let output = ParsedOutput {
        name: name.clone(),
        kind: OutputKind::TemporaryView,
        comment: None,
        partition_cols: Vec::new(),
        table_properties: BTreeMap::new(),
        format: None,
        schema: None,
        if_not_exists: false,
        or_refresh: false,
    };
    let flow = ParsedFlow {
        name: None,
        target: name,
        query_sql: query,
        once: false,
        by_name: false,
    };
    Ok(ParsedStatement::OutputAndFlow { output, flow })
}

fn parse_create_flow(stmt: &str) -> Result<ParsedStatement> {
    let mut cursor = Cursor::new(stmt);
    cursor.expect_keyword("CREATE")?;
    let header_once = cursor.try_keyword("ONCE")?;
    cursor.expect_keyword("FLOW")?;
    let name = Some(cursor.parse_identifier()?);
    cursor.expect_keyword("AS")?;
    cursor.expect_keyword("INSERT")?;
    let insert_once = cursor.try_keyword("ONCE")?;
    cursor.expect_keyword("INTO")?;
    let target = cursor.parse_identifier()?;
    let by_name = match cursor.try_keyword("BY")? {
        true => {
            cursor.expect_keyword("NAME")?;
            true
        }
        false => false,
    };
    let query = cursor.rest().trim().to_string();
    validate_query(&query)?;
    Ok(ParsedStatement::Flow(ParsedFlow {
        name,
        target,
        query_sql: query,
        once: header_once || insert_once,
        by_name,
    }))
}

fn parse_refresh_materialized_view(stmt: &str) -> Result<ParsedStatement> {
    let mut cursor = Cursor::new(stmt);
    cursor.expect_keyword("REFRESH")?;
    cursor.expect_keyword("MATERIALIZED")?;
    cursor.expect_keyword("VIEW")?;
    let name = cursor.parse_identifier()?;
    if !cursor.rest().trim().is_empty() {
        return Err(plan_error(
            stmt,
            "unexpected tokens after REFRESH MATERIALIZED VIEW",
        ));
    }
    Ok(ParsedStatement::Refresh(name))
}

fn parse_view_clauses_before_as(
    cursor: &mut Cursor<'_>,
    output: &mut ParsedOutput,
    stmt: &str,
) -> Result<()> {
    loop {
        let rest = cursor.rest();
        if rest.is_empty() {
            break;
        }
        if rest_starts_with_keyword(rest, "AS") {
            break;
        }
        if cursor.try_keyword("COMMENT")? {
            output.comment = Some(cursor.parse_string_literal()?);
            continue;
        }
        if cursor.try_keyword("PARTITIONED")? {
            cursor.expect_keyword("BY")?;
            cursor.expect_char('(')?;
            let cols = cursor.parse_identifier_list(')')?;
            cursor.expect_char(')')?;
            output.partition_cols = cols;
            continue;
        }
        if cursor.try_keyword("TBLPROPERTIES")? {
            output.table_properties = cursor.parse_tblproperties()?;
            continue;
        }
        if cursor.try_keyword("SCHEDULE")? {
            return Err(unsupported_kind(
                stmt,
                "SCHEDULE EVERY on CREATE MATERIALIZED VIEW",
            ));
        }
        return Err(plan_error(
            cursor.input,
            "unexpected clause in CREATE MATERIALIZED VIEW",
        ));
    }
    Ok(())
}

fn parse_table_clauses(cursor: &mut Cursor<'_>, output: &mut ParsedOutput) -> Result<()> {
    loop {
        let rest = cursor.rest();
        if rest.is_empty() {
            break;
        }
        if rest_starts_with_keyword(rest, "AS") || rest_starts_with_keyword(rest, "FLOW") {
            break;
        }
        if cursor.try_char('(')? {
            let inner = cursor.parse_balanced('(', ')')?;
            output.schema = Some(format!("({inner})"));
            continue;
        }
        if cursor.try_keyword("COMMENT")? {
            output.comment = Some(cursor.parse_string_literal()?);
            continue;
        }
        if cursor.try_keyword("PARTITIONED")? {
            cursor.expect_keyword("BY")?;
            cursor.expect_char('(')?;
            let cols = cursor.parse_identifier_list(')')?;
            cursor.expect_char(')')?;
            output.partition_cols = cols;
            continue;
        }
        if cursor.try_keyword("TBLPROPERTIES")? {
            output.table_properties = cursor.parse_tblproperties()?;
            continue;
        }
        if cursor.try_keyword("USING")? {
            output.format = Some(cursor.parse_identifier()?);
            continue;
        }
        return Err(plan_error(
            cursor.input,
            "unexpected clause in CREATE STREAMING TABLE",
        ));
    }
    Ok(())
}

fn try_parse_create_view(stmt: &str) -> Result<CreateView> {
    let stmts = Parser::parse_sql(&DatabricksDialect {}, stmt)
        .map_err(|e| plan_error(stmt, &format!("parse error: {e}")))?;
    let [Statement::CreateView(cv)] = stmts.as_slice() else {
        return Err(plan_error(stmt, "expected single CREATE VIEW statement"));
    };
    Ok(cv.clone())
}

fn validate_query(query: &str) -> Result<()> {
    let query = query.trim();
    if query.is_empty() {
        return Err(plan_error(query, "query must not be empty"));
    }
    match Parser::parse_sql(&DatabricksDialect {}, query) {
        Ok(stmts) if stmts.len() == 1 => match &stmts[0] {
            Statement::Query(_) => Ok(()),
            _ => Err(plan_error(
                query,
                "expected a query (SELECT, WITH, or VALUES)",
            )),
        },
        Ok(_) => Err(plan_error(query, "query must be a single statement")),
        Err(_) if has_dialect_extension(query) => Ok(()),
        Err(e) => Err(plan_error(query, &format!("invalid query SQL: {e}"))),
    }
}

fn has_dialect_extension(query: &str) -> bool {
    let sanitized = strip_strings_and_comments(query);
    let upper = sanitized.to_ascii_uppercase();
    word_boundary_contains(&upper, "STREAM") || word_boundary_contains(&upper, "READ_FILES")
}

fn query_to_sql(query: &datafusion::sql::sqlparser::ast::Query) -> Result<String> {
    Ok(query.to_string())
}

fn object_name_to_string(name: &datafusion::sql::sqlparser::ast::ObjectName) -> String {
    name.to_string()
        .trim_matches('`')
        .split('.')
        .map(|p| p.trim_matches('`'))
        .collect::<Vec<_>>()
        .join(".")
}

fn plan_error(_stmt: &str, msg: &str) -> Error {
    Error::Plan(msg.to_string())
}

fn unsupported_kind(stmt: &str, kind: &str) -> Error {
    Error::Unsupported(format!(
        "pipeline SQL does not support {kind}: {}",
        statement_preview(stmt)
    ))
}

fn statement_preview(s: &str) -> String {
    let lines: Vec<&str> = s.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
    if lines.is_empty() {
        return String::new();
    }
    if lines.len() == 1 {
        lines[0].to_string()
    } else {
        format!("{} ... {}", lines[0], lines.last().unwrap())
    }
}

fn starts_with_keyword(upper: &str, kw: &str) -> bool {
    let upper_kw = kw.to_ascii_uppercase();
    if !upper.starts_with(&upper_kw) {
        return false;
    }
    let after = upper[kw.len()..].chars().next();
    after.is_none() || !after.unwrap().is_ascii_alphanumeric() && after != Some('_')
}

fn rest_starts_with_keyword(rest: &str, kw: &str) -> bool {
    starts_with_keyword(&rest.to_ascii_uppercase(), kw)
}

fn strip_leading_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix("--") {
            s = match rest.find('\n') {
                Some(i) => &rest[i + 1..],
                None => "",
            };
            continue;
        }
        if let Some(rest) = s.strip_prefix("/*") {
            let mut chars = rest.char_indices().peekable();
            while let Some((i, ch)) = chars.next() {
                if ch == '*' && chars.peek().map(|(_, c)| *c) == Some('/') {
                    s = &rest[i + 2..];
                    break;
                }
            }
            if s.starts_with("/*") {
                return "";
            }
            continue;
        }
        break;
    }
    s
}

fn is_only_comments(s: &str) -> bool {
    strip_leading_comments(s.trim()).is_empty()
}

fn strip_strings_and_comments(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
                out.push(' ');
            }
            continue;
        }
        if in_block_comment {
            if ch == '*' && chars.peek() == Some(&'/') {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }
        if in_double && ch == '\\' {
            chars.next();
            continue;
        }
        if !in_single && !in_double && !in_backtick {
            if ch == '-' && chars.peek() == Some(&'-') {
                chars.next();
                in_line_comment = true;
                continue;
            }
            if ch == '/' && chars.peek() == Some(&'*') {
                chars.next();
                in_block_comment = true;
                continue;
            }
        }
        match ch {
            '\'' if !in_double && !in_backtick => {
                in_single = !in_single;
            }
            '"' if !in_single && !in_backtick => {
                in_double = !in_double;
            }
            '`' if !in_single && !in_double => {
                in_backtick = !in_backtick;
            }
            c if !in_single && !in_double && !in_backtick => out.push(c),
            _ => {}
        }
    }
    out
}

fn contains_phrase_outside_literals(stmt: &str, phrase: &str) -> bool {
    let sanitized = strip_strings_and_comments(stmt);
    let upper = sanitized.to_ascii_uppercase();
    word_boundary_contains(&upper, phrase)
}

fn word_boundary_contains(haystack: &str, needle: &str) -> bool {
    let needle_upper = needle.to_ascii_uppercase();
    let needle_bytes = needle_upper.as_bytes();
    let hay_bytes = haystack.as_bytes();
    if needle_bytes.is_empty() || hay_bytes.len() < needle_bytes.len() {
        return false;
    }
    for i in 0..=hay_bytes.len() - needle_bytes.len() {
        if !haystack[i..i + needle_bytes.len()].eq_ignore_ascii_case(needle) {
            continue;
        }
        let before_ok =
            i == 0 || !hay_bytes[i - 1].is_ascii_alphanumeric() && hay_bytes[i - 1] != b'_';
        let after_idx = i + needle_bytes.len();
        let after_ok = after_idx >= hay_bytes.len()
            || !hay_bytes[after_idx].is_ascii_alphanumeric() && hay_bytes[after_idx] != b'_';
        if before_ok && after_ok {
            return true;
        }
    }
    false
}

/// Split SQL on semicolons outside quotes and comments; returns `(start_line, statement)`.
pub fn split_statements(sql: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut start_line = 1usize;
    let mut line = 1usize;
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            cur.push(ch);
            if ch == '\n' {
                in_line_comment = false;
                line += 1;
            }
            continue;
        }
        if in_block_comment {
            cur.push(ch);
            if ch == '\n' {
                line += 1;
            } else if ch == '*' && chars.peek() == Some(&'/') {
                cur.push('/');
                chars.next();
                in_block_comment = false;
            }
            continue;
        }

        if !in_single && !in_double && !in_backtick {
            if ch == '-' && chars.peek() == Some(&'-') {
                in_line_comment = true;
                cur.push(ch);
                continue;
            }
            if ch == '/' && chars.peek() == Some(&'*') {
                in_block_comment = true;
                cur.push(ch);
                continue;
            }
        }

        if in_double && ch == '\\' {
            cur.push(ch);
            if let Some(next) = chars.next() {
                cur.push(next);
                if next == '\n' {
                    line += 1;
                }
            }
            continue;
        }
        if in_single && ch == '\\' {
            cur.push(ch);
            if let Some(next) = chars.next() {
                cur.push(next);
                if next == '\n' {
                    line += 1;
                }
            }
            continue;
        }

        match ch {
            '\'' if !in_double && !in_backtick => {
                in_single = !in_single;
                cur.push(ch);
            }
            '"' if !in_single && !in_backtick => {
                in_double = !in_double;
                cur.push(ch);
            }
            '`' if !in_single && !in_double => {
                in_backtick = !in_backtick;
                cur.push(ch);
            }
            ';' if !in_single && !in_double && !in_backtick => {
                let trimmed = cur.trim();
                if !trimmed.is_empty() {
                    out.push((start_line, trimmed.to_string()));
                }
                cur.clear();
            }
            '\n' => {
                cur.push(ch);
                line += 1;
                if cur.trim().is_empty() {
                    start_line = line;
                }
            }
            c => {
                if cur.is_empty() && !c.is_whitespace() {
                    start_line = line;
                }
                cur.push(c);
            }
        }
    }
    let trimmed = cur.trim();
    if !trimmed.is_empty() {
        out.push((start_line, trimmed.to_string()));
    }
    out
}

struct Cursor<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn rest(&self) -> &str {
        self.input[self.pos..].trim_start()
    }

    fn skip_ws(&mut self) {
        while self.pos < self.input.len() {
            let b = self.input.as_bytes()[self.pos];
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }
    fn try_keyword(&mut self, kw: &str) -> Result<bool> {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        let upper_rest = rest.to_ascii_uppercase();
        let upper_kw = kw.to_ascii_uppercase();
        if upper_rest.starts_with(&upper_kw) {
            let after = rest[kw.len()..].chars().next();
            if after.is_none() || !after.unwrap().is_ascii_alphanumeric() && after != Some('_') {
                self.pos += self.input[self.pos..].len() - rest.len() + kw.len();
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<()> {
        if self.try_keyword(kw)? {
            Ok(())
        } else {
            Err(plan_error(self.input, &format!("expected keyword {kw}")))
        }
    }

    fn try_char(&mut self, ch: char) -> Result<bool> {
        self.skip_ws();
        if self.input[self.pos..].starts_with(ch) {
            self.pos += ch.len_utf8();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn expect_char(&mut self, ch: char) -> Result<()> {
        if self.try_char(ch)? {
            Ok(())
        } else {
            Err(plan_error(self.input, &format!("expected '{ch}'")))
        }
    }

    fn parse_identifier(&mut self) -> Result<String> {
        let mut parts = Vec::new();
        loop {
            parts.push(self.parse_identifier_segment()?);
            self.skip_ws();
            if self.try_char('.')? {
                continue;
            }
            break;
        }
        Ok(parts.join("."))
    }

    fn parse_identifier_segment(&mut self) -> Result<String> {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        if let Some(rest) = rest.strip_prefix('`') {
            let end = rest
                .find('`')
                .ok_or_else(|| plan_error(self.input, "unterminated quoted identifier"))?;
            let id = rest[..end].to_string();
            self.pos += self.input[self.pos..].len() - rest.len() + 1 + end;
            return Ok(id);
        }
        let end = rest
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .unwrap_or(rest.len());
        if end == 0 {
            return Err(plan_error(self.input, "expected identifier"));
        }
        let id = rest[..end].to_string();
        self.pos += self.input[self.pos..].len() - rest.len() + end;
        Ok(id)
    }

    fn parse_string_literal(&mut self) -> Result<String> {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        let quote = rest
            .chars()
            .next()
            .ok_or_else(|| plan_error(self.input, "expected string literal"))?;
        if quote != '\'' && quote != '"' {
            return Err(plan_error(self.input, "expected string literal"));
        }
        let mut out = String::new();
        let mut iter = rest[1..].char_indices();
        while let Some((i, ch)) = iter.next() {
            if ch == quote {
                self.pos += 1 + i + 1;
                return Ok(out);
            }
            if ch == '\'' && quote == '\'' && rest[1 + i + 1..].starts_with('\'') {
                out.push('\'');
                iter.next();
                continue;
            }
            out.push(ch);
        }
        Err(plan_error(self.input, "unterminated string literal"))
    }

    fn parse_balanced(&mut self, open: char, close: char) -> Result<String> {
        let mut depth = 1usize;
        let start = self.pos;
        let mut in_single = false;
        let mut in_double = false;
        let mut in_backtick = false;
        while self.pos < self.input.len() {
            let ch = self.input[self.pos..].chars().next().unwrap();
            self.pos += ch.len_utf8();
            match ch {
                '\'' if !in_double && !in_backtick => in_single = !in_single,
                '"' if !in_single && !in_backtick => in_double = !in_double,
                '`' if !in_single && !in_double => in_backtick = !in_backtick,
                c if !in_single && !in_double && !in_backtick => {
                    if c == open {
                        depth += 1;
                    } else if c == close {
                        depth -= 1;
                        if depth == 0 {
                            let end = self.pos - ch.len_utf8();
                            return Ok(self.input[start..end].trim().to_string());
                        }
                    }
                }
                _ => {}
            }
        }
        Err(plan_error(self.input, "unbalanced parentheses"))
    }

    fn parse_identifier_list(&mut self, end_char: char) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        loop {
            self.skip_ws();
            if self.input[self.pos..].starts_with(end_char) {
                break;
            }
            ids.push(self.parse_identifier()?);
            self.skip_ws();
            if self.input[self.pos..].starts_with(',') {
                self.pos += 1;
                continue;
            }
            if self.input[self.pos..].starts_with(end_char) {
                break;
            }
            return Err(plan_error(
                self.input,
                "expected ',' or ')' in identifier list",
            ));
        }
        Ok(ids)
    }

    fn parse_tblproperties(&mut self) -> Result<BTreeMap<String, String>> {
        self.expect_char('(')?;
        let mut props = BTreeMap::new();
        loop {
            self.skip_ws();
            if self.input[self.pos..].starts_with(')') {
                self.pos += 1;
                break;
            }
            let key = self.parse_string_literal()?;
            self.skip_ws();
            if !self.input[self.pos..].starts_with('=') {
                return Err(plan_error(self.input, "expected '=' in TBLPROPERTIES"));
            }
            self.pos += 1;
            let value = self.parse_string_literal()?;
            props.insert(key, value);
            self.skip_ws();
            if self.input[self.pos..].starts_with(',') {
                self.pos += 1;
                continue;
            }
            if self.input[self.pos..].starts_with(')') {
                self.pos += 1;
                break;
            }
        }
        Ok(props)
    }
}

/// Context-aware parse with file path and statement index in error messages.
pub fn parse_with_context(sql_text: &str, sql_file_path: Option<&str>) -> Result<SqlGraphElements> {
    let mut elements = SqlGraphElements::default();
    for (index, (line, stmt)) in split_statements(sql_text).into_iter().enumerate() {
        let trimmed = stmt.trim();
        if trimmed.is_empty() || is_only_comments(trimmed) {
            continue;
        }
        let parsed = parse_one_statement(trimmed)
            .map_err(|e| contextualize_error(e, sql_file_path, index + 1, line))?;
        parsed.apply_to(&mut elements);
    }
    Ok(elements)
}

impl ParsedStatement {
    fn apply_to(self, elements: &mut SqlGraphElements) {
        match self {
            ParsedStatement::Output(output) => elements.outputs.push(output),
            ParsedStatement::Flow(flow) => elements.flows.push(flow),
            ParsedStatement::Refresh(name) => elements.refreshes.push(name),
            ParsedStatement::OutputAndFlow { output, flow } => {
                elements.outputs.push(output);
                elements.flows.push(flow);
            }
        }
    }
}

fn contextualize_error(err: Error, path: Option<&str>, stmt_no: usize, line: usize) -> Error {
    let detail = match &err {
        Error::Plan(m) | Error::Unsupported(m) | Error::Execution(m) | Error::Io(m) => m.clone(),
    };
    let location = match path {
        Some(p) => format!("file {p}, statement {stmt_no} (line {line})"),
        None => format!("statement {stmt_no} (line {line})"),
    };
    match err {
        Error::Unsupported(_) => Error::Unsupported(format!("{location}: {detail}")),
        Error::Plan(_) => Error::Plan(format!("{location}: {detail}")),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_file(sql: &str, path: &str) -> Result<SqlGraphElements> {
        parse_with_context(sql, Some(path))
    }

    #[test]
    fn streaming_table_all_clauses_with_query() {
        let sql = "\
CREATE OR REFRESH STREAMING TABLE IF NOT EXISTS bronze.events \
(id INT, ts TIMESTAMP) \
COMMENT 'bronze events' \
PARTITIONED BY (id) \
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true') \
USING DELTA \
AS SELECT * FROM STREAM raw.events";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.flows.len(), 1);
        let o = &out.outputs[0];
        assert_eq!(o.name, "bronze.events");
        assert_eq!(o.kind, OutputKind::Table);
        assert_eq!(o.comment.as_deref(), Some("bronze events"));
        assert_eq!(o.partition_cols, vec!["id"]);
        assert_eq!(
            o.table_properties.get("delta.autoOptimize.optimizeWrite"),
            Some(&"true".to_string())
        );
        assert_eq!(o.format.as_deref(), Some("DELTA"));
        assert_eq!(o.schema.as_deref(), Some("(id INT, ts TIMESTAMP)"));
        assert!(o.if_not_exists);
        assert!(o.or_refresh);
        let f = &out.flows[0];
        assert_eq!(f.target, "bronze.events");
        assert_eq!(f.query_sql, "SELECT * FROM STREAM raw.events");
        assert!(!f.once);
        assert!(!f.by_name);
    }

    #[test]
    fn streaming_table_without_query() {
        let sql = "CREATE STREAMING TABLE raw_ingest (id INT)";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert!(out.flows.is_empty());
        assert_eq!(out.outputs[0].name, "raw_ingest");
        assert_eq!(out.outputs[0].schema.as_deref(), Some("(id INT)"));
    }

    #[test]
    fn materialized_view_with_comment() {
        let sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS gold.summary COMMENT 'daily' AS SELECT count(*) AS c FROM bronze.events";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.flows.len(), 1);
        assert_eq!(out.outputs[0].kind, OutputKind::MaterializedView);
        assert!(out.outputs[0].if_not_exists);
        assert_eq!(out.outputs[0].comment.as_deref(), Some("daily"));
    }

    #[test]
    fn or_refresh_materialized_view() {
        let sql = "CREATE OR REFRESH MATERIALIZED VIEW mv AS SELECT 1";
        let out = parse(sql, None).unwrap();
        assert!(out.outputs[0].or_refresh);
    }

    #[test]
    fn temporary_view() {
        let sql = "CREATE TEMPORARY VIEW staging AS SELECT 1 AS x";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs[0].kind, OutputKind::TemporaryView);
        assert_eq!(out.flows[0].query_sql, "SELECT 1 AS x");
    }

    #[test]
    fn create_flow_by_name() {
        let sql =
            "CREATE FLOW west AS INSERT INTO customers BY NAME SELECT * FROM STREAM(raw.west)";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.flows.len(), 1);
        let f = &out.flows[0];
        assert_eq!(f.name.as_deref(), Some("west"));
        assert_eq!(f.target, "customers");
        assert!(f.by_name);
        assert!(!f.once);
    }

    #[test]
    fn create_once_flow() {
        let sql = "CREATE ONCE FLOW backfill AS INSERT INTO t SELECT 1";
        let out = parse(sql, None).unwrap();
        assert!(out.flows[0].once);
    }

    #[test]
    fn create_flow_insert_once() {
        let sql = "CREATE FLOW bf AS INSERT ONCE INTO t SELECT 1";
        let out = parse(sql, None).unwrap();
        assert!(out.flows[0].once);
    }

    #[test]
    fn refresh_materialized_view() {
        let sql = "REFRESH MATERIALIZED VIEW mv";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.refreshes, vec!["mv"]);
    }

    #[test]
    fn streaming_table_with_spool_properties_plus_mv() {
        let sql = "CREATE OR REFRESH STREAMING TABLE orders_bronze \
            TBLPROPERTIES ('subscribe' = 'orders', 'oxidant.spool.dir' = '/tmp/spool', 'startingOffsets' = 'earliest') \
            USING DELTA AS SELECT 1 AS order_id FROM stream; \
            CREATE MATERIALIZED VIEW revenue_gold AS SELECT sum(amount) FROM orders_bronze";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 2, "{out:?}");
        assert_eq!(out.flows.len(), 2, "{out:?}");
    }

    #[test]
    fn multi_statement_file() {
        let sql = "\
CREATE STREAMING TABLE t AS SELECT 1;
CREATE FLOW f AS INSERT INTO t SELECT 2;
REFRESH MATERIALIZED VIEW mv;
CREATE MATERIALIZED VIEW mv AS SELECT 3";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 2);
        assert_eq!(out.flows.len(), 3);
        assert_eq!(out.refreshes, vec!["mv"]);
    }

    #[test]
    fn rejects_auto_cdc() {
        let err = parse(
            "CREATE FLOW cdc AS AUTO CDC INTO t FROM stream(s) KEYS (id)",
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("AUTO CDC"));
    }

    #[test]
    fn rejects_apply_changes() {
        let err = parse("APPLY CHANGES INTO t FROM stream(s)", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("APPLY CHANGES INTO"));
    }

    #[test]
    fn rejects_create_sink() {
        let err = parse("CREATE SINK s AS SELECT 1", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("CREATE SINK"));
    }

    #[test]
    fn rejects_select() {
        let err = parse("SELECT 1", None).unwrap_err().to_string();
        assert!(err.contains("SELECT"));
    }

    #[test]
    fn rejection_includes_file_path() {
        let err = parse_file("SELECT 1", "pipelines/demo.sql")
            .unwrap_err()
            .to_string();
        assert!(err.contains("pipelines/demo.sql"));
        assert!(err.contains("SELECT"));
    }

    #[test]
    fn split_respects_semicolons_in_strings() {
        let parts: Vec<_> = split_statements("SELECT ';'; SELECT 2")
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        assert_eq!(parts, vec!["SELECT ';'", "SELECT 2"]);
    }

    #[test]
    fn split_respects_semicolons_in_comments() {
        let sql = "CREATE STREAMING TABLE t AS SELECT 1; -- ; comment\nSELECT 2";
        let parts: Vec<_> = split_statements(sql).into_iter().map(|(_, s)| s).collect();
        assert_eq!(parts.len(), 2);
        assert!(parts[0].starts_with("CREATE STREAMING"));
        assert_eq!(parts[1], "-- ; comment\nSELECT 2");
    }

    #[test]
    fn split_respects_block_comments() {
        let sql =
            "CREATE STREAMING TABLE t AS SELECT 1; /* ; */ CREATE FLOW f AS INSERT INTO t SELECT 2";
        let parts: Vec<_> = split_statements(sql).into_iter().map(|(_, s)| s).collect();
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn malformed_streaming_table_errors() {
        let err = parse("CREATE STREAMING TABLE", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("expected identifier") || err.contains("plan error"));
    }

    #[test]
    fn leading_line_comment_then_ddl_parses() {
        let sql = "-- header\nCREATE STREAMING TABLE t AS SELECT 1";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.flows.len(), 1);
        assert_eq!(out.outputs[0].name, "t");
    }

    #[test]
    fn leading_line_comment_select_is_rejected() {
        let err = parse("-- hi\nSELECT 1", None).unwrap_err().to_string();
        assert!(err.contains("SELECT"));
    }

    #[test]
    fn auto_cdc_in_string_literal_is_accepted() {
        let sql = "CREATE STREAMING TABLE t AS SELECT 'AUTO CDC' AS x";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.flows.len(), 1);
    }

    #[test]
    fn auto_cdc_in_comment_clause_is_accepted() {
        let sql = "CREATE STREAMING TABLE t COMMENT 'AUTO CDC report' AS SELECT 1";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.outputs[0].comment.as_deref(), Some("AUTO CDC report"));
    }

    #[test]
    fn apply_changes_in_string_literal_is_accepted() {
        let sql = "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE x = 'APPLY CHANGES INTO y'";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.flows.len(), 1);
    }

    #[test]
    fn multiline_as_without_trailing_space() {
        let sql = "CREATE STREAMING TABLE\nt\nAS\nSELECT 1";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.flows.len(), 1);
        assert_eq!(out.flows[0].query_sql, "SELECT 1");
    }

    #[test]
    fn block_comment_prefix_parses() {
        let sql = "/* head */ CREATE STREAMING TABLE t AS SELECT 1";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.flows.len(), 1);
    }

    #[test]
    fn trailing_junk_after_query_is_rejected() {
        let err = parse(
            "CREATE STREAMING TABLE t AS SELECT 1 garbage tokens here",
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("invalid query SQL") || err.contains("plan error"));
    }

    #[test]
    fn materialized_view_partitioned_by_and_tblproperties() {
        let sql =
            "CREATE MATERIALIZED VIEW mv PARTITIONED BY (dt) TBLPROPERTIES ('k' = 'v') AS SELECT 1";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.outputs[0].partition_cols, vec!["dt"]);
        assert_eq!(
            out.outputs[0].table_properties.get("k"),
            Some(&"v".to_string())
        );
    }

    #[test]
    fn materialized_view_schedule_every_is_rejected() {
        let err = parse(
            "CREATE MATERIALIZED VIEW mv SCHEDULE EVERY 1 HOUR AS SELECT 1",
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("SCHEDULE EVERY"));
    }

    #[test]
    fn chained_backtick_identifiers() {
        let sql = "CREATE STREAMING TABLE `cat`.`schema`.`tab` AS SELECT 1";
        let out = parse(sql, None).unwrap();
        assert_eq!(out.outputs[0].name, "cat.schema.tab");
    }

    #[test]
    fn split_respects_backslash_escape_in_double_quotes() {
        let parts: Vec<_> = split_statements(r#"SELECT "a\"; SELECT 2"#)
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        assert_eq!(parts, vec![r#"SELECT "a\"; SELECT 2"#]);
    }

    #[test]
    fn split_respects_backslash_escape_in_single_quotes() {
        let parts: Vec<_> = split_statements(r"SELECT 'a\'; SELECT 2")
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        assert_eq!(parts, vec![r"SELECT 'a\'; SELECT 2"]);
    }

    #[test]
    fn contextualized_error_includes_statement_number_and_line() {
        let sql = "CREATE STREAMING TABLE t AS SELECT 1;\n-- c\nSELECT 2";
        let err = parse_with_context(sql, Some("pipelines/demo.sql"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("pipelines/demo.sql"), "err: {err}");
        assert!(err.contains("statement 2"), "err: {err}");
        assert!(err.contains("line 2"), "err: {err}");
        assert!(err.contains("SELECT"), "err: {err}");
    }

    #[test]
    fn unsupported_error_includes_multiline_preview() {
        let sql = "CREATE STREAMING TABLE t AS SELECT 1;\nFOO\nBAR";
        let err = parse(sql, None).unwrap_err().to_string();
        assert!(err.contains("FOO ... BAR") || err.contains("unsupported statement"));
    }
}
