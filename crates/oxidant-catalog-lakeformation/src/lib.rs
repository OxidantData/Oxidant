//! An AWS Lake Formation authorization client.
//!
//! Resolves Lake Formation's *effective permissions* for Glue tables with the official AWS SDK
//! for Rust (`aws-sdk-lakeformation`) in-process. Credentials resolve through the standard AWS
//! chain (`aws-config`: environment variables, shared config/credentials files, container
//! credentials, EC2 instance role / IRSA). Two retrieval paths:
//!
//! - [`LakeFormationAuth::effective_permissions`] → `ListPermissions` (paginated), filtered to a
//!   single table resource, optionally narrowed to one principal. Each grant maps to a
//!   [`PrincipalTablePermissions`] — including column-level grants (`TableWithColumns` resources
//!   surface as [`ColumnSelection::Named`] / [`ColumnSelection::AllExcept`]).
//! - [`LakeFormationAuth::data_cells_filters`] → `ListDataCellsFilter` (paginated) for a table,
//!   mapping each filter to a [`TableDataFilter`] with its column selection and row-filter
//!   expression — the input scan planning (KAN-103) rewrites into DataFusion predicates/projections.
//!
//! This crate only *retrieves* authorization information; it grants/revokes nothing and never
//! shells out to the `aws` CLI.

use std::collections::HashMap;

use aws_sdk_lakeformation::error::ProvideErrorMetadata;
use aws_sdk_lakeformation::types::{
    DataCellsFilter, DataLakePrincipal, DataLakeResourceType, PrincipalResourcePermissions,
    Resource, TableResource,
};
use oxidant_common::{Error, Result};

/// Which columns of a table a grant or data-cell filter covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnSelection {
    /// The whole table (a plain table-level grant, or a filter with no column restriction).
    All,
    /// An explicit allow-list of columns.
    Named(Vec<String>),
    /// All columns except the named ones (Lake Formation `ColumnWildcard.ExcludedColumnNames`).
    AllExcept(Vec<String>),
}

/// One principal's effective permissions on one table, as returned by `ListPermissions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalTablePermissions {
    /// The principal identifier (an IAM role/user ARN, or an account ID for cross-account grants).
    pub principal: String,
    /// Permission names as Lake Formation reports them (`"SELECT"`, `"DESCRIBE"`, ...).
    pub permissions: Vec<String>,
    /// The subset of `permissions` the principal may pass on (`PermissionsWithGrantOption`).
    pub grantable: Vec<String>,
    /// The columns the grant covers — `All` for table-level grants, a selection for
    /// column-level (`TableWithColumns`) grants.
    pub columns: ColumnSelection,
}

/// A Lake Formation data-cell filter on a table: the row predicate + column selection scan
/// planning applies when the filter is in effect for the querying principal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDataFilter {
    /// The filter's name (unique per table).
    pub name: String,
    /// The columns visible under this filter.
    pub columns: ColumnSelection,
    /// The row-filter predicate (a PartiQL-ish boolean expression over the table's columns), or
    /// `None` when the filter grants all rows (`AllRowsWildcard`).
    pub row_filter: Option<String>,
}

/// A Lake Formation authorization client for one data catalog (the account-local catalog by
/// default, or `catalog_id` for a cross-account/shared catalog).
pub struct LakeFormationAuth {
    client: aws_sdk_lakeformation::Client,
    /// The Glue data-catalog ID (AWS account ID) the tables live in. `None` means the caller's
    /// own account — Lake Formation's default when `CatalogId` is omitted.
    catalog_id: Option<String>,
}

impl LakeFormationAuth {
    /// Build a client for `region`, loading the surrounding AWS config (credentials chain,
    /// retry/behavior defaults) via `aws-config`. Async because loading the SDK config is; no
    /// network I/O happens here — credentials resolve lazily on first call.
    pub async fn new(region: impl Into<String>, catalog_id: Option<String>) -> Self {
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_lakeformation::config::Region::new(region.into()))
            .load()
            .await;
        Self::from_client(aws_sdk_lakeformation::Client::new(&sdk_config), catalog_id)
    }

    /// Build from a flat options map (`region`, `catalog_id`) — the same shape the Glue catalog
    /// connection options use, so a `glue` catalog's options can be forwarded verbatim. `region`
    /// resolves as option → `AWS_REGION` → `AWS_DEFAULT_REGION` → `us-west-2`.
    pub async fn from_config(options: &HashMap<String, String>) -> Self {
        let region = resolve_region(
            options,
            std::env::var("AWS_REGION").ok().as_deref(),
            std::env::var("AWS_DEFAULT_REGION").ok().as_deref(),
        );
        let catalog_id = options.get("catalog_id").cloned();
        Self::new(region, catalog_id).await
    }

    /// Build from a preconfigured SDK client — tests inject a client pointed at a stub endpoint.
    pub fn from_client(client: aws_sdk_lakeformation::Client, catalog_id: Option<String>) -> Self {
        Self { client, catalog_id }
    }

    /// The effective permissions on `{database}.{table}` as one entry per principal grant.
    ///
    /// `principal` (an IAM ARN) narrows the query server-side to that principal; `None` lists
    /// every principal's grants on the table. An empty result means Lake Formation has no grants
    /// for the table (or none for that principal) — i.e. access is denied, since Lake Formation
    /// is deny-by-default. (Note: `ListPermissions` filtered by `Resource.Table` returns
    /// grants made *directly* on this table/its columns, including any `IAM_ALLOWED_PRINCIPALS`
    /// legacy-hybrid-mode grant, which Lake Formation surfaces as an ordinary principal entry —
    /// but it does not merge in a broader database- or catalog-level grant that would also cover
    /// this table. Resolving that hierarchy, plus LF-Tag-based policies, is out of scope here.)
    pub async fn effective_permissions(
        &self,
        database: &str,
        table: &str,
        principal: Option<&str>,
    ) -> Result<Vec<PrincipalTablePermissions>> {
        let mut grants = Vec::new();
        let mut next_token: Option<String> = None;
        // `ListPermissions` paginates via NextToken; the SDK does not auto-paginate, so loop.
        loop {
            let mut req = self
                .client
                .list_permissions()
                .resource_type(DataLakeResourceType::Table)
                .resource(self.table_resource_filter(database, table))
                .set_next_token(next_token);
            if let Some(id) = &self.catalog_id {
                req = req.catalog_id(id);
            }
            if let Some(p) = principal {
                req = req.principal(
                    DataLakePrincipal::builder()
                        .data_lake_principal_identifier(p)
                        .build(),
                );
            }
            let resp = req
                .send()
                .await
                .map_err(|e| sdk_failure("ListPermissions", &e))?;
            grants.extend(
                resp.principal_resource_permissions
                    .unwrap_or_default()
                    .iter()
                    .map(map_principal_permissions),
            );
            match resp.next_token {
                Some(token) => next_token = Some(token),
                None => break,
            }
        }
        Ok(grants)
    }

    /// All data-cell filters defined on `{database}.{table}` (row filters + column masks scan
    /// planning must apply for principals the filter is granted to).
    pub async fn data_cells_filters(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Vec<TableDataFilter>> {
        let mut filters = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let resp = self
                .client
                .list_data_cells_filter()
                .table(self.table_resource(database, table))
                .set_next_token(next_token)
                .send()
                .await
                .map_err(|e| sdk_failure("ListDataCellsFilter", &e))?;
            filters.extend(
                resp.data_cells_filters
                    .unwrap_or_default()
                    .iter()
                    .map(map_data_cells_filter),
            );
            match resp.next_token {
                Some(token) => next_token = Some(token),
                None => break,
            }
        }
        Ok(filters)
    }

    /// The `TableResource` identifying the table, with `CatalogId` set for cross-account catalogs.
    fn table_resource(&self, database: &str, table: &str) -> TableResource {
        let mut b = TableResource::builder().database_name(database).name(table);
        if let Some(id) = &self.catalog_id {
            b = b.catalog_id(id);
        }
        // Infallible in practice: `database_name` (the only required field) is always set above.
        b.build().expect("database_name is always set")
    }

    /// The `ListPermissions` resource filter for the table (a `Resource::Table` wrapper).
    fn table_resource_filter(&self, database: &str, table: &str) -> Resource {
        Resource::builder()
            .table(self.table_resource(database, table))
            .build()
    }
}

/// Map one `ListPermissions` entry to [`PrincipalTablePermissions`]. Pure (SDK struct in, own
/// struct out) so it's unit-testable without an endpoint.
fn map_principal_permissions(prp: &PrincipalResourcePermissions) -> PrincipalTablePermissions {
    let columns = match prp
        .resource
        .as_ref()
        .and_then(|r| r.table_with_columns.as_ref())
    {
        // A column-level grant: either an explicit column list or a wildcard with exclusions.
        Some(twc) => match &twc.column_wildcard {
            Some(cw) => {
                ColumnSelection::AllExcept(cw.excluded_column_names.clone().unwrap_or_default())
            }
            None => ColumnSelection::Named(twc.column_names.clone().unwrap_or_default()),
        },
        None => ColumnSelection::All,
    };
    PrincipalTablePermissions {
        principal: prp
            .principal
            .as_ref()
            .and_then(|p| p.data_lake_principal_identifier.clone())
            .unwrap_or_default(),
        permissions: prp
            .permissions
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|p| p.as_str().to_string())
            .collect(),
        grantable: prp
            .permissions_with_grant_option
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|p| p.as_str().to_string())
            .collect(),
        columns,
    }
}

/// Map a Lake Formation [`DataCellsFilter`] to [`TableDataFilter`]. Pure so it's unit-testable
/// without an endpoint.
fn map_data_cells_filter(f: &DataCellsFilter) -> TableDataFilter {
    let columns = match (&f.column_wildcard, &f.column_names) {
        (Some(cw), _) => {
            ColumnSelection::AllExcept(cw.excluded_column_names.clone().unwrap_or_default())
        }
        (None, Some(names)) => ColumnSelection::Named(names.clone()),
        (None, None) => ColumnSelection::All,
    };
    // `AllRowsWildcard` (no `FilterExpression`) means the filter passes every row through.
    let row_filter = f
        .row_filter
        .as_ref()
        .and_then(|rf| rf.filter_expression.clone());
    TableDataFilter {
        name: f.name.clone(),
        columns,
        row_filter,
    }
}

/// Classify a failed Lake Formation API call from its error code + message.
///
/// `EntityNotFoundException` (unknown database/table/filter) is an expected "doesn't exist"
/// signal, mapped to [`Error::Plan`] — the same convention the Glue catalog uses, so the catalog
/// bridge treats it as "not found" rather than a hard error. Everything else (notably
/// `AccessDeniedException`) maps to [`Error::Io`], with the service code kept in the message —
/// operators grep for `AccessDeniedException` & friends.
///
/// Pure (code + message in, [`Error`] out) so the mapping is unit-testable without constructing
/// an SDK error.
fn classify_lakeformation_failure(action: &str, code: Option<&str>, message: &str) -> Error {
    let detail = match code {
        Some(code) => format!("{code}: {message}"),
        None => message.to_string(),
    };
    if code == Some("EntityNotFoundException") {
        Error::Plan(format!("aws lakeformation {action}: {detail}"))
    } else {
        Error::Io(format!("aws lakeformation {action}: {detail}"))
    }
}

/// Map a failed SDK call to [`classify_lakeformation_failure`] via the error's service code +
/// message. Non-service failures (timeouts, connector/transport errors, ...) carry no code, so
/// they land in the [`Error::Io`] bucket — a real error, never "not found".
fn sdk_failure<E>(action: &str, err: &aws_sdk_lakeformation::error::SdkError<E>) -> Error
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let detail = err
        .message()
        .map(str::to_string)
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| format!("{err:?}"));
    classify_lakeformation_failure(action, err.code(), &detail)
}

/// Resolve the AWS region: option → `AWS_REGION` → `AWS_DEFAULT_REGION` → `us-west-2`. Env
/// values are injected so unit tests can cover the full precedence chain without mutating
/// process environment. Same precedence as the Glue catalog.
fn resolve_region(
    options: &HashMap<String, String>,
    aws_region: Option<&str>,
    aws_default_region: Option<&str>,
) -> String {
    if let Some(r) = options.get("region").filter(|s| !s.is_empty()) {
        return r.clone();
    }
    if let Some(r) = aws_region.filter(|s| !s.is_empty()) {
        return r.to_string();
    }
    if let Some(r) = aws_default_region.filter(|s| !s.is_empty()) {
        return r.to_string();
    }
    "us-west-2".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_lakeformation::types::{
        AllRowsWildcard, ColumnWildcard, Permission, RowFilter, TableWithColumnsResource,
    };

    // ---------------------------------------------------------------------
    // Pure mapping / classification tests — SDK-type fixtures, no endpoint.
    // ---------------------------------------------------------------------

    fn principal(arn: &str) -> DataLakePrincipal {
        DataLakePrincipal::builder()
            .data_lake_principal_identifier(arn)
            .build()
    }

    fn table_resource(db: &str, table: &str) -> Resource {
        Resource::builder()
            .table(
                TableResource::builder()
                    .database_name(db)
                    .name(table)
                    .build()
                    .expect("table resource"),
            )
            .build()
    }

    #[test]
    fn maps_table_level_grant_to_all_columns() {
        let prp = PrincipalResourcePermissions::builder()
            .principal(principal("arn:aws:iam::123456789012:role/analyst"))
            .resource(table_resource("db1", "orders"))
            .permissions(Permission::Select)
            .permissions(Permission::Describe)
            .permissions_with_grant_option(Permission::Select)
            .build();
        let mapped = map_principal_permissions(&prp);
        assert_eq!(mapped.principal, "arn:aws:iam::123456789012:role/analyst");
        assert_eq!(mapped.permissions, vec!["SELECT", "DESCRIBE"]);
        assert_eq!(mapped.grantable, vec!["SELECT"]);
        assert_eq!(mapped.columns, ColumnSelection::All);
    }

    #[test]
    fn maps_column_grant_with_named_columns() {
        let twc = TableWithColumnsResource::builder()
            .database_name("db1")
            .name("orders")
            .column_names("id")
            .column_names("amount")
            .build()
            .expect("table with columns");
        let prp = PrincipalResourcePermissions::builder()
            .principal(principal("arn:aws:iam::123456789012:role/scientist"))
            .resource(Resource::builder().table_with_columns(twc).build())
            .permissions(Permission::Select)
            .build();
        let mapped = map_principal_permissions(&prp);
        assert_eq!(
            mapped.columns,
            ColumnSelection::Named(vec!["id".to_string(), "amount".to_string()])
        );
        assert!(mapped.grantable.is_empty());
    }

    #[test]
    fn maps_column_grant_with_wildcard_exclusions() {
        let twc = TableWithColumnsResource::builder()
            .database_name("db1")
            .name("orders")
            .column_wildcard(
                ColumnWildcard::builder()
                    .excluded_column_names("ssn")
                    .build(),
            )
            .build()
            .expect("table with columns");
        let prp = PrincipalResourcePermissions::builder()
            .principal(principal("arn:aws:iam::123456789012:role/analyst"))
            .resource(Resource::builder().table_with_columns(twc).build())
            .permissions(Permission::Select)
            .build();
        let mapped = map_principal_permissions(&prp);
        assert_eq!(
            mapped.columns,
            ColumnSelection::AllExcept(vec!["ssn".to_string()])
        );
    }

    #[test]
    fn maps_data_cells_filter_with_row_expression_and_named_columns() {
        let f = DataCellsFilter::builder()
            .table_catalog_id("123456789012")
            .name("region_us")
            .database_name("db1")
            .table_name("orders")
            .row_filter(
                RowFilter::builder()
                    .filter_expression("region = 'us'")
                    .build(),
            )
            .column_names("id")
            .column_names("amount")
            .build()
            .expect("data cells filter");
        let mapped = map_data_cells_filter(&f);
        assert_eq!(mapped.name, "region_us");
        assert_eq!(mapped.row_filter.as_deref(), Some("region = 'us'"));
        assert_eq!(
            mapped.columns,
            ColumnSelection::Named(vec!["id".to_string(), "amount".to_string()])
        );
    }

    #[test]
    fn all_rows_wildcard_maps_to_no_row_filter() {
        let f = DataCellsFilter::builder()
            .table_catalog_id("123456789012")
            .database_name("db1")
            .table_name("orders")
            .name("hide_pii")
            .row_filter(
                RowFilter::builder()
                    .all_rows_wildcard(AllRowsWildcard::builder().build())
                    .build(),
            )
            .column_wildcard(
                ColumnWildcard::builder()
                    .excluded_column_names("ssn")
                    .build(),
            )
            .build()
            .expect("data cells filter");
        let mapped = map_data_cells_filter(&f);
        assert_eq!(mapped.row_filter, None);
        assert_eq!(
            mapped.columns,
            ColumnSelection::AllExcept(vec!["ssn".to_string()])
        );
    }

    #[test]
    fn filter_without_column_spec_covers_all_columns() {
        let f = DataCellsFilter::builder()
            .table_catalog_id("123456789012")
            .database_name("db1")
            .table_name("orders")
            .name("recent")
            .row_filter(
                RowFilter::builder()
                    .filter_expression("dt >= '2025-01-01'")
                    .build(),
            )
            .build()
            .expect("data cells filter");
        let mapped = map_data_cells_filter(&f);
        assert_eq!(mapped.columns, ColumnSelection::All);
    }

    #[test]
    fn entity_not_found_classifies_as_not_found() {
        match classify_lakeformation_failure(
            "ListPermissions",
            Some("EntityNotFoundException"),
            "Entity Not Found",
        ) {
            Error::Plan(msg) => assert!(msg.contains("Entity Not Found")),
            other => panic!("expected Error::Plan, got {other:?}"),
        }
    }

    #[test]
    fn access_denied_classifies_as_io_error() {
        match classify_lakeformation_failure(
            "ListPermissions",
            Some("AccessDeniedException"),
            "not authorized",
        ) {
            Error::Io(msg) => assert!(msg.contains("AccessDeniedException")),
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }

    #[test]
    fn generic_failure_classifies_as_io_error() {
        // Transport/connector failures carry no service error code.
        match classify_lakeformation_failure("ListDataCellsFilter", None, "connection closed") {
            Error::Io(msg) => assert!(msg.contains("connection closed")),
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }

    #[test]
    fn region_precedence_option_env_default() {
        let mut opts = HashMap::new();
        opts.insert("region".to_string(), "eu-west-1".to_string());
        assert_eq!(
            resolve_region(&opts, Some("us-east-1"), Some("ap-south-1")),
            "eu-west-1"
        );
        assert_eq!(
            resolve_region(&HashMap::new(), Some("us-east-1"), Some("ap-south-1")),
            "us-east-1"
        );
        assert_eq!(
            resolve_region(&HashMap::new(), None, Some("ap-south-1")),
            "ap-south-1"
        );
        assert_eq!(resolve_region(&HashMap::new(), None, None), "us-west-2");
        opts.insert("region".to_string(), "".to_string());
        assert_eq!(
            resolve_region(&opts, Some(""), Some("ap-south-1")),
            "ap-south-1"
        );
    }

    // ---------------------------------------------------------------------
    // Stub-endpoint integration test: a raw mini HTTP server speaks just enough of the AWS JSON
    // 1.1 protocol (POST + `x-amz-target` dispatch) for the real SDK client to run
    // `effective_permissions` / `data_cells_filters` end-to-end — no AWS in CI.
    // ---------------------------------------------------------------------

    use aws_sdk_lakeformation::config::{
        BehaviorVersion, Credentials, Region, SharedCredentialsProvider,
    };

    // Page 1 carries a `NextToken`; page 2 only answers when the follow-up request threads the
    // token through, so the two-page assertions double as proof the manual pagination loop works.
    const PERMISSIONS_PAGE1_JSON: &str = r#"{"PrincipalResourcePermissions":[{"Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123456789012:role/analyst"},"Resource":{"Table":{"DatabaseName":"db1","Name":"orders"}},"Permissions":["SELECT","DESCRIBE"],"PermissionsWithGrantOption":["SELECT"]}],"NextToken":"p2"}"#;
    const PERMISSIONS_PAGE2_JSON: &str = r#"{"PrincipalResourcePermissions":[{"Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123456789012:role/scientist"},"Resource":{"TableWithColumns":{"DatabaseName":"db1","Name":"orders","ColumnNames":["id","amount"]}},"Permissions":["SELECT"]}]}"#;
    const DATA_FILTERS_JSON: &str = r#"{"DataCellsFilters":[{"TableCatalogId":"123456789012","DatabaseName":"db1","TableName":"orders","Name":"region_us","RowFilter":{"FilterExpression":"region = 'us'"},"ColumnNames":["id","amount"]},{"DatabaseName":"db1","TableName":"orders","Name":"hide_pii","RowFilter":{"AllRowsWildcard":{}},"ColumnWildcard":{"ExcludedColumnNames":["ssn"]}}]}"#;
    // Lake Formation's deny-by-default response for a principal with no grants at all: an empty
    // list, not an error — `effective_permissions` must surface this as `Ok(vec![])`.
    const EMPTY_PERMISSIONS_JSON: &str = r#"{"PrincipalResourcePermissions":[]}"#;
    const ACCESS_DENIED_JSON: &str =
        r#"{"__type":"AccessDeniedException","message":"User is not authorized"}"#;

    /// Bind a stub Lake Formation endpoint on a loopback ephemeral port; each accepted connection
    /// reads one request and answers from the `x-amz-target` / body. Returns the port; the server
    /// task runs until aborted.
    async fn spawn_lakeformation_stub() -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub");
        let port = listener.local_addr().expect("local addr").port();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0_u8; 8192];
                    // Read the full request: headers first, then exactly Content-Length bytes.
                    let header_end = loop {
                        let n = sock.read(&mut chunk).await.expect("read");
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let content_length = head
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.trim()
                                .eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < header_end + content_length {
                        let n = sock.read(&mut chunk).await.expect("read body");
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    let request = String::from_utf8_lossy(&buf).to_string();
                    let page2 = request.contains(r#""NextToken":"p2""#);
                    let (status, body) = if request.contains("denied") {
                        ("400 Bad Request", ACCESS_DENIED_JSON)
                    } else if request.contains("ListDataCellsFilter") {
                        ("200 OK", DATA_FILTERS_JSON)
                    } else if request.contains("ListPermissions") {
                        if request.contains("nobody") {
                            ("200 OK", EMPTY_PERMISSIONS_JSON)
                        } else if page2 {
                            ("200 OK", PERMISSIONS_PAGE2_JSON)
                        } else {
                            ("200 OK", PERMISSIONS_PAGE1_JSON)
                        }
                    } else {
                        (
                            "400 Bad Request",
                            r#"{"__type":"ValidationException","message":"?"}"#,
                        )
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/x-amz-json-1.1\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }

    fn stub_client(port: u16) -> LakeFormationAuth {
        let conf = aws_sdk_lakeformation::Config::builder()
            .endpoint_url(format!("http://127.0.0.1:{port}"))
            .region(Region::new("us-west-2"))
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "akid", "secret", None, None, "test",
            )))
            .behavior_version(BehaviorVersion::latest())
            .build();
        LakeFormationAuth::from_client(
            aws_sdk_lakeformation::Client::from_conf(conf),
            Some("123456789012".to_string()),
        )
    }

    #[tokio::test]
    async fn sdk_client_round_trips_against_stub_endpoint() {
        let port = spawn_lakeformation_stub().await;
        let auth = stub_client(port);

        // ListPermissions → one entry per principal grant across BOTH pages: a table-level
        // grant (page 1) and a column-level grant (page 2, only reachable via NextToken).
        let grants = auth
            .effective_permissions("db1", "orders", None)
            .await
            .expect("permissions");
        assert_eq!(grants.len(), 2, "both paginated pages");
        assert_eq!(
            grants[0].principal,
            "arn:aws:iam::123456789012:role/analyst"
        );
        assert_eq!(grants[0].permissions, vec!["SELECT", "DESCRIBE"]);
        assert_eq!(grants[0].grantable, vec!["SELECT"]);
        assert_eq!(grants[0].columns, ColumnSelection::All);
        assert_eq!(
            grants[1].columns,
            ColumnSelection::Named(vec!["id".to_string(), "amount".to_string()])
        );

        // ListDataCellsFilter → both filters on the table: a row filter with named columns and
        // an all-rows column mask with exclusions.
        let filters = auth
            .data_cells_filters("db1", "orders")
            .await
            .expect("data filters");
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].name, "region_us");
        assert_eq!(filters[0].row_filter.as_deref(), Some("region = 'us'"));
        assert_eq!(
            filters[0].columns,
            ColumnSelection::Named(vec!["id".to_string(), "amount".to_string()])
        );
        assert_eq!(filters[1].name, "hide_pii");
        assert_eq!(filters[1].row_filter, None);
        assert_eq!(
            filters[1].columns,
            ColumnSelection::AllExcept(vec!["ssn".to_string()])
        );

        // A 400 AccessDeniedException maps to `Error::Io` (a real error), never "not found".
        let err = auth
            .effective_permissions(
                "db1",
                "orders",
                Some("arn:aws:iam::123456789012:role/denied"),
            )
            .await
            .expect_err("denied principal");
        assert!(matches!(err, Error::Io(_)), "{err:?}");

        // No grants at all for a principal is a successful empty result, not an error — Lake
        // Formation is deny-by-default, so the caller must treat `Ok(vec![])` as "no access"
        // rather than special-casing an error path.
        let grants = auth
            .effective_permissions(
                "db1",
                "orders",
                Some("arn:aws:iam::123456789012:role/nobody"),
            )
            .await
            .expect("empty permissions is not an error");
        assert!(grants.is_empty(), "{grants:?}");
    }
}
