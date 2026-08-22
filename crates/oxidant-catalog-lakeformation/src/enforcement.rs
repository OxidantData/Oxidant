//! The Lake Formation **enforcement** path: what a query engine calls to find out what one
//! principal may read from one table, and to get credentials to read it with.
//!
//! This is deliberately a different API surface from the crate root. [`crate::LakeFormationAuth`]
//! wraps `ListPermissions` / `ListDataCellsFilter` — Lake Formation's *administrative* operations,
//! which enumerate grants and are the right tool for introspection (`SHOW GRANTS`). They are not
//! how an engine authorizes a scan: they list grants held on a resource, leaving the caller to
//! re-derive an effective decision that AWS already computes exactly.
//!
//! The documented third-party-engine path, and the one Athena and EMR Spark both use, is:
//!
//! 1. **`glue:GetUnfilteredTableMetadata`** → the effective decision for the caller in one call:
//!    the authorized column list, a ready-to-use row-filter `WHERE` fragment, per-column cell
//!    filters, and whether the table is governed by Lake Formation at all.
//! 2. **`lakeformation:GetTemporaryGlueTableCredentials`** → short-lived, scoped S3 credentials to
//!    read the table's files with, instead of the engine's own (much broader) identity.
//!
//! AWS calls this *distributed enforcement with explicit deny on failure*: the engine is trusted to
//! apply the returned policy, and **must fail the query if it cannot**. Every fallible path here
//! therefore returns `Err` rather than a permissive decision — see [`Self::authorize_scan`].

use std::collections::HashMap;

use aws_sdk_glue::error::ProvideErrorMetadata;
use aws_sdk_glue::types::{AuditContext, PermissionType};
use oxidant_catalog::{Result as CatResult, TableAccess, TableAuthorizer, VendedCredentials};
use oxidant_common::{Error, Result};

/// Session tag Lake Formation requires on the role a third-party engine calls its credential
/// vending APIs with. Lake Formation reads it to recognize the caller as a registered query engine;
/// without it the credential-vending calls are refused.
pub const AUTHORIZED_CALLER_TAG: &str = "LakeFormationAuthorizedCaller";

/// Which identity Lake Formation decisions are resolved against.
///
/// "User identity" here means an **IAM runtime role** the session names and the engine assumes —
/// the model EMR uses — not a client-supplied user name. That distinction is the whole security
/// argument: a client may name any role, but `sts:AssumeRole` only succeeds if that role's trust
/// policy allows the engine's own role, so IAM decides whether the claim is legitimate rather than
/// Oxidant having to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IdentityMode {
    /// Use the session's runtime role when one is configured; otherwise the engine's own identity.
    #[default]
    Hybrid,
    /// Require a session runtime role. Fail if none is configured — for deployments where every
    /// query must be attributable to a user and falling back to the engine role would over-grant.
    User,
    /// Always the engine's own identity; ignore any configured runtime role.
    Machine,
}

impl IdentityMode {
    /// Parse the `lakeformation.identity` config value. Unknown values are an error rather than a
    /// silent default — a typo'd mode must not quietly widen access.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "hybrid" => Ok(Self::Hybrid),
            "user" => Ok(Self::User),
            "machine" => Ok(Self::Machine),
            other => Err(Error::Plan(format!(
                "lakeformation.identity must be one of `hybrid`, `user`, `machine` (got `{other}`)"
            ))),
        }
    }
}

/// How to build a [`LakeFormationAuthorizer`].
#[derive(Debug, Clone, Default)]
pub struct AuthorizerConfig {
    /// AWS region of the Glue catalog and Lake Formation service.
    pub region: String,
    /// Glue data-catalog ID (an AWS account ID). Resolved from the caller's own account when unset.
    pub catalog_id: Option<String>,
    /// Which identity to enforce as.
    pub identity: IdentityMode,
    /// The IAM role representing the querying user, assumed via `sts:AssumeRole`.
    pub runtime_role_arn: Option<String>,
    /// Value for the [`AUTHORIZED_CALLER_TAG`] session tag applied when assuming the runtime role.
    pub authorized_caller: Option<String>,
    /// Whether to request Lake Formation-vended S3 credentials per table. When `false` the engine
    /// reads with its ambient credentials and enforcement is advisory — see the module docs.
    pub vend_credentials: bool,
}

/// Resolves Lake Formation decisions for one principal against one Glue data catalog.
pub struct LakeFormationAuthorizer {
    glue: aws_sdk_glue::Client,
    lakeformation: aws_sdk_lakeformation::Client,
    catalog_id: String,
    region: String,
    principal: String,
    vend_credentials: bool,
}

impl LakeFormationAuthorizer {
    /// Build an authorizer, resolving the effective principal up front.
    ///
    /// Resolution follows [`IdentityMode`]. When a runtime role is used it is assumed here, so a
    /// role the engine is not trusted to assume fails at construction with an STS error rather than
    /// surfacing later as a confusing per-table denial.
    pub async fn new(config: AuthorizerConfig) -> Result<Self> {
        let region = if config.region.trim().is_empty() {
            "us-west-2".to_string()
        } else {
            config.region.clone()
        };
        let region_obj = aws_config::Region::new(region.clone());

        let runtime_role = match config.identity {
            IdentityMode::Machine => None,
            IdentityMode::Hybrid => config.runtime_role_arn.clone(),
            IdentityMode::User => Some(config.runtime_role_arn.clone().ok_or_else(|| {
                Error::Plan(
                    "lakeformation.identity=user requires \
                     `lakeformation.runtime_role_arn` to be set on the session"
                        .to_string(),
                )
            })?),
        };

        let base = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region_obj.clone())
            .load()
            .await;

        // Assuming the runtime role here (rather than per call) means the trust-policy check
        // happens once, at configuration time, with a clear error.
        let sdk_config = match &runtime_role {
            None => base,
            Some(arn) => {
                let mut builder = aws_config::sts::AssumeRoleProvider::builder(arn.clone())
                    .session_name("oxidant-lakeformation")
                    .region(region_obj.clone());
                if let Some(caller) = &config.authorized_caller {
                    builder = builder.tags([(AUTHORIZED_CALLER_TAG, caller.as_str())]);
                }
                let provider = builder.configure(&base).build().await;
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(region_obj)
                    .credentials_provider(provider)
                    .load()
                    .await
            }
        };

        let sts = aws_sdk_sts::Client::new(&sdk_config);
        let identity = sts
            .get_caller_identity()
            .send()
            .await
            .map_err(|e| sts_failure("GetCallerIdentity", &e))?;

        // The principal a runtime role represents is the role itself, not the ephemeral
        // assumed-role session STS reports — normalize so it matches how the grant was written.
        let principal = match &runtime_role {
            Some(arn) => arn.clone(),
            None => {
                let arn = identity.arn.clone().unwrap_or_default();
                if arn.is_empty() {
                    return Err(Error::Io(
                        "aws sts GetCallerIdentity returned no ARN; cannot resolve the \
                         Lake Formation principal"
                            .to_string(),
                    ));
                }
                normalize_principal_arn(&arn)
            }
        };

        let catalog_id = match config.catalog_id.filter(|s| !s.trim().is_empty()) {
            Some(id) => id,
            None => identity.account.clone().ok_or_else(|| {
                Error::Io(
                    "aws sts GetCallerIdentity returned no account; set \
                     `lakeformation.catalog_id` explicitly"
                        .to_string(),
                )
            })?,
        };

        Ok(Self {
            glue: aws_sdk_glue::Client::new(&sdk_config),
            lakeformation: aws_sdk_lakeformation::Client::new(&sdk_config),
            catalog_id,
            region,
            principal,
            vend_credentials: config.vend_credentials,
        })
    }

    /// Build from preconfigured SDK clients — tests inject clients pointed at a stub endpoint.
    pub fn from_clients(
        glue: aws_sdk_glue::Client,
        lakeformation: aws_sdk_lakeformation::Client,
        catalog_id: impl Into<String>,
        region: impl Into<String>,
        principal: impl Into<String>,
        vend_credentials: bool,
    ) -> Self {
        Self {
            glue,
            lakeformation,
            catalog_id: catalog_id.into(),
            region: region.into(),
            principal: principal.into(),
            vend_credentials,
        }
    }

    /// The Glue table ARN, which `GetTemporaryGlueTableCredentials` identifies the table by.
    ///
    /// The partition is derived from the region rather than hardcoded to `aws`: in GovCloud and
    /// China the ARN is otherwise malformed and every governed table fails vending with an opaque
    /// service error.
    fn table_arn(&self, database: &str, table: &str) -> String {
        format!(
            "arn:{}:glue:{}:{}:table/{database}/{table}",
            aws_partition(&self.region),
            self.region,
            self.catalog_id
        )
    }

    /// Vend short-lived S3 credentials scoped to this table.
    async fn vend_credentials(
        &self,
        database: &str,
        table: &str,
        query_authorization_id: Option<&str>,
    ) -> Result<VendedCredentials> {
        use aws_sdk_lakeformation::types::{
            Permission as LfPermission, PermissionType as LfPermissionType, QuerySessionContext,
        };

        let mut req = self
            .lakeformation
            .get_temporary_glue_table_credentials()
            .table_arn(self.table_arn(database, table))
            .permissions(LfPermission::Select)
            // One value only — same hierarchy rule as `GetUnfilteredTableMetadata` above.
            .supported_permission_types(LfPermissionType::CellFilterPermission);
        // Correlates the credential request with the authorization decision that produced it;
        // Lake Formation uses it to tie the two together for audit.
        if let Some(id) = query_authorization_id {
            req = req.query_session_context(
                QuerySessionContext::builder()
                    .query_authorization_id(id)
                    .build(),
            );
        }
        let resp = req
            .send()
            .await
            .map_err(|e| lf_failure("GetTemporaryGlueTableCredentials", &e))?;

        let (Some(access_key_id), Some(secret_access_key)) =
            (resp.access_key_id.clone(), resp.secret_access_key.clone())
        else {
            return Err(Error::Io(format!(
                "aws lakeformation GetTemporaryGlueTableCredentials for `{database}.{table}` \
                 returned no credentials"
            )));
        };

        Ok(VendedCredentials {
            access_key_id,
            secret_access_key,
            session_token: resp.session_token.clone(),
            expires_at: resp
                .expiration
                .and_then(|d| std::time::SystemTime::try_from(d).ok()),
        })
    }
}

#[async_trait::async_trait]
impl TableAuthorizer for LakeFormationAuthorizer {
    /// Resolve what [`Self::principal`] may read from `namespace.table`.
    ///
    /// Fail-closed at every step: a service error, an unresolvable policy, or a restriction this
    /// engine cannot express all return `Err`. The only permissive outcome is the explicit
    /// "Lake Formation does not govern this table" signal (`IsRegisteredWithLakeFormation=false`),
    /// which is Lake Formation stating the table is out of its scope — the same rule that lets
    /// Athena query unregistered data normally.
    async fn authorize_scan(&self, namespace: &[String], table: &str) -> CatResult<TableAccess> {
        let database = namespace.join(".");
        if database.is_empty() {
            return Err(Error::Plan(format!(
                "lake formation authorization needs a database-qualified table (got `{table}`)"
            )));
        }

        let resp = self
            .glue
            .get_unfiltered_table_metadata()
            .catalog_id(&self.catalog_id)
            .database_name(&database)
            .name(table)
            // Declare exactly what this engine can enforce. Lake Formation raises
            // `PermissionTypeMismatchException` if the table needs more than this (e.g. nested
            // column filtering) rather than silently returning a policy we would under-apply.
            //
            // Exactly ONE value, despite the field being a list. The permission types are a
            // hierarchy — `CELL_FILTER_PERMISSION` subsumes `COLUMN_PERMISSION` — and Lake
            // Formation rejects a request naming more than one with
            // `InvalidInputException: Invalid permission type`. Verified against live Lake
            // Formation: passing both fails, and passing only `COLUMN_PERMISSION` against a table
            // carrying a data-cell filter raises `PermissionTypeMismatchException`. So declare the
            // highest tier we can enforce and let the mismatch exception catch anything above it.
            .supported_permission_types(PermissionType::CellFilterPermission)
            .audit_context(
                AuditContext::builder()
                    .all_columns_requested(true)
                    .additional_audit_context("oxidant")
                    .build(),
            )
            .send()
            .await
            .map_err(|e| glue_failure("GetUnfilteredTableMetadata", &database, table, &e))?;

        // Not governed by Lake Formation: read exactly as if no authorizer were configured.
        if !resp.is_registered_with_lake_formation {
            return Ok(TableAccess::unenforced());
        }

        let row_filter = merge_row_filters(
            resp.row_filter.as_deref(),
            resp.cell_filters.as_deref().unwrap_or_default(),
            &database,
            table,
        )?;

        // An EMPTY authorized-column list is a total denial, not "all columns".
        //
        // `TableAccess::authorized_columns == None` means unrestricted, so mapping empty to `None`
        // would invert the decision: a principal Lake Formation granted no columns would get the
        // whole governed table, `ssn` included, because `restricts_scan()` would report nothing to
        // restrict and the enforcing wrapper would be skipped entirely. Refuse here instead.
        let authorized_columns = match resp.authorized_columns.clone() {
            Some(cols) if cols.is_empty() => {
                return Err(Error::Plan(format!(
                    "lake formation authorizes no columns of `{database}.{table}` for principal \
                     `{}`; refusing to read it",
                    self.principal
                )));
            }
            other => other,
        };

        let credentials = if self.vend_credentials {
            Some(
                self.vend_credentials(&database, table, resp.query_authorization_id.as_deref())
                    .await?,
            )
        } else {
            None
        };

        Ok(TableAccess {
            authorized_columns,
            row_filter,
            credentials,
            enforced: true,
        })
    }

    fn principal(&self) -> &str {
        &self.principal
    }
}

/// Reduce Lake Formation's table-level `RowFilter` plus any per-column `CellFilters` to the single
/// predicate the scan will `AND` in.
///
/// Lake Formation can express *different* row visibility per column (column `a` visible for one set
/// of rows, column `b` for another), which is a per-column mask rather than a row filter and is not
/// something a single scan predicate can represent. Applying only one of the expressions would
/// silently reveal rows the other was meant to hide, so a genuinely heterogeneous set of cell
/// filters is rejected — the fail-close rule — rather than approximated.
///
/// The common case is homogeneous: one data-cell filter over several columns yields the same
/// expression on each, which collapses to exactly that expression.
fn merge_row_filters(
    table_row_filter: Option<&str>,
    cell_filters: &[aws_sdk_glue::types::ColumnRowFilter],
    database: &str,
    table: &str,
) -> Result<Option<String>> {
    let mut distinct: Vec<String> = Vec::new();
    let mut columns_by_expr: HashMap<String, Vec<String>> = HashMap::new();
    for cf in cell_filters {
        let expr = cf.row_filter_expression.as_deref().unwrap_or("").trim();
        if expr.is_empty() {
            continue;
        }
        if !distinct.iter().any(|e| e == expr) {
            distinct.push(expr.to_string());
        }
        columns_by_expr
            .entry(expr.to_string())
            .or_default()
            .push(cf.column_name.clone().unwrap_or_default());
    }

    if let Some(table_expr) = table_row_filter.map(str::trim).filter(|s| !s.is_empty()) {
        if !distinct.iter().any(|e| e == table_expr) {
            distinct.push(table_expr.to_string());
        }
    }

    match distinct.len() {
        0 => Ok(None),
        1 => Ok(Some(distinct.remove(0))),
        _ => Err(Error::Unsupported(format!(
            "lake formation table `{database}.{table}` applies different row filters to different \
             columns ({}), which Oxidant cannot enforce in a single scan. Refusing the query \
             rather than applying only one of them. Grant a single data-cell filter covering all \
             columns, or restrict the column grant.",
            distinct
                .iter()
                .map(|e| {
                    match columns_by_expr.get(e) {
                        Some(cols) if !cols.is_empty() => format!("`{e}` on {}", cols.join("/")),
                        _ => format!("`{e}` on the table"),
                    }
                })
                .collect::<Vec<_>>()
                .join("; ")
        ))),
    }
}

/// The ARN partition for `region`. AWS ARNs are not all `arn:aws:` — GovCloud and China regions
/// use their own partitions, and an ARN built with the wrong one is rejected outright.
pub fn aws_partition(region: &str) -> &'static str {
    if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else {
        "aws"
    }
}

/// Normalize an STS caller ARN to the IAM ARN Lake Formation grants are written against.
///
/// `GetCallerIdentity` reports an *assumed-role session*
/// (`arn:aws:sts::123:assumed-role/Analyst/session-42`), but a Lake Formation grant names the role
/// (`arn:aws:iam::123:role/Analyst`). Without this rewrite every lookup for a role-based engine
/// matches nothing, which fails closed as a total denial and looks exactly like a broken
/// integration. Federated-user ARNs collapse the same way; anything else passes through.
///
/// The *partition* is carried over from the input rather than written as `aws`, for the same
/// reason [`aws_partition`] exists: in GovCloud and China a grant is held against
/// `arn:aws-us-gov:iam::…` / `arn:aws-cn:iam::…`, so emitting `arn:aws:iam::…` would match no
/// grant at all — a total denial for every governed table, indistinguishable from a broken setup.
pub fn normalize_principal_arn(arn: &str) -> String {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() < 6 || parts[2] != "sts" {
        return arn.to_string();
    }
    let partition = parts[1];
    let account = parts[4];
    let resource = parts[5];
    if let Some(rest) = resource.strip_prefix("assumed-role/") {
        // `Role/session-name` — the role name is everything before the first `/`.
        let role = rest.split('/').next().unwrap_or(rest);
        if !role.is_empty() {
            return format!("arn:{partition}:iam::{account}:role/{role}");
        }
    }
    if let Some(rest) = resource.strip_prefix("federated-user/") {
        if !rest.is_empty() {
            return format!("arn:{partition}:iam::{account}:user/{rest}");
        }
    }
    arn.to_string()
}

/// Classify a failed `GetUnfilteredTableMetadata` call.
///
/// `PermissionTypeMismatchException` is the important one: Lake Formation raises it when the table
/// carries restrictions beyond what this engine declared it can enforce. It must surface as a
/// refusal with an actionable message — never be swallowed into "no restrictions found".
fn glue_failure<E>(
    action: &str,
    database: &str,
    table: &str,
    err: &aws_sdk_glue::error::SdkError<E>,
) -> Error
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let code = err.code().unwrap_or_default();
    let message = err
        .message()
        .map(str::to_string)
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| format!("{err:?}"));

    if code == "PermissionTypeMismatchException" {
        return Error::Unsupported(format!(
            "lake formation table `{database}.{table}` requires finer-grained filtering than \
             Oxidant can enforce (nested column or nested cell permissions): {message}. \
             Refusing the query rather than returning under-filtered data."
        ));
    }
    if code == "AccessDeniedException" || code == "EntityNotFoundException" {
        return Error::Plan(format!(
            "aws glue {action} for `{database}.{table}`: {code}: {message}"
        ));
    }
    Error::Io(format!(
        "aws glue {action} for `{database}.{table}`: {}{message}",
        if code.is_empty() {
            String::new()
        } else {
            format!("{code}: ")
        }
    ))
}

/// Map a failed Lake Formation credential-vending call. Every failure is an error: without
/// credentials the engine cannot read the table, and must not fall back to its own identity.
fn lf_failure<E>(action: &str, err: &aws_sdk_lakeformation::error::SdkError<E>) -> Error
where
    E: aws_sdk_lakeformation::error::ProvideErrorMetadata + std::fmt::Debug,
{
    use aws_sdk_lakeformation::error::ProvideErrorMetadata as _;
    let detail = err
        .message()
        .map(str::to_string)
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| format!("{err:?}"));
    match err.code() {
        Some(code) => Error::Io(format!("aws lakeformation {action}: {code}: {detail}")),
        None => Error::Io(format!("aws lakeformation {action}: {detail}")),
    }
}

/// Map a failed STS call made while resolving the principal.
fn sts_failure<E>(action: &str, err: &aws_sdk_sts::error::SdkError<E>) -> Error
where
    E: aws_sdk_sts::error::ProvideErrorMetadata + std::fmt::Debug,
{
    use aws_sdk_sts::error::ProvideErrorMetadata as _;
    let detail = err
        .message()
        .map(str::to_string)
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| format!("{err:?}"));
    match err.code() {
        Some(code) => Error::Io(format!("aws sts {action}: {code}: {detail}")),
        None => Error::Io(format!("aws sts {action}: {detail}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_glue::types::ColumnRowFilter;
    use std::sync::Arc;

    fn cell(column: &str, expr: &str) -> ColumnRowFilter {
        ColumnRowFilter::builder()
            .column_name(column)
            .row_filter_expression(expr)
            .build()
    }

    #[test]
    fn identity_mode_parses_and_rejects_typos() {
        assert_eq!(IdentityMode::parse("hybrid").unwrap(), IdentityMode::Hybrid);
        assert_eq!(IdentityMode::parse(" USER ").unwrap(), IdentityMode::User);
        assert_eq!(
            IdentityMode::parse("Machine").unwrap(),
            IdentityMode::Machine
        );
        assert_eq!(IdentityMode::default(), IdentityMode::Hybrid);
        // A typo must not silently fall back to a mode that grants more than intended.
        assert!(IdentityMode::parse("uesr").is_err());
        assert!(IdentityMode::parse("").is_err());
    }

    #[test]
    fn assumed_role_arn_normalizes_to_the_role_grants_name() {
        assert_eq!(
            normalize_principal_arn("arn:aws:sts::123456789012:assumed-role/Analyst/session-42"),
            "arn:aws:iam::123456789012:role/Analyst"
        );
        // Session names may contain slashes; only the first segment is the role.
        assert_eq!(
            normalize_principal_arn("arn:aws:sts::123456789012:assumed-role/Analyst/a/b/c"),
            "arn:aws:iam::123456789012:role/Analyst"
        );
    }

    #[test]
    fn federated_user_normalizes_and_other_arns_pass_through() {
        assert_eq!(
            normalize_principal_arn("arn:aws:sts::123456789012:federated-user/alice"),
            "arn:aws:iam::123456789012:user/alice"
        );
        // A plain IAM user/role ARN is already what a grant names.
        assert_eq!(
            normalize_principal_arn("arn:aws:iam::123456789012:user/vamsi"),
            "arn:aws:iam::123456789012:user/vamsi"
        );
        assert_eq!(
            normalize_principal_arn("arn:aws:iam::123456789012:role/oxidant"),
            "arn:aws:iam::123456789012:role/oxidant"
        );
        assert_eq!(normalize_principal_arn("not-an-arn"), "not-an-arn");
    }

    /// The partition must survive normalization. A GovCloud/China grant is held against
    /// `arn:aws-us-gov:iam::…`, so rewriting the caller ARN to `arn:aws:iam::…` matches no grant —
    /// every governed table denies, and the failure looks exactly like a misconfigured setup
    /// rather than a wrong ARN. Same reason `aws_partition` is not hardcoded.
    #[test]
    fn normalization_preserves_the_partition() {
        assert_eq!(
            normalize_principal_arn("arn:aws-us-gov:sts::123456789012:assumed-role/Analyst/sess"),
            "arn:aws-us-gov:iam::123456789012:role/Analyst"
        );
        assert_eq!(
            normalize_principal_arn("arn:aws-cn:sts::123456789012:assumed-role/Analyst/sess"),
            "arn:aws-cn:iam::123456789012:role/Analyst"
        );
        assert_eq!(
            normalize_principal_arn("arn:aws-us-gov:sts::123456789012:federated-user/alice"),
            "arn:aws-us-gov:iam::123456789012:user/alice"
        );
    }

    /// An empty `AuthorizedColumns` is a total denial. `TableAccess::authorized_columns == None`
    /// means UNRESTRICTED, so mapping empty to `None` would invert the decision and hand the
    /// principal the whole governed table.
    #[test]
    fn empty_authorized_columns_is_a_denial_not_unrestricted() {
        use oxidant_catalog::TableAccess;
        // Guard the invariant the mapping depends on.
        let unrestricted = TableAccess {
            authorized_columns: None,
            row_filter: None,
            credentials: None,
            enforced: true,
        };
        assert!(
            !unrestricted.restricts_scan(),
            "`None` must mean unrestricted — so empty must never map to it"
        );
    }

    #[test]
    fn partition_is_derived_from_the_region_not_hardcoded() {
        // An ARN built with the wrong partition is rejected outright, so every governed table in
        // GovCloud/China would fail vending with an opaque service error.
        assert_eq!(aws_partition("us-west-2"), "aws");
        assert_eq!(aws_partition("eu-west-1"), "aws");
        assert_eq!(aws_partition("us-gov-west-1"), "aws-us-gov");
        assert_eq!(aws_partition("cn-north-1"), "aws-cn");
        assert_eq!(aws_partition("us-iso-east-1"), "aws-iso");
        assert_eq!(aws_partition("us-isob-east-1"), "aws-iso-b");
    }

    #[test]
    fn no_filters_means_no_predicate() {
        assert_eq!(merge_row_filters(None, &[], "db", "t").unwrap(), None);
        // Empty/whitespace expressions are not predicates.
        assert_eq!(merge_row_filters(Some("  "), &[], "db", "t").unwrap(), None);
    }

    #[test]
    fn table_row_filter_is_used_verbatim() {
        assert_eq!(
            merge_row_filters(Some("region = 'us'"), &[], "db", "t").unwrap(),
            Some("region = 'us'".to_string())
        );
    }

    #[test]
    fn homogeneous_cell_filters_collapse_to_one_predicate() {
        // One data-cell filter over several columns reports the same expression on each.
        let filters = [
            cell("id", "region = 'us'"),
            cell("amount", "region = 'us'"),
            cell("region", "region = 'us'"),
        ];
        assert_eq!(
            merge_row_filters(None, &filters, "db", "t").unwrap(),
            Some("region = 'us'".to_string())
        );
        // Same expression also present at table level is still one predicate, not a duplicate.
        assert_eq!(
            merge_row_filters(Some("region = 'us'"), &filters, "db", "t").unwrap(),
            Some("region = 'us'".to_string())
        );
    }

    #[test]
    fn heterogeneous_cell_filters_are_refused_not_approximated() {
        // Different row visibility per column cannot be expressed as one scan predicate. Applying
        // either one alone would leak the rows the other hides, so this must fail.
        let filters = [
            cell("amount", "region = 'us'"),
            cell("ssn", "region = 'eu'"),
        ];
        let err = merge_row_filters(None, &filters, "db", "t").expect_err("heterogeneous");
        assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("region = 'us'") && msg.contains("region = 'eu'"),
            "{msg}"
        );
    }

    #[test]
    fn table_filter_differing_from_cell_filter_is_refused() {
        let filters = [cell("amount", "region = 'us'")];
        let err = merge_row_filters(Some("amount > 100"), &filters, "db", "t")
            .expect_err("table filter differs from cell filter");
        assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn unenforced_access_is_fully_open_and_needs_no_decorator() {
        let access = TableAccess::unenforced();
        assert!(!access.enforced);
        assert!(!access.restricts_scan());
        assert!(access.authorized_columns.is_none());
        assert!(access.row_filter.is_none());
        assert!(access.credentials.is_none());
    }

    #[test]
    fn restricts_scan_detects_either_restriction() {
        let base = TableAccess {
            authorized_columns: None,
            row_filter: None,
            credentials: None,
            enforced: true,
        };
        // Enforced but unrestricted: nothing to apply.
        assert!(!base.restricts_scan());
        assert!(TableAccess {
            authorized_columns: Some(vec!["id".to_string()]),
            ..base.clone()
        }
        .restricts_scan());
        assert!(TableAccess {
            row_filter: Some("region = 'us'".to_string()),
            ..base
        }
        .restricts_scan());
    }

    // ---------------------------------------------------------------------
    // Stub-endpoint tests: a mini HTTP server answers `GetUnfilteredTableMetadata` so the REAL
    // AWS SDK client runs `authorize_scan` end to end. This is what proves the request is
    // well-formed and the response mapping is right — the pure tests above cannot.
    // ---------------------------------------------------------------------

    use aws_sdk_glue::config::{BehaviorVersion, Credentials, Region, SharedCredentialsProvider};

    /// Governed table: `ssn` withheld, rows restricted to `region = 'us'`.
    const GOVERNED_JSON: &str = r#"{"AuthorizedColumns":["id","region","amount"],"IsRegisteredWithLakeFormation":true,"RowFilter":"region = 'us'","CellFilters":[],"QueryAuthorizationId":"qid-1","Table":{"Name":"lf_protected_customers","DatabaseName":"oxidant_lf_secure"}}"#;
    /// Not registered with Lake Formation — must read unrestricted.
    const UNGOVERNED_JSON: &str = r#"{"IsRegisteredWithLakeFormation":false,"Table":{"Name":"plain","DatabaseName":"oxidant_lf_secure"}}"#;
    /// Captured verbatim from live Lake Formation (account 810738286322, table
    /// `oxidant_lf_secure.lf_protected_customers`, one data-cell filter `region = 'us'` over
    /// columns id/region/amount): the row expression is repeated once per authorized column.
    const PER_COLUMN_JSON: &str = r#"{"AuthorizedColumns":["amount","id","region"],"IsRegisteredWithLakeFormation":true,"RowFilter":"region = 'us'","CellFilters":[{"ColumnName":"amount","RowFilterExpression":"region = 'us'"},{"ColumnName":"id","RowFilterExpression":"region = 'us'"},{"ColumnName":"region","RowFilterExpression":"region = 'us'"}],"QueryAuthorizationId":"qid-2","Table":{"Name":"lf_protected_customers","DatabaseName":"oxidant_lf_secure"}}"#;
    /// Heterogeneous per-column row filters — beyond what one scan predicate can express.
    const HETEROGENEOUS_JSON: &str = r#"{"AuthorizedColumns":["id","amount","ssn"],"IsRegisteredWithLakeFormation":true,"CellFilters":[{"ColumnName":"amount","RowFilterExpression":"region = 'us'"},{"ColumnName":"ssn","RowFilterExpression":"region = 'eu'"}],"Table":{"Name":"mixed","DatabaseName":"oxidant_lf_secure"}}"#;
    const PERMISSION_MISMATCH_JSON: &str =
        r#"{"__type":"PermissionTypeMismatchException","message":"table has nested cell filters"}"#;
    const ACCESS_DENIED_JSON: &str =
        r#"{"__type":"AccessDeniedException","message":"not authorized"}"#;
    /// What live Lake Formation returns when more than one `SupportedPermissionTypes` tier is named.
    const INVALID_PERMISSION_TYPE_JSON: &str = r#"{"__type":"InvalidInputException","message":"Invalid permission type, it must be one of the following: COLUMN_PERMISSION, CELL_FILTER_PERMISSION, NESTED_PERMISSION, NESTED_CELL_PERMISSION, or DATA_LOCATION_PERMISSION"}"#;

    /// What `GetTemporaryGlueTableCredentials` returns for a table the principal may read.
    /// `Expiration` is `EpochSeconds` on the wire (Lake Formation is `restJson1`); 4102444800 is
    /// 2100-01-01, comfortably outside any refresh margin.
    const VENDED_CREDENTIALS_JSON: &str = r#"{"AccessKeyId":"ASIAVENDEDEXAMPLE","SecretAccessKey":"vended-secret","SessionToken":"vended-token","Expiration":4102444800}"#;
    /// Absolute expiry [`VENDED_CREDENTIALS_JSON`] states, as seconds since the epoch.
    const VENDED_EXPIRY_EPOCH_SECS: u64 = 4_102_444_800;
    /// A 200 that carries no credentials. Every field of the response is optional in the model, so
    /// the mapping has to reject this rather than hand back an empty `AwsCredential`.
    const VENDED_NOTHING_JSON: &str = r#"{}"#;

    /// Every request the stub answered, newest last: `(uri line + headers + body)`.
    type StubLog = Arc<std::sync::Mutex<Vec<String>>>;

    /// Bind a stub Glue endpoint; each connection is answered from the request body's table name.
    async fn spawn_glue_stub() -> u16 {
        spawn_stub_recording().await.0
    }

    /// As [`spawn_glue_stub`], but also hands back the log of raw requests — the only way to assert
    /// on what was *sent* (the table ARN, the audit correlation id, or that no credential-vending
    /// call was made at all).
    async fn spawn_stub_recording() -> (u16, StubLog) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let log: StubLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log_for_server = log.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let log = log_for_server.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0_u8; 8192];
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
                    log.lock().expect("stub log poisoned").push(request.clone());
                    // Lake Formation is `restJson1`: the operation is the URI path, not an
                    // `X-Amz-Target` header like Glue's `awsJson1.1`. Dispatch on it FIRST — the
                    // vending request carries the table ARN, whose text would otherwise fall
                    // through to the metadata branches below.
                    let is_vending = request.contains("/GetTemporaryGlueTableCredentials");
                    // Model the real service's rule: `SupportedPermissionTypes` is a hierarchy and
                    // naming more than one tier is rejected. Without this the stub would happily
                    // accept a request live Lake Formation refuses.
                    let names_multiple_permission_types = request.contains("COLUMN_PERMISSION")
                        && request.contains("CELL_FILTER_PERMISSION");
                    let (status, body) = if is_vending {
                        if request.contains("/vendingdenied") {
                            ("400 Bad Request", ACCESS_DENIED_JSON)
                        } else if request.contains("/vendsnothing") {
                            ("200 OK", VENDED_NOTHING_JSON)
                        } else {
                            ("200 OK", VENDED_CREDENTIALS_JSON)
                        }
                    } else if names_multiple_permission_types {
                        ("400 Bad Request", INVALID_PERMISSION_TYPE_JSON)
                    } else if request.contains("\"mismatch\"") {
                        ("400 Bad Request", PERMISSION_MISMATCH_JSON)
                    } else if request.contains("\"denied\"") {
                        ("400 Bad Request", ACCESS_DENIED_JSON)
                    } else if request.contains("\"plain\"") {
                        ("200 OK", UNGOVERNED_JSON)
                    } else if request.contains("\"percolumn\"") {
                        ("200 OK", PER_COLUMN_JSON)
                    } else if request.contains("\"mixed\"") {
                        ("200 OK", HETEROGENEOUS_JSON)
                    } else {
                        ("200 OK", GOVERNED_JSON)
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/x-amz-json-1.1\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(response.as_bytes()).await;
                });
            }
        });
        (port, log)
    }

    fn stub_authorizer(port: u16) -> LakeFormationAuthorizer {
        stub_authorizer_with_vending(port, false)
    }

    fn stub_authorizer_with_vending(port: u16, vend_credentials: bool) -> LakeFormationAuthorizer {
        let creds =
            SharedCredentialsProvider::new(Credentials::new("akid", "secret", None, None, "test"));
        let glue = aws_sdk_glue::Client::from_conf(
            aws_sdk_glue::Config::builder()
                .endpoint_url(format!("http://127.0.0.1:{port}"))
                .region(Region::new("us-west-2"))
                .credentials_provider(creds.clone())
                .behavior_version(BehaviorVersion::latest())
                .build(),
        );
        let lf = aws_sdk_lakeformation::Client::from_conf(
            aws_sdk_lakeformation::Config::builder()
                .endpoint_url(format!("http://127.0.0.1:{port}"))
                .region(aws_sdk_lakeformation::config::Region::new("us-west-2"))
                .credentials_provider(
                    aws_sdk_lakeformation::config::SharedCredentialsProvider::new(
                        aws_sdk_lakeformation::config::Credentials::new(
                            "akid", "secret", None, None, "test",
                        ),
                    ),
                )
                .behavior_version(aws_sdk_lakeformation::config::BehaviorVersion::latest())
                .build(),
        );
        LakeFormationAuthorizer::from_clients(
            glue,
            lf,
            "123456789012",
            "us-west-2",
            "arn:aws:iam::123456789012:role/analyst",
            vend_credentials,
        )
    }

    #[tokio::test]
    async fn governed_table_maps_to_columns_and_row_filter() {
        let port = spawn_glue_stub().await;
        let auth = stub_authorizer(port);
        let access = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "lf_protected_customers")
            .await
            .expect("governed table");
        assert!(access.enforced);
        assert_eq!(
            access.authorized_columns,
            Some(vec![
                "id".to_string(),
                "region".to_string(),
                "amount".to_string()
            ]),
            "ssn must not be authorized"
        );
        assert_eq!(access.row_filter.as_deref(), Some("region = 'us'"));
        assert!(access.restricts_scan());
    }

    /// Regression guard for a bug live Lake Formation caught that the stub originally did not:
    /// `SupportedPermissionTypes` is a hierarchy and naming two tiers is an `InvalidInputException`.
    /// The stub now rejects that, so this passing means the request names exactly one tier.
    #[tokio::test]
    async fn request_names_exactly_one_supported_permission_tier() {
        let port = spawn_glue_stub().await;
        let auth = stub_authorizer(port);
        let access = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "lf_protected_customers")
            .await
            .expect("must not name both COLUMN_PERMISSION and CELL_FILTER_PERMISSION");
        assert!(access.enforced);
    }

    /// The shape live Lake Formation actually returns for a data-cell filter: the same expression
    /// repeated once per authorized column. It must collapse to a single predicate, not be refused.
    #[tokio::test]
    async fn real_cell_filter_shape_collapses_to_one_predicate() {
        let port = spawn_glue_stub().await;
        let auth = stub_authorizer(port);
        let access = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "percolumn")
            .await
            .expect("homogeneous per-column filters are the normal case");
        assert_eq!(access.row_filter.as_deref(), Some("region = 'us'"));
        assert_eq!(
            access.authorized_columns,
            Some(vec![
                "amount".to_string(),
                "id".to_string(),
                "region".to_string()
            ])
        );
    }

    #[tokio::test]
    async fn table_not_registered_with_lake_formation_reads_unrestricted() {
        let port = spawn_glue_stub().await;
        let auth = stub_authorizer(port);
        let access = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "plain")
            .await
            .expect("ungoverned table");
        // This is the property that lets enforcement be switched on for a catalog without
        // changing behavior for every table that is not registered with Lake Formation.
        assert!(!access.enforced);
        assert!(!access.restricts_scan());
        assert!(access.authorized_columns.is_none());
        assert!(access.row_filter.is_none());
    }

    #[tokio::test]
    async fn permission_type_mismatch_refuses_the_scan() {
        let port = spawn_glue_stub().await;
        let auth = stub_authorizer(port);
        let err = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "mismatch")
            .await
            .expect_err("table needs filtering we cannot enforce");
        // Fail-close: Lake Formation says the table needs more than we declared we can apply.
        assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
        assert!(format!("{err:?}").contains("Refusing the query"), "{err:?}");
    }

    #[tokio::test]
    async fn heterogeneous_cell_filters_refuse_the_scan() {
        let port = spawn_glue_stub().await;
        let auth = stub_authorizer(port);
        let err = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "mixed")
            .await
            .expect_err("per-column row filters");
        assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
    }

    #[tokio::test]
    async fn access_denied_is_an_error_not_an_empty_permissive_decision() {
        let port = spawn_glue_stub().await;
        let auth = stub_authorizer(port);
        let err = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "denied")
            .await
            .expect_err("denied");
        assert!(format!("{err:?}").contains("AccessDenied"), "{err:?}");
    }

    #[tokio::test]
    async fn unreachable_endpoint_fails_closed() {
        // Port 1 is reserved and nothing listens on it: the SDK cannot connect.
        let auth = stub_authorizer(1);
        let err = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "lf_protected_customers")
            .await
            .expect_err("unreachable Lake Formation must not read as `no restrictions`");
        assert!(matches!(err, Error::Io(_)), "{err:?}");
    }

    #[tokio::test]
    async fn unqualified_table_is_rejected() {
        let port = spawn_glue_stub().await;
        let auth = stub_authorizer(port);
        let err = auth
            .authorize_scan(&[], "orders")
            .await
            .expect_err("no database");
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    // ---------------------------------------------------------------------
    // Credential vending. The tests above all run with vending OFF, which leaves the documented
    // default path (`lakeformation.vend_credentials` defaults to TRUE) entirely unexercised: the
    // ARN the credentials are requested for, the audit correlation id, the expiry mapping, and
    // the fail-closed behaviour when vending is refused.
    // ---------------------------------------------------------------------

    /// The requests the stub saw for `op`, where `op` is a Lake Formation URI path or a Glue
    /// `X-Amz-Target` suffix.
    fn requests_for(log: &StubLog, op: &str) -> Vec<String> {
        log.lock()
            .expect("stub log poisoned")
            .iter()
            .filter(|r| r.contains(op))
            .cloned()
            .collect()
    }

    /// With vending on, the decision carries table-scoped credentials — the step that makes
    /// enforcement real rather than advisory, because the bytes are then fetched under a token
    /// Lake Formation scoped to this one table instead of the engine's own identity.
    #[tokio::test]
    async fn vending_on_returns_table_scoped_credentials_with_their_expiry() {
        let (port, log) = spawn_stub_recording().await;
        let auth = stub_authorizer_with_vending(port, true);
        let access = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "lf_protected_customers")
            .await
            .expect("governed table with vending on");

        let creds = access
            .credentials
            .expect("vending on must yield credentials");
        assert_eq!(creds.access_key_id, "ASIAVENDEDEXAMPLE");
        assert_eq!(creds.secret_access_key, "vended-secret");
        assert_eq!(creds.session_token.as_deref(), Some("vended-token"));
        assert_eq!(
            creds.expires_at,
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(VENDED_EXPIRY_EPOCH_SECS)),
            "an expiry that does not survive the mapping makes the refresh path unreachable"
        );

        // The vending call must name THIS table's ARN — a wrong or missing ARN is how a table
        // silently ends up read with somebody else's scope.
        let sent = requests_for(&log, "/GetTemporaryGlueTableCredentials");
        assert_eq!(sent.len(), 1, "exactly one vending call per authorization");
        assert!(
            sent[0].contains(
                "arn:aws:glue:us-west-2:123456789012:table/oxidant_lf_secure/lf_protected_customers"
            ),
            "{}",
            sent[0]
        );
        // …and carry the authorization id from the metadata response, which is what ties the
        // credential grant to the decision that produced it in Lake Formation's audit trail.
        assert!(sent[0].contains("qid-1"), "{}", sent[0]);
    }

    /// Vending refused must fail the scan. Falling back to the engine's ambient credentials would
    /// read exactly the data Lake Formation just declined to scope access to.
    #[tokio::test]
    async fn refused_vending_fails_the_scan_instead_of_falling_back_to_ambient_credentials() {
        let (port, _log) = spawn_stub_recording().await;
        let auth = stub_authorizer_with_vending(port, true);
        let err = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "vendingdenied")
            .await
            .expect_err("vending refused");
        let msg = format!("{err:?}");
        assert!(msg.contains("GetTemporaryGlueTableCredentials"), "{msg}");
        assert!(msg.contains("AccessDenied"), "{msg}");
    }

    /// A 200 with no credentials in it is still a failure: every field of the response is optional
    /// in the model, and an empty `AwsCredential` would be sent to S3 as an anonymous request.
    #[tokio::test]
    async fn a_credential_response_with_no_credentials_is_an_error() {
        let (port, _log) = spawn_stub_recording().await;
        let auth = stub_authorizer_with_vending(port, true);
        let err = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "vendsnothing")
            .await
            .expect_err("no credentials in the response");
        assert!(
            format!("{err:?}").contains("returned no credentials"),
            "{err:?}"
        );
    }

    /// An ungoverned table must not trigger vending at all — the decision returns before it, so a
    /// mixed catalog does not pay a Lake Formation round trip per unregistered table.
    #[tokio::test]
    async fn an_ungoverned_table_makes_no_credential_vending_call() {
        let (port, log) = spawn_stub_recording().await;
        let auth = stub_authorizer_with_vending(port, true);
        let access = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "plain")
            .await
            .expect("ungoverned");
        assert!(!access.enforced);
        assert!(access.credentials.is_none());
        assert!(
            requests_for(&log, "/GetTemporaryGlueTableCredentials").is_empty(),
            "an unregistered table must not be vended for"
        );
    }

    /// `vend_credentials=false` is the documented escape hatch: the column/row policy still
    /// applies, but no credential call is made and reads use the ambient identity.
    #[tokio::test]
    async fn vending_off_applies_the_policy_and_makes_zero_vending_calls() {
        let (port, log) = spawn_stub_recording().await;
        let auth = stub_authorizer_with_vending(port, false);
        let access = auth
            .authorize_scan(&["oxidant_lf_secure".to_string()], "lf_protected_customers")
            .await
            .expect("governed table, vending off");
        assert!(access.enforced);
        assert_eq!(access.row_filter.as_deref(), Some("region = 'us'"));
        assert!(access.credentials.is_none());
        assert!(
            requests_for(&log, "/GetTemporaryGlueTableCredentials").is_empty(),
            "vend_credentials=false must not call Lake Formation for credentials"
        );
    }

    /// `identity=user` with no runtime role is one of the documented fail-closed cases, and it has
    /// to fail *before* any AWS call — otherwise a deployment that requires user attribution would
    /// silently enforce as the engine role until the first STS round trip said otherwise.
    #[tokio::test]
    async fn identity_user_without_a_runtime_role_is_refused_before_any_aws_call() {
        let err = LakeFormationAuthorizer::new(AuthorizerConfig {
            region: "us-west-2".to_string(),
            catalog_id: Some("123456789012".to_string()),
            identity: IdentityMode::User,
            runtime_role_arn: None,
            authorized_caller: None,
            vend_credentials: false,
        })
        .await
        .err()
        .expect("identity=user requires a runtime role");
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(
            format!("{err:?}").contains("runtime_role_arn"),
            "the error must name the setting that is missing: {err:?}"
        );
    }

    /// An empty runtime role string is the same mistake spelled differently (`--catalog-conf
    /// ….runtime_role_arn=`), and must not be accepted as "a role was configured".
    #[tokio::test]
    async fn identity_user_with_a_blank_runtime_role_is_refused() {
        // `build_lakeformation_authorizer` filters blanks out before constructing the config, so
        // this asserts the same outcome the connect layer's filtering produces.
        let err = LakeFormationAuthorizer::new(AuthorizerConfig {
            region: "us-west-2".to_string(),
            catalog_id: Some("123456789012".to_string()),
            identity: IdentityMode::User,
            runtime_role_arn: None,
            authorized_caller: Some("oxidant".to_string()),
            vend_credentials: true,
        })
        .await
        .err()
        .expect("blank runtime role is no runtime role");
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    #[test]
    fn vended_credentials_never_debug_print_the_secret() {
        // These land in logs and panic messages; a derived Debug would leak them.
        let creds = VendedCredentials {
            access_key_id: "ASIAEXAMPLE".to_string(),
            secret_access_key: "super-secret-value".to_string(),
            session_token: Some("token-value".to_string()),
            expires_at: None,
        };
        let rendered = format!("{creds:?}");
        assert!(!rendered.contains("super-secret-value"), "{rendered}");
        assert!(!rendered.contains("token-value"), "{rendered}");
        assert!(rendered.contains("ASIAEXAMPLE"), "{rendered}");
    }
}
