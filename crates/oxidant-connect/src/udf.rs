//! Spark Connect UDF registration: artifacts, `register_function`, and inline Python UDFs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use oxidant_loom::udf_registry::{UdfDef, UdfRegistry};
use oxidant_proto::spark::connect as sc;
use tonic::Status;

use crate::types::spark_to_arrow;

/// Stored Python UDF artifact bytes keyed by module path.
#[derive(Debug, Default)]
pub struct ArtifactStore {
    files: HashMap<String, Vec<u8>>,
}

impl ArtifactStore {
    pub fn insert(&mut self, path: String, data: Vec<u8>) {
        self.files.insert(path, data);
    }

    #[allow(dead_code)]
    pub fn get(&self, path: &str) -> Option<&[u8]> {
        self.files.get(path).map(|v| v.as_slice())
    }
}

pub type SharedArtifacts = Arc<Mutex<ArtifactStore>>;

fn python_udfs_allowed() -> bool {
    std::env::var("OXIDANT_ALLOW_PYTHON_UDF")
        .ok()
        .as_deref()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true)
}

fn python_udf_disabled_message() -> String {
    "Python UDF execution is disabled (OXIDANT_ALLOW_PYTHON_UDF=false)".to_string()
}

/// Handle `Command.register_function` from PySpark.
pub fn register_connect_udf(
    ctx: &SessionContext,
    registry: &mut UdfRegistry,
    udf: &sc::CommonInlineUserDefinedFunction,
) -> Result<(), Status> {
    let name = udf.function_name.clone();
    if let Some(sc::common_inline_user_defined_function::Function::PythonUdf(py)) =
        udf.function.as_ref()
    {
        if !python_udfs_allowed() {
            return Err(Status::permission_denied(python_udf_disabled_message()));
        }
        let return_type = py
            .output_type
            .as_ref()
            .map(spark_to_arrow)
            .transpose()?
            .unwrap_or(DataType::Int32);

        let arg_count = udf.arguments.len();
        let command = py.command.clone();
        let udf_name = name.clone();

        let py_udf = PythonUdf {
            name: name.clone(),
            command,
            return_type,
            arg_count,
        };
        ctx.register_udf(ScalarUDF::from(py_udf));

        registry.register_sql_fn(UdfDef {
            name: udf_name,
            sql_body: None,
            param_names: (0..arg_count).map(|i| format!("arg{i}")).collect(),
            return_type: "INT".into(),
        });
        return Ok(());
    }

    registry.register_sql_fn(UdfDef {
        name,
        sql_body: Some("1".into()),
        param_names: vec![],
        return_type: "INT".into(),
    });
    registry
        .apply_to_context(ctx)
        .map_err(|e| Status::internal(e.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PythonUdf {
    name: String,
    command: Vec<u8>,
    return_type: DataType,
    arg_count: usize,
}

impl ScalarUDFImpl for PythonUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| {
            Signature::one_of(
                vec![TypeSignature::Exact(vec![]), TypeSignature::VariadicAny],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let scalar_args: Vec<ScalarValue> = args
            .args
            .iter()
            .map(columnar_to_scalar)
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        Ok(ColumnarValue::Scalar(eval_python_udf_scalar(
            &self.name,
            &self.command,
            &scalar_args,
            &self.return_type,
        )?))
    }
}

fn columnar_to_scalar(cv: &ColumnarValue) -> datafusion::common::Result<ScalarValue> {
    match cv {
        ColumnarValue::Scalar(s) => Ok(s.clone()),
        ColumnarValue::Array(a) => ScalarValue::try_from_array(a, 0),
    }
}

fn eval_python_udf_scalar(
    name: &str,
    command: &[u8],
    args: &[ScalarValue],
    return_type: &DataType,
) -> datafusion::common::Result<ScalarValue> {
    if !python_udfs_allowed() {
        return exec_err!("{}", python_udf_disabled_message());
    }
    let dir = std::env::temp_dir().join("oxidant-pyudf");
    std::fs::create_dir_all(&dir).map_err(|e| {
        datafusion::common::DataFusionError::Execution(format!(
            "Python UDF '{name}' could not create a scratch directory: {e}"
        ))
    })?;
    let script = dir.join(format!("{name}.pkl"));
    std::fs::write(&script, command).map_err(|e| {
        datafusion::common::DataFusionError::Execution(format!(
            "Python UDF '{name}' could not write its payload: {e}"
        ))
    })?;
    let arg_literals: Vec<String> = args.iter().map(|v| format!("{v:?}")).collect();
    let out = std::process::Command::new("python3")
        .arg("-c")
        .arg(
            "import pickle,sys; \
             udf=pickle.loads(open(sys.argv[1],'rb').read()); \
             args=[eval(a) for a in sys.argv[2:]]; \
             print(udf(*args) if args else udf())",
        )
        .arg(&script)
        .args(arg_literals)
        .output();
    match out {
        Ok(o) if o.status.success() => {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            parse_python_result(&s, return_type)
        }
        Ok(o) => exec_err!(
            "Python UDF '{name}' exited unsuccessfully (status {})",
            o.status
        ),
        Err(e) => exec_err!("Python UDF '{name}' could not be started: {e}"),
    }
}

fn parse_python_result(s: &str, dt: &DataType) -> datafusion::common::Result<ScalarValue> {
    match dt {
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(s.to_string()))),
        DataType::Int64 => {
            let v = s.parse::<i64>().map_err(|_| {
                datafusion::common::DataFusionError::Execution(format!(
                    "Python UDF returned {s:?}, which is not an Int64"
                ))
            })?;
            Ok(ScalarValue::Int64(Some(v)))
        }
        _ => {
            let v = s.parse::<i32>().map_err(|_| {
                datafusion::common::DataFusionError::Execution(format!(
                    "Python UDF returned {s:?}, which is not an Int32"
                ))
            })?;
            Ok(ScalarValue::Int32(Some(v)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_allow_python_udf<T>(value: Option<&str>, f: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::env::var("OXIDANT_ALLOW_PYTHON_UDF").ok();
        match value {
            Some(v) => std::env::set_var("OXIDANT_ALLOW_PYTHON_UDF", v),
            None => std::env::remove_var("OXIDANT_ALLOW_PYTHON_UDF"),
        }
        let out = f();
        match previous {
            Some(v) => std::env::set_var("OXIDANT_ALLOW_PYTHON_UDF", v),
            None => std::env::remove_var("OXIDANT_ALLOW_PYTHON_UDF"),
        }
        out
    }

    #[test]
    fn disabled_python_udf_is_an_error_not_a_zero() {
        let err = with_allow_python_udf(Some("false"), || {
            eval_python_udf_scalar("review_plus_one", b"unused", &[], &DataType::Int32)
                .expect_err("disabled UDF must fail")
        });
        let msg = err.to_string();
        assert!(
            msg.to_ascii_lowercase().contains("disabled")
                || msg.to_ascii_lowercase().contains("permission"),
            "error must say the UDF is disabled, got {msg}"
        );
        assert!(
            !msg.contains("unused") && !msg.contains("pickle"),
            "error must not leak the serialized function: {msg}"
        );
    }

    #[test]
    fn process_or_parse_failure_is_an_error_not_a_zero() {
        let err = with_allow_python_udf(Some("true"), || {
            eval_python_udf_scalar(
                "broken",
                b"not-a-pickle",
                &[ScalarValue::Int32(Some(1))],
                &DataType::Int32,
            )
            .expect_err("a failing python UDF must fail the query")
        });
        let msg = err.to_string();
        assert!(
            !msg.contains("not-a-pickle"),
            "error must not leak serialized bytes: {msg}"
        );
        let ok_zero = ScalarValue::Int32(Some(0));
        assert_ne!(
            format!("{err}"),
            format!("{ok_zero}"),
            "must not present a successful zero"
        );
    }

    #[test]
    fn register_refuses_python_udf_when_disabled() {
        let ctx = SessionContext::new();
        let mut registry = UdfRegistry::default();
        let udf = sc::CommonInlineUserDefinedFunction {
            function_name: "review_plus_one".to_string(),
            function: Some(
                sc::common_inline_user_defined_function::Function::PythonUdf(sc::PythonUdf {
                    output_type: None,
                    command: b"unused".to_vec(),
                    ..Default::default()
                }),
            ),
            ..Default::default()
        };
        let err = with_allow_python_udf(Some("false"), || {
            register_connect_udf(&ctx, &mut registry, &udf).expect_err("register must refuse")
        });
        assert_ne!(err.code(), tonic::Code::Ok);
        let msg = err.message().to_ascii_lowercase();
        assert!(
            msg.contains("disabled") || msg.contains("permission"),
            "register error must name the policy, got {err}"
        );
    }
}
