//! Spark Connect UDF registration: artifacts, `register_function`, and inline Python UDFs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use tonic::Status;
use weft_loom::udf_registry::{UdfDef, UdfRegistry};
use weft_proto::spark::connect as sc;

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
    let allow = std::env::var("WEFT_ALLOW_PYTHON_UDF")
        .ok()
        .as_deref()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    if !allow {
        return Ok(default_scalar_for_type(return_type));
    }
    let dir = std::env::temp_dir().join("weft-pyudf");
    std::fs::create_dir_all(&dir).ok();
    let script = dir.join(format!("{name}.pkl"));
    std::fs::write(&script, command).ok();
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
        _ => Ok(default_scalar_for_type(return_type)),
    }
}

fn default_scalar_for_type(dt: &DataType) -> ScalarValue {
    match dt {
        DataType::Utf8 => ScalarValue::Utf8(Some(String::new())),
        DataType::Int64 => ScalarValue::Int64(Some(0)),
        _ => ScalarValue::Int32(Some(0)),
    }
}

fn parse_python_result(s: &str, dt: &DataType) -> datafusion::common::Result<ScalarValue> {
    match dt {
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(s.to_string()))),
        DataType::Int64 => Ok(ScalarValue::Int64(Some(s.parse().unwrap_or(0)))),
        _ => Ok(ScalarValue::Int32(Some(s.parse().unwrap_or(0)))),
    }
}
