//! Immutable session names attached to one local query's DataFusion configuration.
//!
//! Physical scalar expressions retain an `Arc<ConfigOptions>` and pass it to the UDF
//! even in spawned partition tasks. A caller's Tokio task-local is not inherited there.

use std::any::Any;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::common::{config_err, Result};

#[derive(Debug, Clone)]
pub(crate) struct QuerySessionNames {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
}

impl ConfigExtension for QuerySessionNames {
    const PREFIX: &'static str = "oxidant_query_session";
}

impl ExtensionOptions for QuerySessionNames {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, _key: &str, _value: &str) -> Result<()> {
        config_err!("query session names are read-only; use USE to select catalog/namespace")
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        // Physical-expression equality compares config entries. Include both values so
        // expressions bound to different sessions cannot compare equal.
        vec![
            ConfigEntry {
                key: format!("{}.catalog", Self::PREFIX),
                value: Some(self.catalog.clone()),
                description: "Query's current catalog",
            },
            ConfigEntry {
                key: format!("{}.namespace", Self::PREFIX),
                value: Some(self.namespace.clone()),
                description: "Query's current database/schema",
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use datafusion::arrow::array::StringArray;
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_plan::ExecutionPlanProperties;

    #[test]
    fn session_name_config_is_read_only_and_deep_cloned() {
        let mut config = ConfigOptions::default();
        config.extensions.insert(QuerySessionNames {
            catalog: "prod".into(),
            namespace: "sales".into(),
        });
        let snapshot = config.clone();
        assert!(config
            .set("oxidant_query_session.catalog", "other")
            .is_err());
        config
            .extensions
            .get_mut::<QuerySessionNames>()
            .unwrap()
            .namespace = "finance".into();
        assert_eq!(
            snapshot
                .extensions
                .get::<QuerySessionNames>()
                .unwrap()
                .namespace,
            "sales"
        );
        assert_ne!(config.entries(), snapshot.entries());
    }

    #[tokio::test]
    async fn reused_logical_plan_keeps_independent_immutable_execution_snapshots() {
        let engine = Engine::new();
        let template = engine
            .plan_spark(
                "SELECT current_catalog() AS c, current_schema() AS s \
             UNION ALL SELECT current_catalog() AS c, current_schema() AS s",
            )
            .await
            .unwrap();
        // Fixture the current-name cell directly: no table resolution/provider is involved.
        *engine.current.lock().unwrap() = ("prod".into(), vec!["sales".into()]);
        let (_, first) = engine.bind_session_names(template.clone());
        *engine.current.lock().unwrap() = ("other".into(), vec!["finance".into()]);
        let (_, second) = engine.bind_session_names(template.clone());
        // Both the session and the original template remain usable after binding. Even a
        // switch BEFORE physical planning must not change a previously bound DataFrame.
        *engine.current.lock().unwrap() = ("later".into(), vec!["changed".into()]);
        for (df, catalog, namespace) in [
            (first, "prod", "sales"),
            (second, "other", "finance"),
            (template, "spark_catalog", "default"),
        ] {
            let plan = df.create_physical_plan().await.unwrap();
            assert!(plan.output_partitioning().partition_count() > 1);
            let task_ctx = engine.ctx.task_ctx();
            // Deliberately use an unbound TaskContext in a fresh task: the physical
            // expressions themselves carry their snapshot, not the caller or task-local.
            let batches = tokio::spawn(datafusion::physical_plan::collect(plan, task_ctx))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
            for batch in batches {
                let cats = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let namespaces = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    assert_eq!(cats.value(row), catalog);
                    assert_eq!(namespaces.value(row), namespace);
                }
            }
        }
    }
}
