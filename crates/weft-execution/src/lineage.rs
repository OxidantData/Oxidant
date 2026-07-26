//! Stage lineage: record which worker produced each shuffle bucket so failed consumers can
//! trigger producer recomputation on a healthy alternate.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Key for one producer task: `(stage_id, partition_id)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProducerKey {
    pub stage_id: u32,
    pub partition_id: u32,
}

/// Tracks completed producer stages for shuffle durability / recompute.
#[derive(Debug, Default)]
pub struct StageLineage {
    /// Producer task → worker endpoint that successfully ran it.
    producers: Mutex<HashMap<ProducerKey, String>>,
}

impl StageLineage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_producer(&self, stage_id: u32, partition_id: u32, endpoint: &str) {
        self.producers.lock().expect("lineage poisoned").insert(
            ProducerKey {
                stage_id,
                partition_id,
            },
            endpoint.to_string(),
        );
    }

    pub fn producer_endpoint(&self, stage_id: u32, partition_id: u32) -> Option<String> {
        self.producers
            .lock()
            .expect("lineage poisoned")
            .get(&ProducerKey {
                stage_id,
                partition_id,
            })
            .cloned()
    }

    pub fn clear(&self) {
        self.producers.lock().expect("lineage poisoned").clear();
    }
}

pub type SharedLineage = Arc<StageLineage>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_retrieves_producer() {
        let l = StageLineage::new();
        l.record_producer(0, 1, "http://w:50561");
        assert_eq!(l.producer_endpoint(0, 1).as_deref(), Some("http://w:50561"));
        assert!(l.producer_endpoint(0, 2).is_none());
    }
}
