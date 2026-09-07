//! Per-assertion parity ratchet (OxidantData/Oxidant#191).
//!
//! Aggregate strict/semantic counts can hide a pass→failure if another block
//! failure→pass in the same run. Compare named block scores.

use std::collections::HashMap;

/// One assertion's score in a baseline or current run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockScore {
    pub id: String,
    pub strict: bool,
    pub semantic: bool,
}

/// A named assertion that lost a previously recorded pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Regression {
    pub id: String,
}

/// Return every old pass whose matching new score is no longer a pass.
pub fn find_regressions(old: &[BlockScore], new: &[BlockScore]) -> Vec<Regression> {
    let new_by: HashMap<&str, &BlockScore> = new.iter().map(|s| (s.id.as_str(), s)).collect();
    let mut out = Vec::new();
    for o in old {
        let Some(n) = new_by.get(o.id.as_str()) else {
            if o.strict || o.semantic {
                out.push(Regression { id: o.id.clone() });
            }
            continue;
        };
        if (o.semantic && !n.semantic) || (o.strict && !n.strict) {
            out.push(Regression { id: o.id.clone() });
        }
    }
    out
}
