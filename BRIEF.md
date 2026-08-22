# SPIKE: ONNX batch inference in the Oxidant engine (issue OxidantData/Oxidant#118)

This is a THROWAWAY spike branch (`ml-predict-spike`) in a worktree of the public
OxidantData/Oxidant repo. NEVER push. Goal: validate feasibility and produce numbers + a
recommended design. Do NOT polish for merge — but do keep the spike code committed on the
branch so it can be reviewed and mined.

## Read first
AGENTS.md (gates), and find where SQL scalar UDFs live (search for existing UDF
implementations, e.g. in oxidant-execution or a functions crate).

## Questions to answer (issue #118)
1. **tract feasibility**: add `tract-onnx` to a scratch crate or an existing functions home.
   Export two real models to ONNX in a python venv: sklearn GradientBoostingClassifier via
   skl2onnx (the wine-quality classifier from the Databricks tutorial shape: 11 double
   features → class + probabilities) and a tiny torch MLP via torch.onnx.export. Load both
   with tract; note ANY ops gaps or dtype gymnastics (this is the #1 risk — GBDT ONNX graphs
   exercise TreeEnsemble ops).
2. **Scoring strategy**: implement the UDF two ways — (a) per-row, (b) per-RecordBatch with
   features stacked into one tensor. Benchmark both over ~1M synthetic rows; report rows/sec
   for each. Batch should win; if it doesn't, find out why.
3. **Model lifecycle**: load from a local path and from s3:// via the engine's existing
   object-store wiring (find how table scans read S3); cache per executor keyed by
   uri+etag/size; measure load time; note memory footprint of the cached model.
4. **API recommendation**: `ml_predict('s3://bucket/model.onnx', col1, ...)` scalar UDF vs
   DDL-registered model. Try the UDF; write up which you'd ship and why.

## Deliverables (in the final message + a spike report committed at docs/spikes/ml-predict.md)
- Working `ml_predict` UDF scoring both ONNX models in `oxidant sql` (or the fastest SQL
  entrypoint) with REAL output shown
- Benchmark numbers (row vs batch, 1M rows, your machine noted)
- Ops/dtype gaps found in tract for sklearn + torch exports
- Recommendation: ship/no-ship, API shape, estimated build effort, risks

## Gates (spike-relaxed but real)
It must compile and run: `cargo build -p oxidant-cli`, `cargo test -p <touched>` for any
crate you touched, fmt+clippy on touched crates (allow(dead_code) etc. acceptable in spike
code with a comment). Keep commits small. Kill any background servers when done.
