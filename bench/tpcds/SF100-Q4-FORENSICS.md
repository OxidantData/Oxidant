# TPC-DS SF100 Q4 forensics (2026-08-11)

Artifacts: `/tmp/oxidant-sf100-q4-forensics/` (driver + 2 workers journals/env, client smoke JSON).

## Timeline

| UTC | Event |
|-----|--------|
| 04:44 | Driver boot; membership later hot-fixed to `172.31.37.31:50561,172.31.5.214:50561` (AMI `log()` stdout bug had injected `[oxidant-bootstrap]:50561`) |
| 05:37–05:38 | Driver **panics** in `parquet` `push_buffers` (`Range length must match buffer length` 524288 vs 393948) while client hits Connect → `RST_STREAM` |
| 05:39 | Restart driver; empty `OXIDANT_S3_CACHE_DIR` on workers |
| 05:40 | `LIMIT 1` on `store_sales` OK; workers emit `num_partitions=200`, ~19.5k batches, ~44s |
| 05:40–05:48+ | Q4 client connected; **workers idle** (no further stage summaries); driver ~5.3 GiB RSS / ~23% CPU; **no driver query journals** (AMI `RUST_LOG` unset; `tracing::info` invisible) |

## Root causes (stacked)

1. **AMI membership bug** — `log()` wrote to stdout → fake worker host → fan-out 3≠2. Source fixed; **must rebake AMI**.
2. **All-string TPC-DS Parquet** — Q4 failed planning (`Utf8 - Utf8`). Fixed via `tpcds_types.tsv` + typed reconvert; Glue now `decimal(7,2)`.
3. **Driver parquet panic** — truncated S3 range read on driver (likely mid-sync race and/or object_store). Driver should not scan SF100 facts; still need visibility into *why* driver touched Parquet.
4. **Q4 hang / prior OOM** — After one distributed leaf stage, work stuck on the **8 GiB driver** (prior EMR-parity Q4 error: SMJ retry exhausted a ~5.3 GiB fair pool). Workers never got subsequent stages (or never finished logging them).

## Gaps for next smoke

Driver journal had **zero** `Oxidant distributed:` / stage-dispatch lines. Next AMI/binary must emit always-on `eprintln!` progress (landed in tree):

- `Oxidant distributed: begin|optimized|split ok|dispatching|stages done|finalize|ok`
- `Oxidant driver: query=… stages=…` and per-stage `dispatch` / `barrier complete`

Also stamp `RUST_LOG=info,oxidant=info,…` from bootstrap (driver + worker).

## Next Q4 smoke checklist

1. Rebake AMI with bootstrap `log() >&2`, IPv4 CSV filter, `RUST_LOG`, empty worker S3 cache (or cap).
2. Confirm `OXIDANT_WORKERS` is exactly 2 IPs before query.
3. Sync typed Parquet **fully** before any Connect query (avoid range-read panic during overwrite).
4. `journalctl -u oxidant-driver -f` should show split stage count within seconds; if stuck before `dispatching`, hang is **planner/optimize** on driver.
5. If stuck after first `barrier complete` with workers idle, hang is **downstream stage / finalize on driver** — consider larger driver or push more work to workers.
