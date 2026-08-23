/**
 * The Pipelines page's derivation, under test.
 *
 * `crates/oxidant-ui-server/src/pipeline_derive.js` is the reducer the embedded console runs:
 * it turns a connector's JSONL log into what an operator is told about a running pipeline.
 * The console is a single hand-written HTML file with no build step and no module loader, so
 * the file is spliced into the page by the server (`static_files.rs`) — and imported here as
 * source and evaluated, so it is the *same* code both places. A copy would drift.
 *
 * Every case below is a behaviour the page got wrong before: each one names its finding.
 */
import { describe, expect, it } from "vitest";
// Vite hands `?raw` imports over as the file's text — the same bytes the server splices.
import derivationSource from "../../../crates/oxidant-ui-server/src/pipeline_derive.js?raw";

const PD = new Function(`${derivationSource}\nreturn __oxidantPipelines;`)();

/** A log entry as `GET /api/v1/pipelines` returns it. */
const entry = (name: string, modifiedMs?: number) => ({
  name,
  sizeBytes: 4096,
  modifiedMs: modifiedMs ?? null,
});

/** A tail as `GET /api/v1/pipelines/{name}/logs` returns it. */
const tail = (events: unknown[]) => ({ code: 200, events, malformed: 0, truncated: false });

const iso = (ms: number) => new Date(ms).toISOString();

/** A `batch` line as `postgres_cdc.rs` writes it. */
const batch = (at: number, rows: number, i: number) => ({
  ts: iso(at),
  event: "batch",
  rows,
  duration_ms: 120,
  start_lsn: `0/${(i * 16).toString(16).toUpperCase()}`,
  end_lsn: `0/${((i + 1) * 16).toString(16).toUpperCase()}`,
});

const T0 = Date.UTC(2026, 0, 1, 12, 0, 0);

describe("finding 1 — /api/status is a cross-check, not a substring match on SQL", () => {
  it("does not read a pipeline's name out of an unrelated query's SQL text", () => {
    const queries = [
      // The failing ad-hoc query an operator ran in the Editor. `tag` is the query's
      // *description* — its truncated SQL text — and it happens to name a table that starts
      // with a pipeline's name.
      { id: "stmt-9", tag: "SELECT count(*) FROM daily_totals WHERE bad_col = 1", state: "failed" },
      // …and a long-running one over the other pipeline's table.
      { id: "stmt-8", tag: "SELECT * FROM orders", state: "running" },
    ];
    const now = T0 + 1_000;
    const pipelines = PD.derivePipelines(
      [entry("daily"), entry("orders")],
      {
        // `daily` is healthy: it committed a batch a second ago.
        daily: tail([batch(T0, 500, 1)]),
        // `orders` has logged nothing for an hour: it is idle, whatever the driver says.
        orders: tail([batch(T0 - 3_600_000, 500, 1)]),
      },
      now,
      queries,
    );
    const byName = Object.fromEntries(pipelines.map((p: any) => [p.name, p]));

    expect(byName.daily.state).toBe("running");
    expect(byName.daily.live).toBeNull();
    expect(byName.orders.state).toBe("idle");
    expect(byName.orders.live).toBeNull();
  });

  it("matches only a whole-tag streaming identity, never SQL text", () => {
    for (const tag of ["pipeline: daily", "pipeline=daily", "streaming query: daily", "StreamingQuery[daily]"]) {
      expect(PD.pipeIdentityOf(tag)).toBe("daily");
    }
    for (const tag of [
      "SELECT * FROM daily_totals",
      "SELECT * FROM daily",
      "daily",
      "DataFrame",
      "-- pipeline: daily\nSELECT 1",
      "",
      null,
    ]) {
      expect(PD.pipeIdentityOf(tag)).toBeNull();
    }
  });

  it("lets a matched query annotate an idle pipeline but never overrule a live log", () => {
    const queries = [{ id: "op-1", tag: "pipeline: orders", state: "failed" }];
    const fresh = PD.pipeFromLog(entry("orders"), tail([batch(T0, 10, 1)]), T0 + 1_000, queries);
    // The connector committed a batch a second ago: the driver's opinion does not paint it red.
    expect(fresh.state).toBe("running");
    expect(fresh.live?.id).toBe("op-1");

    const stale = PD.pipeFromLog(entry("orders"), tail([batch(T0, 10, 1)]), T0 + 3_600_000, queries);
    // Nothing logged for an hour and the driver reports the query failed: now it is evidence.
    expect(stale.state).toBe("error");
    expect(PD.pipeLastError(stale)).toContain("op-1");
  });
});

describe("finding 5 — a value_dropped warning keeps its content", () => {
  it("carries the column, the reason and the row count", () => {
    const p = PD.pipeFromLog(
      entry("orders"),
      tail([
        {
          ts: iso(T0),
          event: "value_dropped",
          column: "payload",
          reason: "timestamps outside the range Arrow can represent",
          rows_in_this_batch: 40000,
        },
      ]),
      T0 + 1_000,
      [],
    );
    expect(p.warnings).toHaveLength(1);
    const [w] = p.warnings;
    expect(w.kind).toBe("value dropped");
    expect(w.text).toContain("payload");
    expect(w.text).toContain("timestamps outside the range");
    expect(w.text).toContain("40,000");
    // The bare kind was all the page used to show.
    expect(w.text).not.toBe("value_dropped");
  });

  it("bounds the text: a connector writes prose, a warnings row is a summary", () => {
    const p = PD.pipeFromLog(
      entry("orders"),
      tail([{ ts: iso(T0), event: "large_transaction", buffered_bytes: 1_000_000, changes: 9, action: "x".repeat(4000) }]),
      T0 + 1_000,
      [],
    );
    expect(p.warnings[0].text.length).toBeLessThanOrEqual(PD.PIPE_MAX_WARNING_CHARS);
    expect(p.warnings[0].text).toContain("9 changes");
  });

  it("renders every warning the connector writes as content, not as its own name", () => {
    // One line per warning kind in `postgres_cdc.rs`, with the fields that file writes.
    const events = [
      { ts: iso(T0), event: "schema_change", table: "public.orders", added_columns: ["region"], removed_columns: [], action: "the stream continues on the schema it started with" },
      { ts: iso(T0), event: "schema_change", warning: "publication publishes no columns for public.orders" },
      { ts: iso(T0), event: "large_transaction", buffered_bytes: 512_000_000, changes: 4_000_000, action: "run it in smaller transactions on the publisher" },
      { ts: iso(T0), event: "value_dropped", column: "amount", reason: "NaN values", rows_in_this_batch: 3 },
      { ts: iso(T0), event: "snapshot_start", tables: [{ table: "public.orders" }], slot: "ox_orders", reason: "the checkpoint's slot is gone" },
    ];
    const p = PD.pipeFromLog(entry("orders"), tail(events), T0 + 1_000, []);
    expect(p.warnings).toHaveLength(events.length);
    for (const w of p.warnings) {
      expect(w.text.length).toBeGreaterThan(0);
      expect(w.text).not.toBe(w.kind.replace(/ /g, "_"));
    }
  });
});

describe("finding 6 — the last batch tracks the newest batch, not the retention window", () => {
  const busy = (count: number) =>
    Array.from({ length: count }, (_, i) => batch(T0 + i * 1_000, 100, i));

  it("numbers batches over the whole tail, not over the retained slice", () => {
    const p = PD.pipeFromLog(entry("orders"), tail(busy(120)), T0 + 120_000, []);
    // The window still bounds what the drawer renders…
    expect(p.batches).toHaveLength(PD.PIPE_MAX_BATCHES);
    expect(p.batchesSeen).toBe(120);
    // …but the newest batch is the 120th, not `#39` — which is what it read forever before.
    expect(p.last.ordinal).toBe(119);
    expect(p.last.ordinal).not.toBe(PD.PIPE_MAX_BATCHES - 1);
  });

  it("moves when the pipeline does", () => {
    const before = PD.pipeFromLog(entry("orders"), tail(busy(120)), T0 + 120_000, []);
    const after = PD.pipeFromLog(entry("orders"), tail(busy(121)), T0 + 121_000, []);
    expect(after.last.ordinal).toBe(before.last.ordinal + 1);
    // The list column shows the newest end LSN: a label that advances with the stream.
    expect(after.lastBatchLabel).toBe(after.last.endLsn);
    expect(after.lastBatchLabel).not.toBe(before.lastBatchLabel);
  });

  it("labels a batch with no LSN by its stamp rather than a window ordinal", () => {
    const p = PD.pipeFromLog(
      entry("orders"),
      tail([{ ts: iso(T0), event: "snapshot_done", table: "public.orders", rows: 10 }]),
      T0 + 1_000,
      [],
    );
    expect(p.lastBatchLabel).toBe(PD.shortTs(iso(T0)));
  });
});

describe("finding 7 — the rate window is driver-clock only", () => {
  const batches = Array.from({ length: 6 }, (_, i) => batch(T0 + i * 10_000, 1_000, i));

  it("reports a rate even when the browser's clock is well ahead of the driver's", () => {
    // A laptop back from sleep: 90 s ahead of the driver, which used to empty the window.
    const skewed = PD.pipeFromLog(entry("orders"), tail(batches), T0 + 50_000 + 90_000, []);
    expect(skewed.rowsPerSec).not.toBeNull();
    expect(skewed.windowRows).toBe(6_000);
  });

  it("gives the same rate whatever the browser clock says", () => {
    const rate = (now: number) => PD.pipeFromLog(entry("orders"), tail(batches), now, []).rowsPerSec;
    expect(rate(T0 + 50_000)).toBe(rate(T0 + 50_000 + 3_600_000));
    expect(rate(T0 + 50_000)).toBe(rate(T0 - 3_600_000));
  });

  it("still measures liveness against the browser clock, floored for skew", () => {
    const live = PD.pipeFromLog(entry("orders"), tail(batches), T0 + 50_000 + 5_000, []);
    expect(live.state).toBe("running");
    const dead = PD.pipeFromLog(entry("orders"), tail(batches), T0 + 50_000 + 3_600_000, []);
    expect(dead.state).toBe("idle");
  });
});

describe("finding 3 — a tail refreshes on every poll, at the cadence the page claims", () => {
  it("is due again one poll interval after the request started, not after it returned", () => {
    // t=0 the tick starts a fetch; it lands at t=50 and is stamped with its *start*.
    const started = { loading: true, at: 0 };
    expect(PD.shouldRefetchTail(started, 10, false)).toBe(false);
    const done = { loading: false, at: 0, doneAt: 50 };
    // The next tick, at t=5000: this used to be a skip, so every tail refreshed every 10 s.
    expect(PD.shouldRefetchTail(done, PD.PIPE_POLL_MS, false)).toBe(true);
    // …and a tick that fires a hair early is still the poll it was meant to be.
    expect(PD.shouldRefetchTail(done, PD.PIPE_POLL_MS - 100, false)).toBe(true);
  });

  it("does not re-fetch inside the interval, and never twice at once", () => {
    expect(PD.shouldRefetchTail({ loading: false, at: 0 }, 1_000, false)).toBe(false);
    expect(PD.shouldRefetchTail({ loading: true, at: 0 }, 60_000, false)).toBe(false);
    expect(PD.shouldRefetchTail({ loading: true, at: 0 }, 60_000, true)).toBe(false);
    // Opening the drawer asks for a tail now.
    expect(PD.shouldRefetchTail({ loading: false, at: 0 }, 1_000, true)).toBe(true);
    expect(PD.shouldRefetchTail(undefined, 0, false)).toBe(true);
  });
});

describe("finding 4 — a repaint keeps the reader where they were", () => {
  /** jsdom has no layout: give the pane the geometry a real one would report. */
  function pane(el: HTMLElement, { scrollTop = 0, clientHeight = 260, scrollHeight = 2000 } = {}) {
    Object.defineProperty(el, "clientHeight", { configurable: true, value: clientHeight });
    Object.defineProperty(el, "scrollHeight", { configurable: true, value: scrollHeight });
    el.scrollTop = scrollTop;
    return el;
  }

  function drawer(scrolls: { body: number; log: number }, geometry = { log: 2000 }) {
    const host = document.createElement("div");
    host.innerHTML = `<div class="drawer-body" data-scroll="body"></div><div class="logwrap" data-scroll="log"></div>`;
    pane(host.querySelector<HTMLElement>('[data-scroll="body"]')!, { scrollTop: scrolls.body, clientHeight: 800, scrollHeight: 4000 });
    pane(host.querySelector<HTMLElement>('[data-scroll="log"]')!, { scrollTop: scrolls.log, scrollHeight: geometry.log });
    return host;
  }

  it("restores the sheet and the log pane after the innerHTML is replaced", () => {
    const host = drawer({ body: 900, log: 400 });
    const snapshot = PD.capturePipeScroll(host);

    // What paintPipeDrawer does every poll.
    host.innerHTML = `<div class="drawer-body" data-scroll="body"></div><div class="logwrap" data-scroll="log"></div>`;
    pane(host.querySelector<HTMLElement>('[data-scroll="body"]')!, { clientHeight: 800, scrollHeight: 4000 });
    pane(host.querySelector<HTMLElement>('[data-scroll="log"]')!, { scrollHeight: 2200 });
    PD.restorePipeScroll(host, snapshot);

    expect(host.querySelector<HTMLElement>('[data-scroll="body"]')!.scrollTop).toBe(900);
    expect(host.querySelector<HTMLElement>('[data-scroll="log"]')!.scrollTop).toBe(400);
  });

  it("re-pins a log pane that was following the newest line to the new bottom", () => {
    // Scrolled to the end of a 2000 px pane showing 260 px.
    const host = drawer({ body: 0, log: 1740 });
    const snapshot = PD.capturePipeScroll(host);
    expect(snapshot.find((s: any) => s.key === "log").pinned).toBe(true);

    host.innerHTML = `<div class="drawer-body" data-scroll="body"></div><div class="logwrap" data-scroll="log"></div>`;
    pane(host.querySelector<HTMLElement>('[data-scroll="body"]')!, { clientHeight: 800, scrollHeight: 4000 });
    // Two more lines arrived.
    pane(host.querySelector<HTMLElement>('[data-scroll="log"]')!, { scrollHeight: 2100 });
    PD.restorePipeScroll(host, snapshot);

    expect(host.querySelector<HTMLElement>('[data-scroll="log"]')!.scrollTop).toBe(2100);
  });

  it("is a no-op on a drawer that is not open", () => {
    expect(PD.capturePipeScroll(null)).toEqual([]);
    expect(() => PD.restorePipeScroll(null, [])).not.toThrow();
  });
});

describe("the rest of what the page believes", () => {
  it("reads a postgres_cdc tail into a source, a position and a slot", () => {
    const p = PD.pipeFromLog(
      entry("orders"),
      tail([
        { ts: iso(T0), event: "snapshot_start", tables: [{ table: "public.orders" }], slot: "ox_orders" },
        { ts: iso(T0 + 1_000), event: "snapshot_done", table: "public.orders", rows: 1_000, duration_ms: 900, consistent_point: "0/1A0" },
        { ts: iso(T0 + 2_000), event: "batch", rows: 12, duration_ms: 30, start_lsn: "0/1A0", end_lsn: "0/1B0" },
        { ts: iso(T0 + 2_100), event: "commit", confirmed_flush_lsn: "0/1B0", sent: true },
        { ts: iso(T0 + 2_200), event: "slot_metrics", slot: "ox_orders", retained_bytes: 4096, lag_bytes: 128, position: "0/1B0" },
      ]),
      T0 + 3_000,
      [],
    );
    expect(p.kind).toBe("postgres_cdc");
    expect(p.source).toBe("PostgresCDC[public.orders slot=ox_orders]");
    expect(p.position).toBe("0/1B0");
    expect(p.slot.retainedBytes).toBe(4096);
    expect(p.state).toBe("running");
  });

  it("tells a retried error from a fatal one, and a fatal one from history", () => {
    const failing = [{ ts: iso(T0), event: "error", message: "replication stream lost", will_retry: true }];
    const retrying = PD.pipeFromLog(entry("orders"), tail(failing), T0 + 1_000, []);
    expect(retrying.retrying).toBe(true);
    expect(retrying.state).toBe("running");
    expect(PD.pipeLastError(retrying)).toContain("still being retried");

    const fatal = PD.pipeFromLog(
      entry("orders"),
      tail([{ ts: iso(T0), event: "error", message: "slot does not exist", will_retry: false }]),
      T0 + 1_000,
      [],
    );
    expect(fatal.state).toBe("error");

    const recovered = PD.pipeFromLog(
      entry("orders"),
      tail([...failing, batch(T0 + 1_000, 5, 1)]),
      T0 + 2_000,
      [],
    );
    expect(recovered.retrying).toBe(false);
    expect(recovered.state).toBe("running");
  });

  it("counts a replayed range once, on the line that carries the numbers", () => {
    const p = PD.pipeFromLog(
      entry("orders"),
      tail([
        { ts: iso(T0), event: "batch", replay: true, start_lsn: "0/10", end_lsn: "0/20" },
        { ts: iso(T0 + 10), event: "batch", rows: 7, duration_ms: 5, start_lsn: "0/10", end_lsn: "0/20" },
      ]),
      T0 + 1_000,
      [],
    );
    expect(p.batches).toHaveLength(1);
    expect(p.batches[0].replayed).toBe(true);
    expect(p.batches[0].rows).toBe(7);
  });

  it("falls back to the log file's mtime when the tail holds no stamped event", () => {
    const p = PD.pipeFromLog(entry("orders", T0), { code: 200, events: [] }, T0 + 1_000, []);
    expect(p.lastEventMs).toBe(T0);
    expect(p.state).toBe("running");
  });

  it("says nothing at all about a pipeline whose log it could not read", () => {
    const p = PD.pipeFromLog(entry("orders"), { code: 401 }, T0, []);
    expect(p.batches).toHaveLength(0);
    expect(p.state).toBe("idle");
    expect(p.logCode).toBe(401);
  });

  it("sorts the newest-written pipeline first", () => {
    const ordered = PD.derivePipelines(
      [entry("a"), entry("b")],
      { a: tail([batch(T0, 1, 1)]), b: tail([batch(T0 + 10_000, 1, 1)]) },
      T0 + 20_000,
      [],
    );
    expect(ordered.map((p: any) => p.name)).toEqual(["b", "a"]);
  });
});
