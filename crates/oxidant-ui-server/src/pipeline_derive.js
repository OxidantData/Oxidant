/* Pipelines: everything that turns a connector's JSONL log into what the page says.
 *
 * This file is *the page's* code — it is spliced into `embedded_ui.html` at the
 * `__PIPELINE_DERIVE_JS__` marker when the binary serves the page, so the served page stays
 * one self-contained file that fetches nothing. It lives here rather than inline because a
 * reducer that decides what an operator believes about a running pipeline has to be testable:
 * `ui/src/lib/pipelineDerive.test.ts` evaluates this exact source and pins the behaviours
 * below, and `static_files.rs` pins the splice.
 *
 * Nothing here touches `document`, `fetch` or page state except `capturePipeScroll` /
 * `restorePipeScroll`, which take the node they operate on. Everything else is a pure
 * function of (log events, driver status, now).
 *
 * Event shapes are the connector's — see `oxidant-streaming/src/postgres_cdc.rs`, which is
 * where these keys are written, and `connector_log.rs`, which adds `ts` and `event` to every
 * line.
 */
var __oxidantPipelines = (function () {
  'use strict';

  /* How often the page polls, and the one cadence its caption and docs quote. The connector
     log tail is rate-limited against this same number: see `shouldRefetchTail`. */
  var PIPE_POLL_MS = 5000;
  /* Slack on that rate limit. The guard compares against the moment a fetch *started*, but a
     browser fires an interval a hair early or late; without slack a tail that started at t=0
     and a tick that lands at t=4998 would skip, and the tail would refresh every 10 s while
     the page said 5 s. */
  var PIPE_POLL_SLACK_MS = 500;
  var PIPE_RATE_WINDOW_MS = 60000;
  var PIPE_MAX_BATCHES = 40;
  var PIPE_LOG_TAIL = 300;
  /* Log tails fetched per poll. One request per pipeline per poll is fine for the handful a
     driver runs; a directory with more says so in the caption instead of quietly stopping. */
  var PIPE_MAX_TAILED = 12;
  /* Floor on "recently enough to call it running". Batch stamps come from the driver's clock
     and `now` from the browser's; a floor this wide makes ordinary skew a non-event. */
  var PIPE_MIN_LIVE_MS = 30000;
  /* Longest connector-warning text the drawer will carry. The connector writes prose (a
     `large_transaction` action is a paragraph), and a warnings row is a summary, not the log
     pane — which is right below it and holds the whole line. */
  var PIPE_MAX_WARNING_CHARS = 240;
  /* Scroll positions within this many pixels of the bottom count as "pinned to the newest
     line" and are re-pinned rather than restored literally. */
  var PIPE_SCROLL_PIN_SLACK_PX = 24;

  var ts = function (v) { var t = v ? new Date(v).getTime() : NaN; return Number.isFinite(t) ? t : 0; };
  var shortTs = function (v) { var t = ts(v); return t ? new Date(t).toISOString().slice(11, 23) : (v ? String(v) : '—'); };
  var fmtRate = function (r) { return r == null ? '—' : (r >= 100 ? Math.round(r).toLocaleString() : r.toFixed(r >= 10 ? 1 : 2)); };
  var fmtInterval = function (ms) { return ms == null ? '—' : (ms < 1000 ? ms + ' ms' : (ms < 90000 ? (ms / 1000).toFixed(ms < 10000 ? 1 : 0) + ' s' : (ms / 60000).toFixed(1) + ' min')); };
  var fmtAgo = function (ms) { return ms == null ? '—' : (ms < 1000 ? 'just now' : fmtInterval(ms) + ' ago'); };
  var num = function (v) { return (typeof v === 'number' && Number.isFinite(v)) ? v : (v == null ? null : (Number.isFinite(Number(v)) ? Number(v) : null)); };

  /* ---------- /api/status: identity, never substring ---------- */

  /* The pipeline a `/api/status` query names — or null, which is the answer for every query
   * the engine actually produces today.
   *
   * `QueryStatus.tag` is a *description*: truncated SQL text, or `DataFrame` for a Connect
   * plan (`oxidant-observability/src/status.rs`). It is not a job tag and not a query name.
   * Matching a pipeline name as a substring of it is therefore a collision waiting to happen:
   * a failing `SELECT count(*) FROM daily_totals` would have marked a healthy pipeline named
   * `daily` stopped, and a long `SELECT ... FROM orders` would have marked a dead one running
   * — which is the failure this page exists to prevent.
   *
   * So a match must be *structural*: the whole tag has to be a streaming identity naming this
   * pipeline and nothing else. No SQL text can be one. Nothing in the engine emits one either
   * — streaming batches never reach the execution store at all (see the module doc on the
   * page) — so this returns null in practice today, deliberately: it is the shape a per-batch
   * observer would have to write for the cross-check to light up, not a guess.
   */
  var PIPE_IDENTITY_RE = /^(?:streaming(?:[ _-]?query)?|pipeline)\s*[:=[]\s*([A-Za-z0-9._-]{1,128})\s*\]?$/i;

  function pipeIdentityOf(tag) {
    var m = PIPE_IDENTITY_RE.exec(String(tag == null ? '' : tag).trim());
    return m ? m[1] : null;
  }

  function statusQueryFor(name, queries) {
    var qs = queries || [];
    for (var i = 0; i < qs.length; i++) {
      if (qs[i] && pipeIdentityOf(qs[i].tag) === name) return qs[i];
    }
    return null;
  }

  /* ---------- Connector warnings ---------- */

  function clamp(text) {
    var s = String(text == null ? '' : text).replace(/\s+/g, ' ').trim();
    return s.length > PIPE_MAX_WARNING_CHARS ? s.slice(0, PIPE_MAX_WARNING_CHARS - 1) + '…' : s;
  }

  /* What a warning row says. Every warning the connector writes carries its content in
   * different keys — `value_dropped` names a column and a reason and counts rows, and carries
   * no `action`/`warning`/`message` at all, so reading only those keys rendered the literal
   * string `value_dropped` and threw away everything an operator needed. Each kind is read on
   * its own terms; the generic keys stay as the fallback for a kind added later.
   */
  function warningText(ev, kind) {
    if (!ev) return kind;
    if (kind === 'value_dropped' && ev.column) {
      var rows = num(ev.rows_in_this_batch);
      return clamp('column ' + ev.column + ': ' + (ev.reason || 'values arrive as NULL') +
        (rows == null ? '' : ' (' + rows.toLocaleString() + ' rows in this batch)'));
    }
    if (kind === 'schema_change' && (ev.added_columns || ev.removed_columns)) {
      var parts = [];
      if (ev.table) parts.push(String(ev.table));
      if (ev.added_columns && ev.added_columns.length) parts.push('+' + [].concat(ev.added_columns).join(', +'));
      if (ev.removed_columns && ev.removed_columns.length) parts.push('-' + [].concat(ev.removed_columns).join(', -'));
      if (ev.action) parts.push(String(ev.action));
      return clamp(parts.join(' · '));
    }
    if (kind === 'large_transaction' && ev.buffered_bytes != null) {
      var changes = num(ev.changes);
      return clamp((changes == null ? '' : changes.toLocaleString() + ' changes, ') +
        num(ev.buffered_bytes).toLocaleString() + ' bytes buffered: ' + (ev.action || 'a batch cannot end inside a transaction'));
    }
    var generic = ev.action || ev.warning || ev.message || ev.reason;
    return clamp(generic || kind);
  }

  /* ---------- One pipeline, folded out of its log tail ---------- */

  function pipeFromLog(entry, log, now, statusQueries) {
    var p = {
      name: entry.name,
      kind: 'connector',
      source: null,
      batches: [],
      warnings: [],
      slot: null,
      commit: null,
      snapshot: null,
      lastError: null,
      lastEventAt: null,
      sizeBytes: entry.sizeBytes,
      logCode: log ? log.code : null,
      logLoading: !!(log && log.loading && !log.events),
    };
    var events = (log && log.code === 200 && log.events) || [];
    // Progress recorded *after* the newest error. Zero means the error is still the last
    // word, which is the difference between "failing" and "hit a bad batch an hour ago".
    var sinceError = 0;
    var replayPending = false;
    var tables = null;
    var slotName = null;

    for (var i = 0; i < events.length; i++) {
      var ev = events[i];
      if (!ev || typeof ev !== 'object' || Array.isArray(ev)) continue;
      var kind = String(ev.event || '');
      var at = ev.ts || null;
      if (at) p.lastEventAt = at;
      switch (kind) {
        case 'batch':
          // A `replay: true` line announces a re-read of a range; the batch line that
          // follows is the one with the numbers on it.
          if (ev.replay) { replayPending = true; break; }
          p.batches.push({
            kind: 'batch', at: at,
            rows: num(ev.rows), durationMs: num(ev.duration_ms),
            startLsn: ev.start_lsn, endLsn: ev.end_lsn, replayed: replayPending,
          });
          replayPending = false;
          sinceError++;
          break;
        case 'snapshot_done':
          p.batches.push({
            kind: 'snapshot', at: at, table: ev.table,
            rows: num(ev.rows), durationMs: num(ev.duration_ms), endLsn: ev.consistent_point,
          });
          p.snapshot = { table: ev.table, consistentPoint: ev.consistent_point, counted: ev.counted !== false };
          sinceError++;
          break;
        case 'snapshot_start':
          if (Array.isArray(ev.tables) && ev.tables.length) {
            tables = ev.tables.map(function (t) { return (t && t.table) || null; }).filter(Boolean);
          }
          if (ev.slot) slotName = ev.slot;
          // A `reason` on a snapshot_start is a restart the operator did not ask for.
          if (ev.reason) p.warnings.push({ at: at, kind: 'snapshot restart', text: clamp(ev.reason) });
          break;
        case 'slot_metrics':
          if (ev.slot) slotName = ev.slot;
          p.slot = {
            at: at, slot: ev.slot,
            retainedBytes: num(ev.retained_bytes),
            lagBytes: num(ev.lag_bytes),
            serverFlushLsn: ev.server_flush_lsn,
            confirmedFlushLsn: ev.confirmed_flush_lsn,
            position: ev.position,
          };
          break;
        case 'commit':
          p.commit = {
            at: at, confirmedFlushLsn: ev.confirmed_flush_lsn,
            // `sent: false` means the position was committed locally but never announced to
            // the publisher — the slot keeps growing until it is.
            sent: ev.sent !== false,
            tablesDone: num(ev.snapshot_tables_done),
          };
          sinceError++;
          break;
        case 'schema_change':
        case 'large_transaction':
        case 'value_dropped':
          p.warnings.push({ at: at, kind: kind.replace(/_/g, ' '), text: warningText(ev, kind) });
          break;
        case 'error':
          p.lastError = { at: at, message: String(ev.message || ev.error || 'error'), willRetry: ev.will_retry !== false };
          sinceError = 0;
          break;
        default:
          // Anything a future connector writes that reads like a failure still lands.
          if (ev.error || /^err|^fatal/i.test(String(ev.level || ev.severity || ''))) {
            p.lastError = { at: at, message: String(ev.error || ev.message || JSON.stringify(ev)), willRetry: true };
            sinceError = 0;
          }
      }
    }

    // A slot or an introspected table list is what a Postgres CDC connector leaves behind;
    // nothing else in the tree writes either.
    if (slotName || tables) {
      p.kind = 'postgres_cdc';
      // Tables come from `snapshot_start`, which a long-running pipeline's tail has usually
      // rolled past. Then the slot alone names the source — better than a `?` standing in for
      // a list this page never saw.
      p.source = 'PostgresCDC[' +
        [tables && tables.length ? tables.join(', ') : null, slotName ? 'slot=' + slotName : null]
          .filter(Boolean).join(' ') + ']';
      p.slotName = slotName;
      p.tables = tables;
    }

    // Ordinals are assigned over every batch the tail held, *before* the retention window is
    // applied. Numbering after the slice pinned the newest batch at `#39` forever on any
    // pipeline busy enough to fill the window — a column that looks like it tracks progress
    // and does not. They are still tail-relative, which is why the newest batch is *labelled*
    // by its end LSN or its stamp (`lastBatchLabel`) and the ordinal stays in the drawer's
    // history, where the caption explains it: the connector log records LSN ranges, not
    // batch ids.
    p.batchesSeen = p.batches.length;
    for (var b = 0; b < p.batches.length; b++) p.batches[b].ordinal = b;
    if (p.batches.length > PIPE_MAX_BATCHES) p.batches = p.batches.slice(-PIPE_MAX_BATCHES);
    p.last = p.batches[p.batches.length - 1] || null;
    p.lastBatchLabel = p.last
      ? (p.last.endLsn ? String(p.last.endLsn) : (p.last.at ? shortTs(p.last.at) : '#' + p.last.ordinal))
      : null;

    var gaps = [];
    for (var g = 1; g < p.batches.length; g++) {
      var gap = ts(p.batches[g].at) - ts(p.batches[g - 1].at);
      if (gap > 0) gaps.push(gap);
    }
    p.triggerMs = gaps.length ? gaps.slice().sort(function (x, y) { return x - y; })[Math.floor(gaps.length / 2)] : null;

    // The rate window is anchored on the newest batch's *own* stamp, not on the browser's
    // clock. Both ends of the comparison are then the driver's clock: a browser 90 s ahead
    // (a laptop back from sleep, a driver without NTP) used to empty the window and blank
    // "Rows / s (60 s)" for every pipeline with nothing on the page saying why. Liveness
    // still compares the two clocks — it has to — and floors the window at PIPE_MIN_LIVE_MS.
    var anchor = p.last ? ts(p.last.at) : 0;
    var window = anchor ? p.batches.filter(function (x) { return ts(x.at) >= anchor - PIPE_RATE_WINDOW_MS; }) : [];
    if (window.length) {
      var rows = window.reduce(function (a, x) { return a + (x.rows || 0); }, 0);
      // The span the window actually covered, floored at the batches' own runtime so one
      // batch does not divide by ~0 and report an absurd rate.
      var span = Math.max(
        ts(window[window.length - 1].at) - ts(window[0].at),
        window.reduce(function (a, x) { return a + (x.durationMs || 0); }, 0),
        1000);
      p.rowsPerSec = rows / (span / 1000);
      p.windowRows = rows;
    } else {
      p.rowsPerSec = null;
      p.windowRows = 0;
    }

    // Freshness falls back to the log file's own mtime, which is the same driver clock and
    // is present even when the tail window holds no timestamped event.
    var lastAt = ts(p.lastEventAt) || entry.modifiedMs || 0;
    p.lastEventMs = lastAt || null;
    p.idleMs = lastAt ? Math.max(0, now - lastAt) : null;
    var liveWindow = Math.max(3 * (p.triggerMs || 0), PIPE_MIN_LIVE_MS);
    var fresh = lastAt > 0 && p.idleMs <= liveWindow;

    // The driver cross-check annotates; it never overrules the connector log. It is consulted
    // only where the log has no opinion — no fatal error, and nothing logged recently enough
    // to call the pipeline live — so a stale or mis-attributed `/api/status` row cannot paint
    // a committing pipeline red.
    p.live = statusQueryFor(p.name, statusQueries);
    p.retrying = !!(p.lastError && p.lastError.willRetry && sinceError === 0);
    if (p.lastError && !p.lastError.willRetry && sinceError === 0) p.state = 'error';
    else if (fresh) p.state = 'running';
    else if (p.live && p.live.state === 'failed') p.state = 'error';
    else if (p.live && p.live.state === 'running') p.state = 'running';
    else p.state = 'idle';

    p.position = (p.commit && p.commit.confirmedFlushLsn) ||
      (p.slot && (p.slot.confirmedFlushLsn || p.slot.position)) ||
      (p.last && p.last.endLsn) || null;
    return p;
  }

  function derivePipelines(list, logs, now, statusQueries) {
    var pipelines = (list || []).map(function (entry) {
      return pipeFromLog(entry, (logs || {})[entry.name], now, statusQueries);
    });
    pipelines.sort(function (a, b) {
      return (b.lastEventMs || 0) - (a.lastEventMs || 0) || a.name.localeCompare(b.name);
    });
    return pipelines;
  }

  // The text a failure banner shows. The connector log is the only place the message exists.
  function pipeLastError(p) {
    if (p.lastError) {
      return p.lastError.message + (p.lastError.willRetry
        ? '\n\nThe connector marked this retryable and nothing has progressed since, so the batch is still being retried.'
        : '\n\nThe connector marked this fatal: the pipeline has stopped rather than skip data.');
    }
    if (p.live && p.live.state === 'failed') return 'The driver reports this query as failed (' + p.live.id + ').';
    return null;
  }

  /* ---------- Polling and repaint mechanics ---------- */

  /* Whether a connector-log tail is due. `cached.at` is the moment the in-flight fetch
   * *started*, never the moment it returned: re-stamping on completion made the next tick
   * land inside the guard (a fetch that started at 0 and returned at 50 pushed the guard to
   * 5050, and the tick at 5000 skipped), so every tail refreshed every other tick — 10 s,
   * while the caption and the docs said 5 s.
   */
  function shouldRefetchTail(cached, now, force) {
    if (!cached) return true;
    if (cached.loading) return false;
    if (force) return true;
    return (now - (cached.at || 0)) >= (PIPE_POLL_MS - PIPE_POLL_SLACK_MS);
  }

  /* Scroll positions of every `[data-scroll]` pane under `root`, so a repaint can put the
   * reader back where they were. The drawer repaints on every poll — that is how the log
   * tail and the batch history stay live — and replacing its innerHTML dropped the reader to
   * the top of the sheet mid-read, on exactly the workflow the drawer exists for.
   *
   * A pane already at the bottom is recorded as pinned and re-pinned to the *new* bottom
   * instead of to its old offset: that is what "follow the log" means, and it is what the
   * Observability log pane already does.
   */
  function capturePipeScroll(root) {
    if (!root || !root.querySelectorAll) return [];
    var out = [];
    var panes = root.querySelectorAll('[data-scroll]');
    for (var i = 0; i < panes.length; i++) {
      var el = panes[i];
      out.push({
        key: el.getAttribute('data-scroll'),
        top: el.scrollTop,
        pinned: el.scrollTop > 0 && el.scrollTop + el.clientHeight >= el.scrollHeight - PIPE_SCROLL_PIN_SLACK_PX,
      });
    }
    return out;
  }

  function restorePipeScroll(root, snapshot) {
    if (!root || !root.querySelectorAll || !snapshot || !snapshot.length) return;
    var panes = root.querySelectorAll('[data-scroll]');
    for (var i = 0; i < panes.length; i++) {
      var el = panes[i];
      var key = el.getAttribute('data-scroll');
      for (var j = 0; j < snapshot.length; j++) {
        if (snapshot[j].key !== key) continue;
        el.scrollTop = snapshot[j].pinned ? el.scrollHeight : snapshot[j].top;
        break;
      }
    }
  }

  return {
    PIPE_POLL_MS: PIPE_POLL_MS,
    PIPE_POLL_SLACK_MS: PIPE_POLL_SLACK_MS,
    PIPE_RATE_WINDOW_MS: PIPE_RATE_WINDOW_MS,
    PIPE_MAX_BATCHES: PIPE_MAX_BATCHES,
    PIPE_LOG_TAIL: PIPE_LOG_TAIL,
    PIPE_MAX_TAILED: PIPE_MAX_TAILED,
    PIPE_MIN_LIVE_MS: PIPE_MIN_LIVE_MS,
    PIPE_MAX_WARNING_CHARS: PIPE_MAX_WARNING_CHARS,
    ts: ts,
    shortTs: shortTs,
    fmtRate: fmtRate,
    fmtInterval: fmtInterval,
    fmtAgo: fmtAgo,
    num: num,
    pipeIdentityOf: pipeIdentityOf,
    statusQueryFor: statusQueryFor,
    warningText: warningText,
    pipeFromLog: pipeFromLog,
    derivePipelines: derivePipelines,
    pipeLastError: pipeLastError,
    shouldRefetchTail: shouldRefetchTail,
    capturePipeScroll: capturePipeScroll,
    restorePipeScroll: restorePipeScroll,
  };
})();
