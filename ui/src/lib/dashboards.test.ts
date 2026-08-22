/**
 * The client half of the dashboard API: the exact verbs, paths and bodies the Rust side in
 * `crates/oxidant-ui-server/src/dashboards.rs` validates. A PUT where the server expects a
 * PATCH, or a dropped `refreshIntervalSecs: null`, is a bug no render test would catch.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  appendToLayout,
  dashboardsApi,
  fmtRelativeMs,
  newWidgetId,
  WIDGET_TYPES,
  WIDGET_TYPE_LABELS,
  type WidgetSpec,
} from "@/lib/dashboards";

const fetchMock = vi.fn<typeof fetch>();

beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal("fetch", fetchMock);
});

function respond(body: unknown, status = 200) {
  fetchMock.mockResolvedValue(
    new Response(status === 204 ? null : JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    })
  );
}

/** `[method, path, parsed body]` of the single call made. */
function lastCall(): [string, string, unknown] {
  const [path, init] = fetchMock.mock.calls[0] as [string, RequestInit];
  return [
    init.method ?? "GET",
    path,
    init.body ? JSON.parse(init.body as string) : undefined,
  ];
}

describe("dashboardsApi", () => {
  it("unwraps the list envelope", async () => {
    respond({ dashboards: [{ id: "d1", name: "Sales" }] });
    await expect(dashboardsApi.list()).resolves.toEqual([{ id: "d1", name: "Sales" }]);
    expect(lastCall()[1]).toBe("/api/dashboards");
  });

  it("creates with POST and the document body", async () => {
    respond({ id: "d1" }, 201);
    await dashboardsApi.create({ name: "Sales", widgets: [], layout: [] });
    expect(lastCall()).toEqual([
      "POST",
      "/api/dashboards",
      { name: "Sales", widgets: [], layout: [] },
    ]);
  });

  it("updates with PATCH, sending only the fields it was given", async () => {
    respond({ id: "d1" });
    await dashboardsApi.update("d1", { name: "Renamed" });
    expect(lastCall()).toEqual(["PATCH", "/api/dashboards/d1", { name: "Renamed" }]);
  });

  it("keeps an explicit null so auto-refresh can be turned off", async () => {
    respond({ id: "d1" });
    await dashboardsApi.update("d1", { refreshIntervalSecs: null });
    // An omitted key means "leave it alone"; only a literal null clears the interval.
    expect(lastCall()[2]).toEqual({ refreshIntervalSecs: null });
  });

  it("deletes with DELETE and no body", async () => {
    respond(null, 204);
    await dashboardsApi.remove("d1");
    expect(lastCall()).toEqual(["DELETE", "/api/dashboards/d1", undefined]);
  });

  it("surfaces the server's error message rather than a status code", async () => {
    respond({ error: "widget[0] has unknown type \"funnel\"" }, 400);
    await expect(
      dashboardsApi.create({ name: "Sales" })
    ).rejects.toThrow(/unknown type/);
  });
});

describe("layout helpers", () => {
  const widget = (id: string, type: WidgetSpec["type"] = "bar"): WidgetSpec => ({
    id,
    type,
    title: "",
    sql: "SELECT 1",
    options: {},
  });

  it("places a new widget below everything already on the grid", () => {
    const layout = [
      { i: "w1", x: 0, y: 0, w: 6, h: 8 },
      { i: "w2", x: 6, y: 4, w: 6, h: 10 },
    ];
    const placed = appendToLayout(layout, widget("w3"));
    expect(placed.i).toBe("w3");
    expect(placed.x).toBe(0);
    // Below the lowest bottom edge (4 + 10), not the lowest top edge.
    expect(placed.y).toBe(14);
  });

  it("sizes a new widget by its type", () => {
    expect(appendToLayout([], widget("w1", "kpi"))).toMatchObject({ w: 3, h: 4 });
    expect(appendToLayout([], widget("w1", "bar"))).toMatchObject({ w: 6, h: 8 });
  });

  it("picks the first free widget id", () => {
    expect(newWidgetId([])).toBe("w1");
    expect(newWidgetId([widget("w1"), widget("w2")])).toBe("w3");
    // A gap is reused rather than skipped — ids only have to be unique per document.
    expect(newWidgetId([widget("w1"), widget("w3")])).toBe("w2");
  });
});

describe("widget type metadata", () => {
  it("labels every type the API accepts", () => {
    for (const type of WIDGET_TYPES) {
      expect(WIDGET_TYPE_LABELS[type], `${type} needs a label`).toBeTruthy();
    }
    expect(Object.keys(WIDGET_TYPE_LABELS)).toHaveLength(WIDGET_TYPES.length);
  });
});

describe("fmtRelativeMs", () => {
  it("degrades from minutes to a date", () => {
    const now = Date.UTC(2026, 7, 22, 12, 0, 0);
    expect(fmtRelativeMs(now - 10_000, now)).toBe("just now");
    expect(fmtRelativeMs(now - 5 * 60_000, now)).toBe("5m ago");
    expect(fmtRelativeMs(now - 3 * 3_600_000, now)).toBe("3h ago");
    expect(fmtRelativeMs(now - 2 * 86_400_000, now)).toBe("2d ago");
    expect(fmtRelativeMs(now - 400 * 86_400_000, now)).toContain("2025");
  });
});
