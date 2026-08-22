/**
 * Render smoke tests for the card that hosts every widget: one per widget type, plus the
 * loading / error / empty states around them.
 *
 * The statement API is stubbed, so what is exercised is the whole browser-side chain —
 * `WidgetCard` → `useWidgetQuery` → TanStack Query → `WidgetBody` → the renderer for that
 * type. `table` and `kpi` render for real, into the DOM. The chart types render through a
 * stubbed `echarts-for-react` (jsdom has no canvas) that publishes the option it was handed,
 * so the assertion is that the right series reached ECharts; that the option then *draws* is
 * covered against real ECharts in `lib/chartOption.test.ts`.
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StatementDoc, StatementResult } from "@/lib/api";
import { WIDGET_TYPES, type WidgetSpec, type WidgetType } from "@/lib/dashboards";
import WidgetCard from "@/components/dashboard/WidgetCard";

const runStatement = vi.fn<(sql: string) => Promise<StatementDoc>>();
const fetchResult = vi.fn<(id: string) => Promise<StatementResult>>();

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return {
    ...actual,
    runStatement: (sql: string) => runStatement(sql),
    api: {
      ...actual.api,
      statements: {
        ...actual.api.statements,
        result: (id: string) => fetchResult(id),
      },
    },
  };
});

vi.mock("echarts-for-react/lib/core", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => (
    <div data-testid="echart" data-option={JSON.stringify(option)} />
  ),
}));

const SALES: StatementResult = {
  schema: {
    fields: [
      { name: "region", type: "Utf8" },
      { name: "revenue", type: "Float64" },
    ],
  },
  rows: [
    { region: "west", revenue: 120.5 },
    { region: "east", revenue: 80 },
  ],
  rowCount: 2,
  truncated: false,
};

function widget(type: WidgetType, over: Partial<WidgetSpec> = {}): WidgetSpec {
  return {
    id: "w1",
    type,
    title: `${type} widget`,
    sql: `SELECT region, revenue FROM sales -- ${type}`,
    options: {},
    ...over,
  };
}

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

function succeed(result: StatementResult = SALES) {
  runStatement.mockResolvedValue({
    statementId: "s1",
    sql: "…",
    status: "succeeded",
    submittedAtMs: 0,
    rowCount: result.rowCount,
  });
  fetchResult.mockResolvedValue(result);
}

/** The option the stubbed chart component received, once it has rendered. */
async function renderedOption(): Promise<Record<string, never>> {
  const node = await screen.findByTestId("echart");
  return JSON.parse(node.getAttribute("data-option") ?? "{}");
}

beforeEach(() => {
  runStatement.mockReset();
  fetchResult.mockReset();
});

describe("every widget type renders with stubbed data", () => {
  it.each(WIDGET_TYPES)("%s", async (type) => {
    succeed();
    render(<WidgetCard widget={widget(type)} />, { wrapper });

    expect(await screen.findByText(`${type} widget`)).toBeInTheDocument();
    if (type === "table") {
      // The real table: headers from the schema, a cell from the data.
      expect(await screen.findByText("region")).toBeInTheDocument();
      expect(screen.getByText("west")).toBeInTheDocument();
      expect(screen.getByText("120.5")).toBeInTheDocument();
    } else if (type === "kpi") {
      // First numeric column of the first row.
      expect(await screen.findByText("120.5")).toBeInTheDocument();
      expect(screen.getByText("revenue")).toBeInTheDocument();
    } else {
      const option = await renderedOption();
      const series = option.series as unknown as { name: string; data: unknown[] }[];
      expect(series.length).toBeGreaterThan(0);
      expect(JSON.stringify(series)).toContain("120.5");
    }
    expect(runStatement).toHaveBeenCalledWith(widget(type).sql);
  });
});

describe("the card's states", () => {
  it("shows the query running before the result arrives", async () => {
    let release!: (doc: StatementDoc) => void;
    runStatement.mockReturnValue(
      new Promise<StatementDoc>((resolve) => {
        release = resolve;
      })
    );
    fetchResult.mockResolvedValue(SALES);
    render(<WidgetCard widget={widget("table")} />, { wrapper });

    expect(screen.getByText("Running…")).toBeInTheDocument();
    release({
      statementId: "s1",
      sql: "…",
      status: "succeeded",
      submittedAtMs: 0,
    });
    expect(await screen.findByText("west")).toBeInTheDocument();
  });

  it("shows the engine's error message when the statement fails", async () => {
    runStatement.mockResolvedValue({
      statementId: "s1",
      sql: "…",
      status: "failed",
      submittedAtMs: 0,
      error: "table `nope` not found",
    });
    render(<WidgetCard widget={widget("bar")} />, { wrapper });
    expect(await screen.findByText("table `nope` not found")).toBeInTheDocument();
    expect(screen.queryByTestId("echart")).not.toBeInTheDocument();
  });

  it("explains an unplottable result instead of drawing an empty chart", async () => {
    succeed({
      schema: {
        fields: [
          { name: "a", type: "Utf8" },
          { name: "b", type: "Utf8" },
        ],
      },
      rows: [{ a: "x", b: "y" }],
      rowCount: 1,
      truncated: false,
    });
    render(<WidgetCard widget={widget("line")} />, { wrapper });
    expect(await screen.findByText(/No numeric column/)).toBeInTheDocument();
    expect(screen.queryByTestId("echart")).not.toBeInTheDocument();
  });

  it("falls back to the type name when the widget has no title", async () => {
    succeed();
    render(<WidgetCard widget={widget("kpi", { title: "" })} />, { wrapper });
    expect(await screen.findByText("KPI counter")).toBeInTheDocument();
  });

  it("re-runs the statement when the card's Refresh is used", async () => {
    succeed();
    render(<WidgetCard widget={widget("kpi")} />, { wrapper });
    await screen.findByText("120.5");
    expect(runStatement).toHaveBeenCalledTimes(1);

    await userEvent.click(screen.getByRole("button", { name: /Refresh/ }));
    await waitFor(() => expect(runStatement).toHaveBeenCalledTimes(2));
  });

  it("offers Edit and Remove only in edit mode", async () => {
    succeed();
    const onRemove = vi.fn();
    const { rerender } = render(<WidgetCard widget={widget("kpi")} />, { wrapper });
    await screen.findByText("120.5");
    expect(screen.queryByRole("button", { name: /Remove/ })).not.toBeInTheDocument();

    rerender(<WidgetCard widget={widget("kpi")} editing onRemove={onRemove} />);
    await userEvent.click(screen.getByRole("button", { name: /Remove/ }));
    expect(onRemove).toHaveBeenCalledWith(expect.objectContaining({ id: "w1" }));
    expect(screen.queryByRole("button", { name: /Refresh/ })).not.toBeInTheDocument();
  });
});

describe("the table widget", () => {
  it("sorts NULLs last however the column is ordered", async () => {
    succeed({
      schema: {
        fields: [
          { name: "region", type: "Utf8" },
          { name: "revenue", type: "Float64" },
        ],
      },
      rows: [
        { region: "west", revenue: 10 },
        { region: "east", revenue: null },
        { region: "north", revenue: 30 },
      ],
      rowCount: 3,
      truncated: false,
    });
    render(<WidgetCard widget={widget("table")} />, { wrapper });
    const header = await screen.findByText("revenue");

    const regionOrder = () =>
      screen
        .getAllByRole("row")
        .slice(1)
        .map((row) => within(row).getAllByRole("cell")[0].textContent);

    await userEvent.click(header);
    const first = regionOrder();
    expect(first).toHaveLength(3);
    expect(first.slice(0, 2)).toEqual(expect.arrayContaining(["west", "north"]));
    // Absence is not a smallest value: it sits at the bottom.
    expect(first[2]).toBe("east");

    await userEvent.click(header);
    const second = regionOrder();
    expect(second.slice(0, 2)).toEqual(first.slice(0, 2).reverse());
    // …and it stays at the bottom when the order flips.
    expect(second[2]).toBe("east");
  });
});
