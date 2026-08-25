/**
 * The catalog rail's tree logic, under test.
 *
 * `crates/oxidant-ui-server/src/catalog_rail.js` decides what the embedded console's catalog
 * browser shows: which rows survive a filter over a tree that is only partly loaded, which
 * levels the page is thereby told to fetch, and what a click on each kind of node inserts. The
 * console is a single hand-written HTML file with no build step and no module loader, so the
 * file is spliced into the page by the server (`static_files.rs`) — and imported here as source
 * and evaluated, so it is the *same* code both places. A copy would drift.
 *
 * The tests are grouped by the hazard each one exists for, not by function.
 */
import { describe, expect, it } from "vitest";
// Vite hands `?raw` imports over as the file's text — the same bytes the server splices.
import railSource from "../../../crates/oxidant-ui-server/src/catalog_rail.js?raw";

/* eslint-disable @typescript-eslint/no-explicit-any */
const CR = new Function(`${railSource}\nreturn __oxidantCatalog;`)() as any;

type Entry = { state: string; items?: any[]; error?: string; code?: number };
type Cache = { catalogs: Entry; byKey: Record<string, Entry> };

const catalog = (name: string, isCurrent = false) => CR.makeCatalog(name, isCurrent);
const namespace = (c: string, ns: string) => CR.makeNamespace(c, ns);
const table = (c: string, ns: string, t: string) => CR.makeTable(c, ns, t, "TABLE");
const column = (c: string, ns: string, t: string, n: string, ty: string) =>
  CR.makeColumn(c, ns, t, n, ty);

const ready = (items: any[]): Entry => ({ state: "ready", items });

/** A warehouse with two catalogs, one of which is loaded down to its columns. */
function warehouse(): { cache: Cache; nodes: Record<string, any> } {
  const lake = catalog("lake");
  const spark = catalog("spark_catalog", true);
  const sales = namespace("spark_catalog", "sales");
  const dflt = namespace("spark_catalog", "default");
  const orders = table("spark_catalog", "sales", "orders");
  const cache: Cache = {
    catalogs: ready([spark, lake]),
    byKey: {
      [CR.nodeKey(spark)]: ready([dflt, sales]),
      [CR.nodeKey(sales)]: ready([orders]),
      [CR.nodeKey(orders)]: ready([
        column("spark_catalog", "sales", "orders", "order_id", "bigint"),
        column("spark_catalog", "sales", "orders", "total", "double"),
      ]),
    },
  };
  return { cache, nodes: { spark, lake, sales, dflt, orders } };
}

/** `<depth><kind or row type>:<name>` — the shape of a paint, as one readable line each. */
const shape = (rows: any[]) =>
  rows.map((r) =>
    r.type === "node"
      ? `${r.depth} ${r.node.kind}:${r.node.name}${r.match ? " *" : ""}`
      : `${r.depth} ${r.type}:${r.message ?? r.state ?? ""}`,
  );

describe("a filter narrows what is loaded and asks the server for nothing", () => {
  it("surfaces a match three levels down and opens the path to it, without expansion", () => {
    const { cache } = warehouse();
    // Nothing is expanded. A filter still finds `total`, because every level between it and
    // the root is already in the cache.
    const rows = CR.railRows(cache, {}, "total");
    expect(shape(rows)).toEqual([
      "0 catalog:spark_catalog",
      "1 namespace:sales",
      "2 table:orders",
      "3 column:total *",
    ]);
    // Only the match is marked. The three rows above it are the path, not hits — the rail
    // dims them for exactly that reason.
    expect(rows.filter((r: any) => r.match)).toHaveLength(1);
    // …and each ancestor is painted open even though the user opened nothing.
    expect(rows.slice(0, 3).every((r: any) => r.expanded)).toBe(true);
  });

  it("does not descend into a level that is not loaded, so typing cannot start a fetch", () => {
    const { cache, nodes } = warehouse();
    // `lake` has never been opened: its schemas are unknown, and one of them may well be
    // called `sales`. The filter leaves it alone rather than going to look.
    const rows = CR.railRows(cache, {}, "sales");
    expect(shape(rows)).toEqual(["0 catalog:spark_catalog", "1 namespace:sales *"]);
    expect(CR.pendingLoads(rows)).toEqual([]);
    // The *only* thing that asks for `lake`'s schemas is expanding it.
    const opened = CR.railRows(cache, { [CR.nodeKey(nodes.lake)]: true }, "");
    expect(CR.pendingLoads(opened)).toEqual([CR.nodeKey(nodes.lake)]);
  });

  it("drops a branch with no match in it, expanded or not", () => {
    const { cache, nodes } = warehouse();
    const expanded = { [CR.nodeKey(nodes.spark)]: true, [CR.nodeKey(nodes.lake)]: true };
    // Unfiltered, both catalogs and both of `spark_catalog`'s schemas are on screen.
    expect(shape(CR.railRows(cache, expanded, ""))).toEqual([
      "0 catalog:spark_catalog",
      "1 namespace:default",
      "1 namespace:sales",
      "0 catalog:lake",
      "1 loading:idle",
    ]);
    // Filtered to `default`, `sales` goes and so does `lake` — a filter is allowed to hide a
    // branch the user opened, which is the whole point of one.
    expect(shape(CR.railRows(cache, expanded, "default"))).toEqual([
      "0 catalog:spark_catalog",
      "1 namespace:default *",
    ]);
  });

  it("says so when nothing loaded matches, rather than painting an empty rail", () => {
    const { cache } = warehouse();
    expect(shape(CR.railRows(cache, {}, "nonesuch"))).toEqual([
      '0 empty:Nothing loaded matches “nonesuch”',
    ]);
  });

  it("matches on a substring, case-insensitively, and ignores surrounding space", () => {
    const { cache } = warehouse();
    for (const typed of ["ORDER_", "  order_  ", "der_i"]) {
      expect(shape(CR.railRows(cache, {}, typed)).slice(-1)).toEqual(["3 column:order_id *"]);
    }
  });
});

describe("the rows are the request queue", () => {
  it("asks only for a level nothing has asked for yet, never for one in flight", () => {
    const { nodes } = warehouse();
    const key = CR.nodeKey(nodes.spark);
    const expanded = { [key]: true };
    const idle: Cache = { catalogs: ready([nodes.spark]), byKey: {} };
    // An expanded level with no entry at all is the one thing the page must go and get.
    expect(CR.pendingLoads(CR.railRows(idle, expanded, ""))).toEqual([key]);
    // The moment the request is out, the same paint must not queue it a second time — this is
    // the guard that keeps a repaint-per-response from becoming a request storm.
    const inFlight: Cache = {
      catalogs: ready([nodes.spark]),
      byKey: { [key]: { state: "loading" } },
    };
    const rows = CR.railRows(inFlight, expanded, "");
    expect(shape(rows)).toEqual(["0 catalog:spark_catalog", "1 loading:loading"]);
    expect(CR.pendingLoads(rows)).toEqual([]);
  });

  it("leaves the root out of the queue: it has no key, and the mount loads it", () => {
    const rows = CR.railRows({ catalogs: { state: "idle" }, byKey: {} }, {}, "");
    expect(rows).toHaveLength(1);
    expect(rows[0].type).toBe("loading");
    expect(rows[0].key).toBeFalsy();
    expect(CR.pendingLoads(rows)).toEqual([]);
  });
});

describe("a level with nothing in it says which level", () => {
  it("names the level rather than painting a blank box", () => {
    const spark = catalog("spark_catalog");
    const empty = namespace("spark_catalog", "staging");
    const bare = table("spark_catalog", "staging", "t");
    const cache: Cache = {
      catalogs: ready([spark]),
      byKey: {
        [CR.nodeKey(spark)]: ready([empty]),
        [CR.nodeKey(empty)]: ready([bare]),
        [CR.nodeKey(bare)]: ready([]),
      },
    };
    const expanded = {
      [CR.nodeKey(spark)]: true,
      [CR.nodeKey(empty)]: true,
      [CR.nodeKey(bare)]: true,
    };
    expect(shape(CR.railRows(cache, expanded, ""))).toEqual([
      "0 catalog:spark_catalog",
      "1 namespace:staging",
      "2 table:t",
      "3 empty:No columns",
    ]);
    // And each level has its own sentence: "no tables" is not what an empty catalog means.
    expect(CR.emptyChildMessage("catalog")).toBe("No schemas in this catalog");
    expect(CR.emptyChildMessage("namespace")).toBe("No tables in this schema");
  });

  it("distinguishes an engine with no catalogs from one that could not be asked", () => {
    const none = CR.railRows({ catalogs: ready([]), byKey: {} }, {}, "");
    expect(none).toHaveLength(1);
    expect(none[0]).toMatchObject({ type: "empty", scope: "catalogs", message: "No catalogs" });

    const down = CR.railRows(
      { catalogs: { state: "error", error: "The engine did not answer." }, byKey: {} },
      {},
      "",
    );
    expect(down).toHaveLength(1);
    // No key: the page paints this one as the loud whole-rail ErrorState, and its Retry
    // reloads the root rather than one branch.
    expect(down[0]).toMatchObject({
      type: "error",
      message: "Could not load catalogs",
      detail: "The engine did not answer.",
    });
    expect(down[0].key).toBeFalsy();
  });

  it("keeps a failed branch's key, so Retry re-asks that branch and not the world", () => {
    const spark = catalog("spark_catalog");
    const key = CR.nodeKey(spark);
    const rows = CR.railRows(
      { catalogs: ready([spark]), byKey: { [key]: { state: "error", error: "boom", code: 500 } } },
      { [key]: true },
      "",
    );
    expect(rows[1]).toMatchObject({
      type: "error",
      key,
      message: "Could not load schemas",
      code: 500,
    });
  });
});

describe("a node's identity survives names that look like paths", () => {
  it("does not confuse a schema called `a.b` with a table `b` in schema `a`", () => {
    const dotted = CR.nodeKey(namespace("c", "a.b"));
    const nested = CR.nodeKey(table("c", "a", "b"));
    expect(dotted).not.toEqual(nested);
  });

  it("keys a node by its coordinate, not by its display name", () => {
    // Two tables called `orders` in two schemas are two cache entries.
    expect(CR.nodeKey(table("c", "sales", "orders"))).not.toEqual(
      CR.nodeKey(table("c", "returns", "orders")),
    );
  });
});

describe("what a click inserts", () => {
  it("inserts a table fully qualified and a column bare", () => {
    const t = table("spark_catalog", "sales", "orders");
    expect(CR.insertTextFor(t)).toBe("spark_catalog.sales.orders");
    expect(CR.previewSql(t)).toBe("SELECT * FROM spark_catalog.sales.orders LIMIT 100");
    // A column is typed into a SELECT list whose FROM already names the table;
    // `spark_catalog.sales.orders.total` there does not resolve in any engine this page talks to.
    expect(CR.insertTextFor(column("spark_catalog", "sales", "orders", "total", "double"))).toBe(
      "total",
    );
  });

  it("quotes each part on its own, and leaves the ordinary case alone", () => {
    // A multi-level namespace is already dotted on the wire; quoting it whole would produce
    // one nested name that resolves to nothing.
    expect(CR.qualifiedName(table("c", "a.b", "t"))).toBe("c.a.b.t");
    // `default` is what the standard namespace is called and Spark does not reserve it —
    // backticks there would be on nearly every name the rail ever inserts.
    expect(CR.quoteIdent("default")).toBe("default");
    // All lower-case: an upper-case letter is itself a reason to quote — see the case rule
    // below, and `T9` bare would reach the planner as `t9`.
    for (const bare of ["orders", "_x", "t9", "schema", "comment", "date"]) {
      expect(CR.quoteIdent(bare)).toBe(bare);
    }
    // …and the ones that really would not parse.
    expect(CR.quoteIdent("my-schema")).toBe("`my-schema`");
    expect(CR.quoteIdent("2024")).toBe("`2024`");
    expect(CR.quoteIdent("order")).toBe("`order`");
    expect(CR.quoteIdent("a b")).toBe("`a b`");
    // A backtick inside a name is doubled, not dropped: the alternative ends the quote early
    // and the rest of the name becomes SQL.
    expect(CR.quoteIdent("we`ird")).toBe("`we``ird`");
  });

  it("quotes the set operators and join modifiers, not just the obvious keywords", () => {
    // A schema called `minus` or a table called `anti` is not exotic, and bare it is not a
    // name: `FROM orders except` is a set operator with nothing on its right-hand side. The
    // list is the Databricks dialect's own reserved sets, and
    // `crates/oxidant-sql/tests/catalog_rail_reserved_words.rs` asks the parser whether it is
    // still complete — this pins the shape a reader would look for.
    for (const kw of [
      "except",
      "intersect",
      "minus",
      "anti",
      "semi",
      "natural",
      "lateral",
      "window",
      "qualify",
      "pivot",
      "interval",
      "struct",
    ]) {
      expect(CR.quoteIdent(kw)).toBe(`\`${kw}\``);
    }
    // Through the paths a click takes, not just through `quoteIdent`.
    expect(CR.insertTextFor(table("lake", "minus", "anti"))).toBe("lake.`minus`.`anti`");
    expect(CR.previewSql(table("lake", "minus", "anti"))).toBe(
      "SELECT * FROM lake.`minus`.`anti` LIMIT 100",
    );
    // …and the other half of the rule: a word the parser leaves alone stays bare, or every
    // name the rail inserts wears backticks.
    for (const bare of ["default", "schema", "column", "comment", "date"]) {
      expect(CR.quoteIdent(bare)).toBe(bare);
    }
  });

  it("quotes a mixed-case name, which bare would be lowercased into a different table", () => {
    // The engine parses with `enable_ident_normalization` at DataFusion's default of `true`,
    // so a bare `Orders` reaches the planner as `orders`. The catalog routes, meanwhile, hand
    // back the warehouse's real names. Insert one bare and it resolves to something else or to
    // nothing — and `Preview` makes that a recorded failed statement rather than a typo.
    for (const mixed of ["Orders", "MyTable", "ORDERS", "orderID"]) {
      expect(CR.quoteIdent(mixed)).toBe(`\`${mixed}\``);
      expect(CR.needsQuoting(mixed)).toBe(true);
    }
    // Lower-case names round-trip through the parser unchanged and stay bare — the whole point
    // of the rule is that it is about the round trip, not about the character class.
    expect(CR.needsQuoting("orders")).toBe(false);

    // Both paths a click takes: the insert, and the Preview statement.
    const t = table("Sales", "Warehouse", "Orders");
    expect(CR.insertTextFor(t)).toBe("`Sales`.`Warehouse`.`Orders`");
    expect(CR.previewSql(t)).toBe(
      "SELECT * FROM `Sales`.`Warehouse`.`Orders` LIMIT 100",
    );
    // A mixed-case column inserts bare-but-quoted for the same reason.
    expect(CR.insertTextFor(column("Sales", "Warehouse", "Orders", "orderID", "bigint"))).toBe(
      "`orderID`",
    );
    // …and so does a suggestion, which takes the same quoting path.
    expect(
      CR.suggestionInsertText({
        kind: "table",
        name: "Orders",
        qualified: "Sales.Warehouse.Orders",
      }),
    ).toBe("`Sales`.`Warehouse`.`Orders`");
  });
});

describe("where an insertion lands", () => {
  const at = (text: string, i: number, snippet: string) => CR.insertAtCursor(text, i, i, snippet);

  it("adds the space a name needs and none that it does not", () => {
    expect(at("SELECT * FROM ", 14, "db.t")).toEqual({
      text: "SELECT * FROM db.t",
      cursor: 18,
    });
    // Completing a qualifier must not produce `t. col`.
    expect(at("t.", 2, "total")).toEqual({ text: "t.total", cursor: 7 });
    // Nor `count( total )`.
    expect(at("count()", 6, "total")).toEqual({ text: "count(total)", cursor: 11 });
    expect(at("a,", 2, "b")).toEqual({ text: "a,b", cursor: 3 });
    // An empty buffer gets no leading space.
    expect(at("", 0, "x")).toEqual({ text: "x", cursor: 1 });
    // A word butting straight up against the caret does.
    expect(at("SELECT", 6, "x")).toEqual({ text: "SELECT x", cursor: 8 });
  });

  it("replaces a selection and leaves the caret after what it inserted", () => {
    const r = CR.insertAtCursor("SELECT foo FROM t", 7, 10, "bar");
    expect(r.text).toBe("SELECT bar FROM t");
    expect(r.text.slice(0, r.cursor)).toBe("SELECT bar");
  });

  it("clamps a caret the caller got wrong rather than corrupting the buffer", () => {
    expect(CR.insertAtCursor("abc", 99, 99, "x").text).toBe("abc x");
    expect(CR.insertAtCursor("abc", -5, -5, "x").text).toBe("x abc");
    // An inverted range is read as a caret, not as a reversed slice.
    expect(CR.insertAtCursor("abc", 2, 1, "x").text).toBe("ab x c");
  });

  it("appends into a textarea that has never had a caret, rather than prepending", () => {
    // A `<textarea>` nobody has clicked into reports `selectionStart === 0`, and so does one
    // whose caret is parked in front of the query. Only the page can tell those apart, so it
    // says which it has — and the first click on a catalog name, with the editor still holding
    // its default `SELECT 1 AS hello`, is exactly the case that has no caret.
    const noCaret = CR.caretRange(false, 0, 0);
    expect(noCaret).toEqual({ start: null, end: null });
    expect(
      CR.insertAtCursor(
        "SELECT 1 AS hello",
        noCaret.start,
        noCaret.end,
        "spark_catalog.sales.orders",
      ).text,
    ).toBe("SELECT 1 AS hello spark_catalog.sales.orders");

    // …and a real caret at 0 still means the front of the buffer.
    const atZero = CR.caretRange(true, 0, 0);
    expect(atZero).toEqual({ start: 0, end: 0 });
    expect(CR.insertAtCursor("SELECT 1 AS hello", atZero.start, atZero.end, "orders").text).toBe(
      "orders SELECT 1 AS hello",
    );

    // A selection is passed through untouched — the caret flag is about existence, not extent.
    expect(CR.caretRange(true, 7, 10)).toEqual({ start: 7, end: 10 });
  });
});

describe("the filter box doubles as a path box", () => {
  it("sends a dotted path to autocomplete and a bare word to nobody", () => {
    // `/api/v1/catalogs/autocomplete` matches by *prefix*; the box filters by substring. A
    // bare word sent there would answer a different question than the one being asked.
    expect(CR.wantsSuggestions("orders")).toBe(false);
    expect(CR.wantsSuggestions("sales.ord")).toBe(true);
    expect(CR.wantsSuggestions("a.")).toBe(true);
    expect(CR.wantsSuggestions(".")).toBe(false);
    expect(CR.wantsSuggestions("")).toBe(false);
  });

  it("inserts a column suggestion by name: the endpoint's `qualified` omits its table", () => {
    // The REST route builds `catalog.namespace.column` for a column — the table is not in it,
    // so inserting `qualified` would name something that does not exist.
    expect(
      CR.suggestionInsertText({
        kind: "column",
        name: "total",
        qualified: "spark_catalog.sales.total",
      }),
    ).toBe("total");
    expect(
      CR.suggestionInsertText({
        kind: "table",
        name: "my-orders",
        qualified: "spark_catalog.sales.my-orders",
      }),
    ).toBe("spark_catalog.sales.`my-orders`");
  });

  it("reveals the levels above a suggestion, and stops where the path stops", () => {
    const forTable = CR.revealTargets({
      kind: "table",
      name: "orders",
      qualified: "spark_catalog.sales.orders",
    });
    expect(forTable.map((n: any) => `${n.kind}:${n.name}`)).toEqual([
      "catalog:spark_catalog",
      "namespace:sales",
    ]);
    // A column's `qualified` names no table, so the schema is the deepest thing that can be
    // opened — and opening it is what puts the column's table on screen.
    expect(
      CR.revealTargets({
        kind: "column",
        name: "total",
        qualified: "spark_catalog.sales.total",
      }).map((n: any) => `${n.kind}:${n.name}`),
    ).toEqual(["catalog:spark_catalog", "namespace:sales"]);
    // A catalog suggestion has nothing above it to reveal.
    expect(CR.revealTargets({ kind: "catalog", name: "lake", qualified: "lake" })).toEqual([]);
  });
});

describe("the routes", () => {
  it("encodes every segment, so an awkward name is one segment and not two", () => {
    expect(CR.namespacesUrl("my catalog")).toBe("/api/v1/catalogs/my%20catalog/namespaces");
    // A namespace is dot-joined in a query parameter — the server splits it again — and a
    // table is a path segment, which a slash in its name must not split.
    expect(CR.tablesUrl("c", "a.b")).toBe("/api/v1/catalogs/c/tables?namespace=a.b");
    expect(CR.columnsUrl("c", "a.b", "od/ds")).toBe(
      "/api/v1/catalogs/c/tables/od%2Fds/columns?namespace=a.b",
    );
    expect(CR.autocompleteUrl("a.b c")).toBe("/api/v1/catalogs/autocomplete?prefix=a.b%20c");
  });

  it("derives a node's children URL from the node, so no caller builds one by hand", () => {
    expect(CR.childrenUrl(catalog("c"))).toBe(CR.namespacesUrl("c"));
    expect(CR.childrenUrl(namespace("c", "ns"))).toBe(CR.tablesUrl("c", "ns"));
    expect(CR.childrenUrl(table("c", "ns", "t"))).toBe(CR.columnsUrl("c", "ns", "t"));
    // A column is a leaf: there is no level under it to ask for.
    expect(CR.childrenUrl(column("c", "ns", "t", "x", "int"))).toBeNull();
    expect(CR.childKind("column")).toBeNull();
  });
});

describe("the remembered rail state", () => {
  const fakeStorage = () => {
    const map: Record<string, string> = {};
    return {
      map,
      getItem: (k: string) => (k in map ? map[k] : null),
      setItem: (k: string, v: string) => {
        map[k] = v;
      },
    };
  };

  it("defaults to open, and round-trips what was expanded", () => {
    const storage = fakeStorage();
    expect(CR.loadRailPrefs(storage)).toEqual({ open: true, expanded: {} });
    const key = CR.nodeKey(catalog("spark_catalog"));
    CR.saveRailPrefs(storage, { open: false, expanded: { [key]: true } });
    expect(CR.loadRailPrefs(storage)).toEqual({ open: false, expanded: { [key]: true } });
  });

  it("caps what it remembers: every stale key costs a request on the next load", () => {
    const storage = fakeStorage();
    const expanded: Record<string, boolean> = {};
    for (let i = 0; i < CR.CAT_MAX_REMEMBERED + 50; i++) expanded[`k${i}`] = true;
    CR.saveRailPrefs(storage, { open: true, expanded });
    expect(Object.keys(CR.loadRailPrefs(storage).expanded)).toHaveLength(CR.CAT_MAX_REMEMBERED);
  });

  it("renders on a browser whose localStorage throws, and on garbage", () => {
    const hostile = {
      getItem() {
        throw new Error("SecurityError");
      },
      setItem() {
        throw new Error("SecurityError");
      },
    };
    expect(CR.loadRailPrefs(hostile)).toEqual({ open: true, expanded: {} });
    expect(() => CR.saveRailPrefs(hostile, { open: true, expanded: {} })).not.toThrow();
    expect(CR.loadRailPrefs(null)).toEqual({ open: true, expanded: {} });

    const storage = fakeStorage();
    storage.map[CR.CAT_PREFS_KEY] = "{not json";
    expect(CR.loadRailPrefs(storage)).toEqual({ open: true, expanded: {} });
    storage.map[CR.CAT_PREFS_KEY] = JSON.stringify({ open: "yes", expanded: { a: 1 } });
    expect(CR.loadRailPrefs(storage)).toEqual({ open: true, expanded: {} });
  });
});
