/* Catalog rail: everything that decides what the browser tree shows and what a click inserts.
 *
 * This file is *the page's* code — it is spliced into `embedded_ui.html` at the
 * `__CATALOG_RAIL_JS__` marker when the binary serves the page, so the served page stays one
 * self-contained file that fetches nothing. It lives here rather than inline for the same
 * reason `pipeline_derive.js` does: a lazily-loaded tree that a filter narrows *across levels
 * that are not all loaded* is decision-making, not markup, and it has to be testable.
 * `ui/src/lib/catalogRail.test.ts` evaluates this exact source and pins the behaviours below;
 * `static_files.rs` pins the splice and the wiring.
 *
 * Nothing here touches `document` or `fetch`. `railRows` is a pure function of
 * (cache, expanded, filter) and is the only thing that decides which rows exist, at what
 * depth, and which placeholder stands in for a level that is loading, failed or empty. The
 * page paints what it returns and — crucially — *fetches what it returns*: a row for an
 * expanded level that is still `idle` is what kicks that level's request. That inversion is
 * why typing cannot crawl the warehouse: a filter descends only into levels that are already
 * `ready`, so it never opens a level nobody opened.
 *
 * Not quite "a filter starts no requests", which is the claim this file used to make. A level
 * the user *had* expanded and that has not loaded still emits its `idle` placeholder, and if
 * the filter matches that node by name the row stays on screen and `pendingLoads` asks for it.
 * The bound is the honest one: a filter can only ask for levels the unfiltered paint was going
 * to ask for anyway, and it usually asks for fewer.
 *
 * The shapes are the REST catalog API's — see `oxidant-connect/src/rest.rs`:
 *   GET /api/v1/catalogs                                    -> { catalogs: [{name, isCurrent}] }
 *   GET /api/v1/catalogs/{c}/namespaces                     -> { namespaces: ["default", …] }
 *   GET /api/v1/catalogs/{c}/tables?namespace=…              -> { tables: [{name, type}] }
 *   GET /api/v1/catalogs/{c}/tables/{t}/columns?namespace=…  -> { columns: [{name, type}] }
 *   GET /api/v1/catalogs/autocomplete?prefix=…               -> { suggestions: [{kind,name,qualified}] }
 */
var __oxidantCatalog = (function () {
  'use strict';

  /* Where the rail's own state lives. Versioned in the key, like the notebook's: a shape
     change gets a new key rather than a migration in a page with no build step. */
  var CAT_PREFS_KEY = 'oxidant.catalogRail.v1';
  /* Cap on the remembered expansion set. A user who browses a warehouse for an afternoon
     would otherwise grow an unbounded blob in localStorage that is never read again — every
     key in it is re-fetched on the next load, so a stale one costs a request, not just bytes. */
  var CAT_MAX_REMEMBERED = 200;
  /* Rows a `Preview` runs. The number is in the SQL the user can read in the results pane, so
     it is a constant here rather than a magic literal in three places. */
  var CAT_PREVIEW_ROWS = 100;
  /* Longest suggestion list the filter box will show under itself. */
  var CAT_MAX_SUGGESTIONS = 12;

  /* ---------- routes ----------
   * Every segment is encoded. A namespace is dot-joined on the wire (the server splits it
   * again), so it is one query parameter and not a path, and a table whose name contains a
   * slash is a path segment that must not become two.
   */
  function catalogsUrl() {
    return '/api/v1/catalogs';
  }
  function namespacesUrl(catalog) {
    return '/api/v1/catalogs/' + encodeURIComponent(catalog) + '/namespaces';
  }
  function tablesUrl(catalog, namespace) {
    return '/api/v1/catalogs/' + encodeURIComponent(catalog) +
      '/tables?namespace=' + encodeURIComponent(namespace);
  }
  function columnsUrl(catalog, namespace, table) {
    return '/api/v1/catalogs/' + encodeURIComponent(catalog) +
      '/tables/' + encodeURIComponent(table) +
      '/columns?namespace=' + encodeURIComponent(namespace);
  }
  function autocompleteUrl(prefix) {
    return '/api/v1/catalogs/autocomplete?prefix=' + encodeURIComponent(prefix);
  }

  /* ---------- nodes ----------
   * A node carries its whole coordinate, not a parent pointer: the rail is repainted from
   * scratch on every change, and a click handler that had to walk back up to build
   * `catalog.schema.table` is how the wrong name gets inserted.
   */
  function makeCatalog(name, isCurrent) {
    return { kind: 'catalog', name: name, catalog: name, isCurrent: !!isCurrent };
  }
  function makeNamespace(catalog, namespace) {
    return { kind: 'namespace', name: namespace, catalog: catalog, namespace: namespace };
  }
  function makeTable(catalog, namespace, name, tableType) {
    return {
      kind: 'table',
      name: name,
      catalog: catalog,
      namespace: namespace,
      table: name,
      tableType: tableType || 'TABLE',
    };
  }
  function makeColumn(catalog, namespace, table, name, dataType) {
    return {
      kind: 'column',
      name: name,
      catalog: catalog,
      namespace: namespace,
      table: table,
      dataType: dataType || '',
    };
  }

  /* The identity a cache entry and a remembered expansion are keyed by. NUL is the separator
     because a quoted identifier may contain anything else — a schema literally called `a.b`
     and a namespace `a` holding a table `b` would key alike under a dot. Written as an escape,
     never as a raw byte: this file is spliced into an HTML document. */
  function nodeKey(node) {
    if (!node) return '';
    var sep = '\u0000';
    return [node.kind, node.catalog || '', node.namespace || '', node.table || ''].join(sep);
  }

  /* What expanding this node loads. `null` is a leaf — a column, and nothing else. */
  function childKind(kind) {
    if (kind === 'catalog') return 'namespace';
    if (kind === 'namespace') return 'table';
    if (kind === 'table') return 'column';
    return null;
  }

  function childrenUrl(node) {
    if (!node) return null;
    if (node.kind === 'catalog') return namespacesUrl(node.catalog);
    if (node.kind === 'namespace') return tablesUrl(node.catalog, node.namespace);
    if (node.kind === 'table') return columnsUrl(node.catalog, node.namespace, node.table);
    return null;
  }

  /* What a level with nothing in it says. "Empty catalog → say so, not a blank box", and the
     sentence has to name the level: "No tables" under a schema and "No schemas" under a
     catalog are different facts about the warehouse. */
  function emptyChildMessage(kind) {
    if (kind === 'catalog') return 'No schemas in this catalog';
    if (kind === 'namespace') return 'No tables in this schema';
    if (kind === 'table') return 'No columns';
    return 'Nothing here';
  }

  /* What a level is called when a sentence has to name it — in a failure, where the level that
     did not load is the only thing the reader can act on. Keyed by the *parent's* kind, like
     `emptyChildMessage`, plus `catalogs` for the root. */
  function childLabel(kind) {
    if (kind === 'catalog') return 'schemas';
    if (kind === 'namespace') return 'tables';
    if (kind === 'table') return 'columns';
    return 'catalogs';
  }

  /* ---------- identifiers ----------
   * The rail inserts SQL, so it quotes like SQL. Anything that would not come back out of the
   * parser as the name that went in gets backticks (Spark's quote, which is what this engine's
   * parser takes), and a backtick inside a name is doubled. Without this a schema called
   * `my-schema` inserts as three tokens and a subtraction, and a table called `Orders` inserts
   * as one that resolves to `orders`.
   */
  var PLAIN_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;
  /* Keywords that are genuinely reserved — the ones a bare name lands on as a syntax error or,
     worse, is quietly read as part of the grammar.
     This is the parser's own reserved sets, not the whole grammar: sqlparser's
     `RESERVED_FOR_TABLE_ALIAS`, `RESERVED_FOR_COLUMN_ALIAS` and `RESERVED_FOR_IDENTIFIER`,
     which are exactly the words that cannot appear where the rail puts a name. The rail
     inserts into a query it does not control, so "where the rail puts a name" includes the
     alias slot right after a table — `FROM orders except` is a set operator with a missing
     right-hand side, not a table called `except`.
     Everything the parser leaves alone stays bare, which is the other half of the rule:
     `schema`, `column`, `comment`, `date` and `default` — the name of the standard namespace —
     insert as themselves. A false positive here is not free, it is what every inserted name
     looks like, so the list is derived rather than guessed:
     `crates/oxidant-sql/tests/catalog_rail_reserved_words.rs` reads it back out of this file
     and asks the Databricks dialect whether it is still both complete and minimal. */
  var RESERVED = ('all analyze and anti array as asc asof between by case cluster connect '
    + 'create cross delete desc distinct distribute drop else end except exclude exists '
    + 'explain fetch for format from full global grant group having in inner insert intersect '
    + 'interval into is join lateral left like limit match_condition match_recognize minus '
    + 'natural not null offset on open or order outer output partition pivot prewhere primary '
    + 'qualify returning right sample select semi set settings sort start struct table '
    + 'tablesample then top trim union unpivot update using values view when where window '
    + 'with').split(' ');

  function needsQuoting(name) {
    var s = String(name == null ? '' : name);
    if (!PLAIN_IDENT.test(s)) return true;
    /* A *bare* identifier does not survive the parser unchanged: DataFusion lowercases it
       (`sql_parser.enable_ident_normalization` defaults to true and nothing in this engine
       overrides it), while the catalog routes hand back the warehouse's real, case-preserved
       names. So `Orders` inserted bare arrives at the planner as `orders` — a different table,
       or none — and `Preview` turns that into a recorded failed statement. The rule is
       therefore not "is this a legal bare identifier" but "would the round trip be the
       identity": anything with an upper-case letter in it gets backticks, which are not
       normalized. */
    if (s !== s.toLowerCase()) return true;
    return RESERVED.indexOf(s.toLowerCase()) >= 0;
  }

  function quoteIdent(name) {
    var s = String(name == null ? '' : name);
    if (!needsQuoting(s)) return s;
    return '`' + s.replace(/`/g, '``') + '`';
  }

  /* The dotted path to a node, each part quoted on its own. A namespace is already dotted on
     the wire (`a.b`), and each of its parts is an identifier — quoting the whole thing would
     produce one nested name that resolves to nothing. */
  function nodeParts(node) {
    if (!node) return [];
    var parts = [];
    if (node.catalog) parts.push(node.catalog);
    if (node.kind === 'catalog') return parts;
    if (node.namespace) {
      String(node.namespace).split('.').forEach(function (p) {
        if (p) parts.push(p);
      });
    }
    if (node.kind === 'namespace') return parts;
    if (node.table) parts.push(node.table);
    if (node.kind === 'table') return parts;
    return parts;
  }

  function qualifiedName(node) {
    return nodeParts(node).map(quoteIdent).join('.');
  }

  /* What a click puts at the cursor. A column inserts as its bare name: it is being typed
     into a SELECT list whose FROM already names the table, and `cat.schema.tbl.col` there is
     a resolution error in every engine this page talks to. */
  function insertTextFor(node) {
    if (!node) return '';
    if (node.kind === 'column') return quoteIdent(node.name);
    return qualifiedName(node);
  }

  function previewSql(node) {
    return 'SELECT * FROM ' + qualifiedName(node) + ' LIMIT ' + CAT_PREVIEW_ROWS;
  }

  /* ---------- insertion ----------
   * Splice `snippet` over [selStart, selEnd) and say where the caret lands. The spacing rules
   * are the whole point: inserting a table name after `FROM` needs a space in front of it and
   * inserting a column after `t.` must not have one.
   */
  function insertAtCursor(value, selStart, selEnd, snippet) {
    var text = String(value == null ? '' : value);
    var start = Math.max(0, Math.min(text.length, selStart == null ? text.length : selStart));
    var end = Math.max(start, Math.min(text.length, selEnd == null ? start : selEnd));
    var before = text.slice(0, start);
    var after = text.slice(end);
    // No space after an opening bracket, a comma, a dot or existing whitespace — and none at
    // the very start of the buffer.
    var lead = before && !/[\s(,.]$/.test(before) ? ' ' : '';
    // …and none before a closer, a separator or whitespace that is already there.
    var trail = after && !/^[\s),;.]/.test(after) ? ' ' : '';
    var head = before + lead + snippet;
    return { text: head + trail + after, cursor: head.length };
  }

  /* Where `insertAtCursor` should splice, given whether the target actually has a caret.
   *
   * A `<textarea>` nobody has focused reports `selectionStart === 0`, which is the same number
   * a caret deliberately parked in front of the query reports — the DOM has no way to tell
   * them apart, so the page has to remember which textareas have been focused and say. Without
   * that, the *first* click on a catalog name (the likeliest first interaction with the rail)
   * prepends: `spark_catalog.sales.orders SELECT 1 AS hello`.
   *
   * `null` rather than a length, because "no caret" means the end of a buffer this function
   * has not been handed — `insertAtCursor` already reads `null` as end-of-buffer, and it is
   * the one that knows how long the text is.
   */
  function caretRange(hasCaret, selStart, selEnd) {
    if (!hasCaret) return { start: null, end: null };
    return { start: selStart, end: selEnd };
  }

  /* ---------- filtering ---------- */
  function normalizeFilter(filter) {
    return String(filter == null ? '' : filter).trim().toLowerCase();
  }

  function nameMatches(name, needle) {
    if (!needle) return true;
    return String(name == null ? '' : name).toLowerCase().indexOf(needle) >= 0;
  }

  /* The filter box doubles as a jump box: a dotted prefix is a path, and the autocomplete
     endpoint is the only thing that can resolve one against levels this rail has not loaded.
     A bare word is a substring filter over what *is* loaded and asks the server nothing —
     `/api/v1/catalogs/autocomplete` matches by prefix, so it would answer a different question
     than the box is asking. */
  function wantsSuggestions(filter) {
    var raw = String(filter == null ? '' : filter).trim();
    return raw.indexOf('.') >= 0 && raw.length >= 2;
  }

  /* A suggestion inserts what its kind means, on the same rule as a tree node: a column is a
     bare name. The endpoint's `qualified` for a column is `catalog.namespace.column` — the
     table is missing from it — so it is never the thing to insert. */
  function suggestionInsertText(suggestion) {
    if (!suggestion) return '';
    if (suggestion.kind === 'column') return quoteIdent(suggestion.name);
    return String(suggestion.qualified || suggestion.name || '')
      .split('.')
      .filter(function (p) { return p.length > 0; })
      .map(quoteIdent)
      .join('.');
  }

  /* Which levels the tree has to open to show a suggestion. Returned as the nodes themselves
     so the page can load each one in order; it deliberately stops at the table, because
     revealing a column means loading a table's columns and the suggestion already told the
     user the column exists. */
  function revealTargets(suggestion) {
    if (!suggestion) return [];
    var qualified = String(suggestion.qualified || '');
    var parts = qualified.split('.').filter(function (p) { return p.length > 0; });
    if (parts.length < 2) return [];
    var catalog = parts[0];
    var out = [makeCatalog(catalog)];
    if (suggestion.kind === 'namespace') return out;
    // `catalog.ns[.ns…].table` for a table, and for a column `qualified` names no table at all
    // (`catalog.ns.column`) — so in both cases everything between the catalog and the last part
    // is the namespace, and the schema is the deepest level that can be revealed.
    var tail = parts.slice(1, parts.length - 1);
    if (!tail.length) return out;
    out.push(makeNamespace(catalog, tail.join('.')));
    return out;
  }

  /* ---------- the tree ----------
   * `cache` is `{ catalogs: entry, byKey: { <nodeKey>: entry } }` where an entry is
   * `{ state: 'idle'|'loading'|'ready'|'error', items?: node[], error?: string, code?: number }`.
   * `expanded` is a plain object used as a set of node keys.
   *
   * Rows come back flat, each carrying its own depth, because the page renders them into one
   * list: a nested DOM would have to be diffed to keep a filter from collapsing the scroll
   * position on every keystroke.
   */
  function railRows(cache, expanded, filter) {
    var needle = normalizeFilter(filter);
    var open = expanded || {};
    var root = (cache && cache.catalogs) || { state: 'idle' };
    var rows = [];

    if (root.state !== 'ready') {
      rows.push(levelPlaceholder(root, 0, null, 'catalogs'));
      return rows;
    }
    var catalogs = root.items || [];
    if (!catalogs.length) {
      rows.push({ type: 'empty', depth: 0, scope: 'catalogs', message: 'No catalogs' });
      return rows;
    }

    catalogs.forEach(function (node) {
      var sub = subtree(cache, open, needle, node, 0);
      if (sub.keep) rows.push.apply(rows, sub.rows);
    });
    if (needle && !rows.length) {
      rows.push({
        type: 'empty',
        depth: 0,
        scope: 'filter',
        message: 'Nothing loaded matches “' + String(filter).trim() + '”',
      });
    }
    return rows;
  }

  /* One node and everything under it that survives the filter.
   *
   * The two branches that matter:
   *  - **No filter.** Every node is kept, and its children exist only if the user expanded it.
   *  - **A filter.** Children are also computed for a level that is *already loaded*, so a
   *    match three levels down surfaces without a click — and a level that is not loaded is
   *    left alone, so typing never expands a level nobody opened. A node survives if it
   *    matches itself or something under it did.
   *
   * `open[key]` is deliberately three-valued. `true` is the user's own expansion and `false`
   * is "closed", which is not the same as absent: a filter paints the path to a match open
   * whatever the tree remembers, so without a way to say *closed* the chevron on a
   * filter-revealed row has nothing to write that the next paint would not immediately undo.
   * The page keeps those `false`s in a filter-scoped overlay it throws away, which is why they
   * never reach `saveRailPrefs`.
   */
  function subtree(cache, open, needle, node, depth) {
    var key = nodeKey(node);
    var kind = childKind(node.kind);
    var state = open[key];
    var isOpen = state === true;
    var isClosed = state === false;
    var entry = kind ? ((cache.byKey || {})[key] || { state: 'idle' }) : null;
    var selfMatch = nameMatches(node.name, needle);

    var childRows = [];
    // A level pinned closed under a filter is still descended into: whether anything below it
    // matched is what decides this row exists at all, and a match that took its own parent off
    // the screen would be worse than the collapse the user asked for. The rows just are not
    // rendered — see `expanded` below.
    var descend = kind && (isOpen || (needle && entry.state === 'ready'));
    if (descend && entry.state === 'ready') {
      (entry.items || []).forEach(function (child) {
        var sub = subtree(cache, open, needle, child, depth + 1);
        if (sub.keep) childRows.push.apply(childRows, sub.rows);
      });
    }

    var keep = !needle || selfMatch || childRows.length > 0;
    if (!keep) return { keep: false, rows: [] };

    // A filter that surfaced a descendant shows the path to it open, whatever the user's own
    // expansion says — otherwise the match is a row with no visible parent. Closing the row is
    // the one thing that outranks that, or the chevron on it is decoration.
    var revealed = !isClosed && !isOpen && !!needle && childRows.length > 0;
    var expanded = !isClosed && (isOpen || revealed);

    var rows = [{
      type: 'node',
      node: node,
      key: key,
      depth: depth,
      expandable: !!kind,
      expanded: expanded,
      // Showing children the *filter* found, not children the user opened. The page reads this
      // to keep a click on such a row out of the persisted expansion set.
      revealed: revealed,
      match: !!needle && selfMatch,
    }];

    if (kind && isOpen && entry.state !== 'ready') {
      rows.push(levelPlaceholder(entry, depth + 1, key, node.kind));
    } else if (kind && isOpen && entry.state === 'ready' && !(entry.items || []).length) {
      rows.push({
        type: 'empty',
        depth: depth + 1,
        scope: node.kind,
        key: key,
        message: emptyChildMessage(node.kind),
      });
    } else if (expanded) {
      rows.push.apply(rows, childRows);
    }
    return { keep: true, rows: rows };
  }

  /* The row that stands in for a level with no items to show yet. It carries the entry's own
     `state`, because `idle` and `loading` read identically to a human and mean opposite things
     to the page: `idle` is "nothing has asked for this level", and painting it is what asks. */
  function levelPlaceholder(entry, depth, key, scope) {
    if (entry.state === 'error') {
      return {
        type: 'error',
        depth: depth,
        key: key,
        scope: scope,
        message: 'Could not load ' + childLabel(scope),
        detail: entry.error || '',
        code: entry.code || null,
      };
    }
    return {
      type: 'loading',
      depth: depth,
      key: key,
      scope: scope,
      state: entry.state === 'loading' ? 'loading' : 'idle',
    };
  }

  /* Every row the page must turn into a request: an expanded level that nothing has asked for
     yet. Derived from the rows rather than from the tree, so what is fetched is exactly what is
     on screen — and a level already in flight is not asked for twice.

     The root is not in here. It has no node key, and it is loaded once when the rail mounts. */
  function pendingLoads(rows) {
    var out = [];
    (rows || []).forEach(function (row) {
      if (row.type === 'loading' && row.state === 'idle' && row.key) out.push(row.key);
    });
    return out;
  }

  /* ---------- persisted rail state ----------
   * `storage` is passed in rather than reached for: this file is evaluated in a test with no
   * DOM, and a page whose localStorage throws (Safari private browsing) must still render.
   */
  function loadRailPrefs(storage) {
    var prefs = { open: true, expanded: {} };
    try {
      var raw = storage && storage.getItem(CAT_PREFS_KEY);
      if (!raw) return prefs;
      var d = JSON.parse(raw);
      if (d && typeof d.open === 'boolean') prefs.open = d.open;
      if (d && Array.isArray(d.expanded)) {
        d.expanded.slice(0, CAT_MAX_REMEMBERED).forEach(function (k) {
          if (typeof k === 'string' && k) prefs.expanded[k] = true;
        });
      }
    } catch (e) { /* an unreadable preference is not a broken rail */ }
    return prefs;
  }

  function saveRailPrefs(storage, prefs) {
    try {
      if (!storage) return;
      storage.setItem(CAT_PREFS_KEY, JSON.stringify({
        open: !!(prefs && prefs.open),
        expanded: Object.keys((prefs && prefs.expanded) || {}).slice(0, CAT_MAX_REMEMBERED),
      }));
    } catch (e) { /* same */ }
  }

  return {
    CAT_PREFS_KEY: CAT_PREFS_KEY,
    CAT_MAX_REMEMBERED: CAT_MAX_REMEMBERED,
    CAT_PREVIEW_ROWS: CAT_PREVIEW_ROWS,
    CAT_MAX_SUGGESTIONS: CAT_MAX_SUGGESTIONS,
    catalogsUrl: catalogsUrl,
    namespacesUrl: namespacesUrl,
    tablesUrl: tablesUrl,
    columnsUrl: columnsUrl,
    autocompleteUrl: autocompleteUrl,
    makeCatalog: makeCatalog,
    makeNamespace: makeNamespace,
    makeTable: makeTable,
    makeColumn: makeColumn,
    nodeKey: nodeKey,
    childKind: childKind,
    childrenUrl: childrenUrl,
    emptyChildMessage: emptyChildMessage,
    childLabel: childLabel,
    needsQuoting: needsQuoting,
    quoteIdent: quoteIdent,
    nodeParts: nodeParts,
    qualifiedName: qualifiedName,
    insertTextFor: insertTextFor,
    previewSql: previewSql,
    insertAtCursor: insertAtCursor,
    caretRange: caretRange,
    normalizeFilter: normalizeFilter,
    nameMatches: nameMatches,
    wantsSuggestions: wantsSuggestions,
    suggestionInsertText: suggestionInsertText,
    revealTargets: revealTargets,
    railRows: railRows,
    pendingLoads: pendingLoads,
    loadRailPrefs: loadRailPrefs,
    saveRailPrefs: saveRailPrefs,
  };
})();
