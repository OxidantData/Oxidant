import { useEffect, useState } from "react";
import { api, type CatalogInfo, type CatalogTable, type CatalogColumn } from "@/lib/api";

interface CatalogNode {
  catalog: string;
  namespace: string;
  table?: string;
}

export default function CatalogPage() {
  const [catalogs, setCatalogs] = useState<CatalogInfo[]>([]);
  const [namespaces, setNamespaces] = useState<string[]>([]);
  const [tables, setTables] = useState<CatalogTable[]>([]);
  const [columns, setColumns] = useState<CatalogColumn[]>([]);
  const [selected, setSelected] = useState<CatalogNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.catalogs
      .list()
      .then((d) => setCatalogs(d.catalogs))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, []);

  async function selectCatalog(catalog: string) {
    setSelected({ catalog, namespace: "" });
    setNamespaces([]);
    setTables([]);
    setColumns([]);
    setLoading(true);
    setError(null);
    try {
      const ns = await api.catalogs.namespaces(catalog);
      setNamespaces(ns.namespaces);
      if (ns.namespaces.length > 0) {
        await selectNamespace(catalog, ns.namespaces[0]);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  async function selectNamespace(catalog: string, namespace: string) {
    setSelected((s) => ({ catalog, namespace, table: s?.table }));
    setTables([]);
    setColumns([]);
    setLoading(true);
    setError(null);
    try {
      const t = await api.catalogs.tables(catalog, namespace);
      setTables(t.tables);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  async function selectTable(catalog: string, namespace: string, table: string) {
    setSelected({ catalog, namespace, table });
    setLoading(true);
    setError(null);
    try {
      const c = await api.catalogs.columns(catalog, namespace, table);
      setColumns(c.columns);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  function copyName() {
    if (!selected || !selected.namespace) return;
    const qualified = selected.table
      ? `${selected.catalog}.${selected.namespace}.${selected.table}`
      : `${selected.catalog}.${selected.namespace}`;
    navigator.clipboard.writeText(qualified).catch(() => {});
  }

  return (
    <div className="grid h-full gap-4 lg:grid-cols-[200px_200px_1fr_300px]">
      <div className="oxidant-card flex flex-col overflow-hidden">
        <h2 className="mb-2 text-sm font-semibold">Catalogs</h2>
        <div className="-mr-2 flex-1 overflow-y-auto pr-2">
          {catalogs.map((c) => (
            <button
              key={c.name}
              onClick={() => selectCatalog(c.name)}
              className={`block w-full rounded px-2 py-1 text-left text-sm ${
                selected?.catalog === c.name ? "bg-accent/20 text-accent" : "hover:bg-surface"
              }`}
            >
              {c.name}
              {c.isCurrent && (
                <span className="ml-1 text-xs text-muted">(current)</span>
              )}
            </button>
          ))}
        </div>
      </div>

      <div className="oxidant-card flex flex-col overflow-hidden">
        <h2 className="mb-2 text-sm font-semibold">Namespaces</h2>
        <div className="-mr-2 flex-1 overflow-y-auto pr-2">
          {namespaces.map((ns) => (
            <button
              key={ns}
              onClick={() => selectNamespace(selected?.catalog ?? "spark_catalog", ns)}
              className={`block w-full rounded px-2 py-1 text-left text-sm ${
                selected?.namespace === ns ? "bg-accent/20 text-accent" : "hover:bg-surface"
              }`}
            >
              {ns}
            </button>
          ))}
          {!selected?.namespace && (
            <span className="text-sm text-muted">Select a catalog.</span>
          )}
        </div>
      </div>

      <div className="oxidant-card flex flex-col overflow-hidden">
        <div className="mb-2 flex items-center justify-between">
          <h2 className="text-sm font-semibold">Tables</h2>
          {selected?.namespace && (
            <button className="text-xs text-muted hover:text-text" onClick={copyName}>
              Copy namespace
            </button>
          )}
        </div>
        <div className="-mr-2 flex-1 overflow-y-auto pr-2">
          {tables.map((t) => (
            <button
              key={t.name}
              onClick={() =>
                selectTable(selected?.catalog ?? "spark_catalog", selected?.namespace ?? "", t.name)
              }
              className={`flex w-full items-center justify-between rounded px-2 py-1 text-left text-sm ${
                selected?.table === t.name ? "bg-accent/20 text-accent" : "hover:bg-surface"
              }`}
            >
              <span>{t.name}</span>
              <span className="text-xs text-muted">{t.type}</span>
            </button>
          ))}
          {selected?.namespace && tables.length === 0 && !loading && (
            <span className="text-sm text-muted">No tables.</span>
          )}
        </div>
      </div>

      <div className="oxidant-card flex flex-col overflow-hidden">
        <div className="mb-2 flex items-center justify-between">
          <h2 className="text-sm font-semibold">Columns</h2>
          {selected?.table && (
            <button className="text-xs text-muted hover:text-text" onClick={copyName}>
              Copy table
            </button>
          )}
        </div>
        <div className="-mr-2 flex-1 overflow-y-auto pr-2">
          {columns.map((c) => (
            <div
              key={c.name}
              className="flex items-center justify-between border-b border-border px-1 py-1.5 text-sm"
            >
              <span>{c.name}</span>
              <span className="text-xs text-muted">{c.type}</span>
            </div>
          ))}
          {selected?.table && columns.length === 0 && !loading && (
            <span className="text-sm text-muted">No columns.</span>
          )}
          {!selected?.table && (
            <span className="text-sm text-muted">Select a table.</span>
          )}
        </div>
      </div>

      {loading && <div className="text-xs text-muted">Loading…</div>}
      {error && <div className="oxidant-card text-danger">{error}</div>}
    </div>
  );
}
