import { useEffect, useState } from "react";
import {
  api,
  type CatalogInfo,
  type CatalogTable,
  type CatalogColumn,
} from "@/lib/api";

interface CatalogSidebarProps {
  /** Called when the user clicks a namespace/table/column qualified name. */
  onInsert: (qualifiedName: string) => void;
  /** Start expanded; default true. */
  defaultExpanded?: boolean;
}

interface TreeNode {
  catalog: string;
  namespace: string;
  table?: string;
}

export default function CatalogSidebar({
  onInsert,
  defaultExpanded = true,
}: CatalogSidebarProps) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const [catalogs, setCatalogs] = useState<CatalogInfo[]>([]);
  const [namespaces, setNamespaces] = useState<string[]>([]);
  const [tables, setTables] = useState<CatalogTable[]>([]);
  const [columns, setColumns] = useState<CatalogColumn[]>([]);
  const [selected, setSelected] = useState<TreeNode | null>(null);
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

  function insertCatalog(c: CatalogInfo) {
    onInsert(c.name);
  }

  function insertNamespace(catalog: string, namespace: string) {
    onInsert(`${catalog}.${namespace}`);
  }

  function insertTable(catalog: string, namespace: string, table: string) {
    onInsert(`${catalog}.${namespace}.${table}`);
  }

  function insertColumn(
    catalog: string,
    namespace: string,
    table: string,
    column: string
  ) {
    onInsert(`${catalog}.${namespace}.${table}.${column}`);
  }

  if (!expanded) {
    return (
      <div className="flex w-10 flex-col items-center border-r border-hairline bg-surface py-3">
        <button
          title="Open catalog explorer"
          onClick={() => setExpanded(true)}
          className="rounded p-1.5 text-muted hover:bg-raised hover:text-body"
        >
          <DatabaseIcon />
        </button>
      </div>
    );
  }

  return (
    <div className="flex w-60 flex-col border-r border-hairline bg-surface">
      <div className="flex items-center justify-between border-b border-hairline px-3 py-2">
        <span className="text-sm font-semibold tracking-display">Catalog</span>
        <button
          title="Close catalog explorer"
          onClick={() => setExpanded(false)}
          className="rounded p-1 text-muted hover:bg-raised hover:text-body"
        >
          <ChevronLeftIcon />
        </button>
      </div>

      <div className="flex flex-1 flex-col overflow-hidden">
        <Panel label="Catalogs">
          {catalogs.map((c) => (
            <Item
              key={c.name}
              active={selected?.catalog === c.name}
              onClick={() => selectCatalog(c.name)}
              onDblClick={() => insertCatalog(c)}
            >
              <DatabaseIcon className="mr-1.5 h-3.5 w-3.5" />
              {c.name}
              {c.isCurrent && (
                <span className="ml-1 text-[10px] text-muted">(current)</span>
              )}
            </Item>
          ))}
        </Panel>

        <Panel label="Namespaces" loading={loading && !namespaces.length}>
          {namespaces.map((ns) => (
            <Item
              key={ns}
              active={selected?.namespace === ns}
              onClick={() =>
                selectNamespace(selected?.catalog ?? "spark_catalog", ns)
              }
              onDblClick={() =>
                insertNamespace(selected?.catalog ?? "spark_catalog", ns)
              }
            >
              <FolderIcon className="mr-1.5 h-3.5 w-3.5" />
              {ns}
            </Item>
          ))}
          {selected?.catalog && !namespaces.length && !loading && (
            <Empty>No namespaces</Empty>
          )}
        </Panel>

        <Panel label="Tables" loading={loading && !tables.length}>
          {tables.map((t) => (
            <Item
              key={t.name}
              active={selected?.table === t.name}
              onClick={() =>
                selectTable(
                  selected?.catalog ?? "spark_catalog",
                  selected?.namespace ?? "",
                  t.name
                )
              }
              onDblClick={() =>
                insertTable(
                  selected?.catalog ?? "spark_catalog",
                  selected?.namespace ?? "",
                  t.name
                )
              }
            >
              <TableIcon className="mr-1.5 h-3.5 w-3.5" />
              {t.name}
              <span className="ml-auto text-[10px] text-muted">{t.type}</span>
            </Item>
          ))}
          {selected?.namespace && !tables.length && !loading && (
            <Empty>No tables</Empty>
          )}
        </Panel>

        <Panel label="Columns" loading={loading && !columns.length}>
          {columns.map((c) => (
            <Item
              key={c.name}
              onDblClick={() =>
                insertColumn(
                  selected?.catalog ?? "spark_catalog",
                  selected?.namespace ?? "",
                  selected?.table ?? "",
                  c.name
                )
              }
            >
              <ColumnIcon className="mr-1.5 h-3.5 w-3.5" />
              {c.name}
              <span className="ml-auto text-[10px] text-muted">{c.type}</span>
            </Item>
          ))}
          {selected?.table && !columns.length && !loading && (
            <Empty>No columns</Empty>
          )}
        </Panel>
      </div>

      {error && (
        <div className="border-t border-hairline p-2 text-xs text-danger">
          {error}
        </div>
      )}
    </div>
  );
}

function Panel({
  label,
  children,
  loading,
}: {
  label: string;
  children: React.ReactNode;
  loading?: boolean;
}) {
  return (
    <div className="flex min-h-0 flex-1 flex-col border-b border-hairline last:border-b-0">
      <div className="oxidant-eyebrow bg-bg-subtle px-3 py-1.5">
        {label}
      </div>
      <div className="flex-1 overflow-y-auto px-2 py-1">
        {loading ? <Empty>Loading…</Empty> : children}
      </div>
    </div>
  );
}

function Item({
  active,
  onClick,
  onDblClick,
  children,
}: {
  active?: boolean;
  onClick?: () => void;
  onDblClick?: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      onDoubleClick={onDblClick}
      className={`flex w-full items-center rounded px-1.5 py-1 text-left text-xs ${
        active
          ? "bg-raised text-body"
          : "text-body hover:bg-raised"
      }`}
    >
      {children}
    </button>
  );
}

function Empty({ children }: { children: React.ReactNode }) {
  return <div className="px-1.5 py-2 text-xs text-muted">{children}</div>;
}

function DatabaseIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M3 5v14a9 3 0 0 0 18 0V5" />
      <path d="M3 12a9 3 0 0 0 18 0" />
    </svg>
  );
}

function FolderIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z" />
    </svg>
  );
}

function TableIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <rect x="3" y="3" width="18" height="18" rx="2" />
      <path d="M3 9h18" />
      <path d="M9 21V9" />
    </svg>
  );
}

function ColumnIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M14 3v18" />
      <rect x="4" y="3" width="16" height="18" rx="2" />
    </svg>
  );
}

function ChevronLeftIcon() {
  return (
    <svg
      className="h-4 w-4"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="m15 18-6-6 6-6" />
    </svg>
  );
}
