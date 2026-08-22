import { NavLink, Route, Routes } from "react-router-dom";
import { useAppMeta } from "@/lib/usePolling";
import { useTheme } from "@/lib/theme";
import { LogoMark } from "@/components/Logo";
import JobsPage from "@/pages/JobsPage";
import StagesPage from "@/pages/StagesPage";
import SqlPage from "@/pages/SqlPage";
import EditorPage from "@/pages/EditorPage";
import NotebookPage from "@/pages/NotebookPage";
import CatalogPage from "@/pages/CatalogPage";
import ExecutorsPage from "@/pages/ExecutorsPage";
import EnvironmentPage from "@/pages/EnvironmentPage";
import ComparePage from "@/pages/ComparePage";
import ClusterPage from "@/pages/ClusterPage";

const tabs = [
  { to: "/", label: "Jobs", end: true },
  { to: "/stages", label: "Stages" },
  { to: "/sql", label: "SQL" },
  { to: "/editor", label: "Editor" },
  { to: "/notebook", label: "Notebook" },
  { to: "/catalog", label: "Catalog" },
  { to: "/cluster", label: "Cluster" },
  { to: "/executors", label: "Executors" },
  { to: "/environment", label: "Environment" },
  { to: "/compare", label: "Compare" },
];

function ThemeToggle() {
  const { theme, toggle } = useTheme();
  return (
    <button
      onClick={toggle}
      aria-label="Toggle theme"
      title={theme === "dark" ? "Switch to light" : "Switch to dark"}
      className="oxidant-btn-ghost h-8 w-8 px-0 text-muted hover:text-body"
    >
      {theme === "dark" ? "☀" : "☾"}
    </button>
  );
}

export default function App() {
  const { data: meta } = useAppMeta();

  return (
    <div className="flex h-screen flex-col overflow-hidden">
      <header className="flex shrink-0 items-center gap-4 border-b border-hairline bg-surface px-5 py-3">
        <div className="flex items-center gap-2.5 text-body">
          <LogoMark className="h-6 w-6" />
          <span className="text-[15px] font-semibold tracking-display">Oxidant</span>
        </div>
        <span className="h-4 w-px bg-hairline-strong" aria-hidden="true" />
        <span className="truncate text-sm text-muted">
          {meta?.name ?? "Oxidant"} · jobs: {meta?.jobCount ?? 0}
        </span>
        <div className="ml-auto">
          <ThemeToggle />
        </div>
      </header>
      {/* Active tab is weight + full contrast on a raised slab — never a hue. */}
      <nav className="flex shrink-0 flex-wrap gap-1 border-b border-hairline px-5 py-2">
        {tabs.map((t) => (
          <NavLink
            key={t.to}
            to={t.to}
            end={t.end}
            className={({ isActive }) =>
              `rounded-oxidant-sm px-3 py-1.5 text-sm transition-colors ${
                isActive
                  ? "bg-raised font-medium text-body"
                  : "text-muted hover:bg-bg-subtle hover:text-body"
              }`
            }
          >
            {t.label}
          </NavLink>
        ))}
      </nav>
      <main className="flex-1 overflow-auto bg-bg p-4">
        <Routes>
          <Route path="/" element={<JobsPage />} />
          <Route path="/stages" element={<StagesPage />} />
          <Route path="/sql" element={<SqlPage />} />
          <Route path="/editor" element={<EditorPage />} />
          <Route path="/notebook" element={<NotebookPage />} />
          <Route path="/catalog" element={<CatalogPage />} />
          <Route path="/cluster" element={<ClusterPage />} />
          <Route path="/executors" element={<ExecutorsPage />} />
          <Route path="/environment" element={<EnvironmentPage />} />
          <Route path="/compare" element={<ComparePage />} />
        </Routes>
      </main>
    </div>
  );
}
