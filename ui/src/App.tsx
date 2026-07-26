import { NavLink, Route, Routes } from "react-router-dom";
import { useAppMeta } from "@/lib/usePolling";
import JobsPage from "@/pages/JobsPage";
import StagesPage from "@/pages/StagesPage";
import SqlPage from "@/pages/SqlPage";
import ExecutorsPage from "@/pages/ExecutorsPage";
import EnvironmentPage from "@/pages/EnvironmentPage";
import ComparePage from "@/pages/ComparePage";

const tabs = [
  { to: "/", label: "Jobs", end: true },
  { to: "/stages", label: "Stages" },
  { to: "/sql", label: "SQL" },
  { to: "/executors", label: "Executors" },
  { to: "/environment", label: "Environment" },
  { to: "/compare", label: "Compare" },
];

export default function App() {
  const { data: meta } = useAppMeta();

  return (
    <div className="min-h-screen">
      <header className="flex items-center gap-4 border-b border-border px-5 py-3">
        <h1 className="text-lg font-semibold text-accent">Weft</h1>
        <span className="text-sm text-muted">
          {meta?.name ?? "Weft"} · jobs: {meta?.jobCount ?? 0}
        </span>
      </header>
      <nav className="flex flex-wrap gap-1 border-b border-border px-5 py-2">
        {tabs.map((t) => (
          <NavLink
            key={t.to}
            to={t.to}
            end={t.end}
            className={({ isActive }) =>
              `rounded-md px-3 py-1.5 text-sm ${isActive ? "bg-surface text-accent" : "text-muted hover:text-text"}`
            }
          >
            {t.label}
          </NavLink>
        ))}
      </nav>
      <main className="mx-auto max-w-6xl p-5">
        <Routes>
          <Route path="/" element={<JobsPage />} />
          <Route path="/stages" element={<StagesPage />} />
          <Route path="/sql" element={<SqlPage />} />
          <Route path="/executors" element={<ExecutorsPage />} />
          <Route path="/environment" element={<EnvironmentPage />} />
          <Route path="/compare" element={<ComparePage />} />
        </Routes>
      </main>
    </div>
  );
}
