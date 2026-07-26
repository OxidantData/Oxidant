import { api } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";

export default function EnvironmentPage() {
  const { data: env, error } = usePolling(() => api.environment());
  const props = env?.sparkProperties ?? {};
  const keys = Object.keys(props).sort();

  if (error) return <div className="weft-card text-danger">{error}</div>;

  return (
    <div className="weft-card overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-muted">
            <th className="p-2 text-left">Key</th>
            <th className="p-2 text-left">Value</th>
          </tr>
        </thead>
        <tbody>
          {keys.map((k) => (
            <tr key={k} className="border-t border-border">
              <td className="p-2">{k}</td>
              <td className="p-2 font-mono text-xs">{props[k]}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
