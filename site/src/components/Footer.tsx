const REPO = "https://github.com/OxidantData/Oxidant";

export default function Footer() {
  return (
    <footer className="border-t border-hairline">
      <div className="oxidant-container flex flex-col items-center justify-between gap-3 py-8 text-sm text-muted sm:flex-row">
        <div className="flex items-center gap-2">
          <img src="oxidant.svg" alt="" className="h-5 w-5" />
          <span>Oxidant — a drop-in Apache Spark replacement, in Rust.</span>
        </div>
        <div className="flex items-center gap-5">
          <a href={REPO} className="hover:text-body">
            GitHub
          </a>
          <a href={`${REPO}/tree/main/bench/clickbench`} className="hover:text-body">
            Benchmarks
          </a>
          <a href={`${REPO}/blob/main/docs/architecture.md`} className="hover:text-body">
            Architecture
          </a>
        </div>
      </div>
      <div className="border-t border-hairline">
        <div className="oxidant-container flex flex-col items-center justify-between gap-2 py-4 text-xs text-muted sm:flex-row">
          <span>© OxidantData</span>
          <div className="flex items-center gap-4">
            <a href={`${REPO}/blob/main/LICENSE`} className="hover:text-body">
              AGPLv3
            </a>
            <a href={`${REPO}/blob/main/COMMERCIAL.md`} className="hover:text-body">
              Commercial license
            </a>
            <span
              className="rounded-full border border-warning/40 bg-warning/10 px-2 py-0.5 text-[11px] font-medium text-warning"
              title="Oxidant is pre-alpha: the core engine runs the published benchmarks end-to-end, but expect rough edges."
            >
              pre-alpha
            </span>
          </div>
        </div>
      </div>
    </footer>
  );
}
