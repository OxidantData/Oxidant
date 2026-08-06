import { useInView, prefersReducedMotion } from "../lib/useInView";

/**
 * The Loom — a stylized weaving diagram (brand metaphor, not a literal architecture map). Your
 * Spark plan enters as warp threads, the heddle (optimizer) reorders them, and Loom weaves them
 * with the oxidant shuttle into finished cloth (Arrow result batches). When `animated`, the shuttle
 * does one left→right pass on scroll-in and the cloth fills. Theme colors only.
 */
export default function LoomDiagram({ animated = true }: { animated?: boolean }) {
  const { ref, inView } = useInView<HTMLDivElement>();
  const reduce = prefersReducedMotion();
  const go = !animated || reduce || inView;

  const warp = Array.from({ length: 7 }, (_, i) => 70 + i * 9);

  return (
    <div ref={ref}>
      <svg viewBox="0 0 520 240" className="w-full" role="img" aria-label="Oxidant's loom: warp plan, heddle optimizer, Loom weaving Arrow batches">
        <defs>
          <pattern id="cloth" width="8" height="8" patternUnits="userSpaceOnUse">
            <path d="M0 4 H8 M4 0 V8" stroke="var(--oxidant-accent)" strokeWidth="1" opacity="0.5" />
          </pattern>
          <clipPath id="clothClip">
            <rect x="360" y="60" width="140" height="120" rx="6" />
          </clipPath>
        </defs>

        {/* warp — incoming plan threads */}
        {warp.map((y) => (
          <line key={y} x1="20" y1={y} x2="150" y2={y} stroke="var(--oxidant-text)" strokeWidth="1" opacity="0.35" />
        ))}
        <text x="20" y="208" fontSize="11" fill="var(--oxidant-text-muted)" fontFamily="var(--oxidant-font-mono)">
          warp · your plan
        </text>

        {/* heddle — the optimizer bar (the one colored stage) */}
        <rect x="170" y="56" width="34" height="128" rx="5" fill="var(--oxidant-accent)" opacity="0.92" />
        <text x="187" y="208" fontSize="11" fill="var(--oxidant-text-muted)" fontFamily="var(--oxidant-font-mono)" textAnchor="middle">
          heddle
        </text>

        {/* feed arrows */}
        <line x1="150" y1="120" x2="170" y2="120" stroke="var(--oxidant-text-muted)" strokeWidth="1.5" markerEnd="url(#ah)" />
        <line x1="204" y1="120" x2="360" y2="120" stroke="var(--oxidant-text-muted)" strokeWidth="1.5" markerEnd="url(#ah)" />
        <defs>
          <marker id="ah" markerWidth="7" markerHeight="7" refX="5" refY="3.5" orient="auto">
            <path d="M0 0 L6 3.5 L0 7 z" fill="var(--oxidant-text-muted)" />
          </marker>
        </defs>

        {/* cloth — the woven result, fills left→right */}
        <rect x="360" y="60" width="140" height="120" rx="6" fill="none" stroke="var(--oxidant-border)" />
        <rect
          clipPath="url(#clothClip)"
          x="360"
          y="60"
          height="120"
          fill="url(#cloth)"
          style={{
            width: go ? 140 : 0,
            transition: reduce ? "none" : "width 1.1s cubic-bezier(.22,1,.36,1)",
          }}
        />
        {/* oxidant shuttle traveling the cloth's leading edge */}
        <g
          style={{
            transform: go ? "translateX(140px)" : "translateX(0px)",
            transition: reduce ? "none" : "transform 1.1s cubic-bezier(.22,1,.36,1)",
          }}
        >
          <rect x="356" y="113" width="12" height="12" rx="2" transform="rotate(45 362 119)" fill="var(--oxidant-accent)" />
        </g>
        <text x="430" y="208" fontSize="11" fill="var(--oxidant-text-muted)" fontFamily="var(--oxidant-font-mono)" textAnchor="middle">
          Loom · Arrow batches
        </text>
      </svg>
    </div>
  );
}
