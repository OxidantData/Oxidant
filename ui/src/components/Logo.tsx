/**
 * The Oxidant mark — a plain-weave grid (warp over weft, weft over warp), the visual root of the
 * Loom/warp/heddle crate family. Geometry is identical to the website's src/components/Logo.tsx
 * and to public/brand/logo-mark-{black,white}.svg; inlined here and stroked with `currentColor`
 * so one component serves both themes without a second request or a light/dark image swap.
 */

const WEAVE = [
  // warp (vertical) threads, broken where the weft passes over them
  "M12 19.2 L12 36.8",
  "M12 51.2 L12 64",
  "M28 8 L28 20.8",
  "M28 35.2 L28 52.8",
  "M44 19.2 L44 36.8",
  "M44 51.2 L44 64",
  "M60 8 L60 20.8",
  "M60 35.2 L60 52.8",
  // weft (horizontal) threads, broken where the warp passes over them
  "M8 12 L20.8 12",
  "M35.2 12 L52.8 12",
  "M19.2 28 L36.8 28",
  "M51.2 28 L64 28",
  "M8 44 L20.8 44",
  "M35.2 44 L52.8 44",
  "M19.2 60 L36.8 60",
  "M51.2 60 L64 60",
];

export function LogoMark({ className = "h-7 w-7" }: { className?: string }) {
  return (
    <svg viewBox="0 0 72 72" className={className} role="img" aria-label="Oxidant">
      <g fill="none" stroke="currentColor" strokeWidth={8} strokeLinecap="round">
        {WEAVE.map((d) => (
          <path key={d} d={d} />
        ))}
      </g>
    </svg>
  );
}

/**
 * The full lockup — mark + wordmark. The wordmark is live Geist text rather than outlines so it
 * matches the page's typography exactly and stays selectable.
 */
export function LogoLockup({
  markClass = "h-6 w-6",
  wordClass = "text-[15px]",
  className = "",
}: {
  markClass?: string;
  wordClass?: string;
  className?: string;
}) {
  return (
    <div className={`flex items-center gap-2.5 text-body ${className}`}>
      <LogoMark className={markClass} />
      <span className={`font-semibold tracking-display ${wordClass}`}>Oxidant</span>
    </div>
  );
}
