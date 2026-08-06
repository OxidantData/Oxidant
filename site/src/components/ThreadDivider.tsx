import { useId } from "react";

/**
 * Warp/oxidant thread divider — replaces a plain border between sections. A horizontal oxidant baseline
 * crossed by evenly spaced warp ticks, with one orange "shuttle rest" node. `node` is 0–1 across.
 */
export default function ThreadDivider({
  node = 0.5,
  glide = false,
  className = "",
}: {
  node?: number;
  /** animate the node gliding in once on mount (hero edge) */
  glide?: boolean;
  className?: string;
}) {
  const pid = useId();
  const W = 1100;
  const cx = Math.round(node * W);
  return (
    <svg
      aria-hidden
      viewBox={`0 0 ${W} 12`}
      preserveAspectRatio="none"
      className={`block h-3 w-full ${className}`}
    >
      <defs>
        <pattern id={pid} width="22" height="12" patternUnits="userSpaceOnUse">
          <line x1="0" y1="2" x2="0" y2="10" stroke="var(--oxidant-border)" strokeWidth="1" />
        </pattern>
      </defs>
      <line x1="0" y1="6" x2={W} y2="6" stroke="var(--oxidant-border)" strokeWidth="1" />
      <rect x="0" y="0" width={W} height="12" fill={`url(#${pid})`} />
      <circle
        cx={cx}
        cy="6"
        r="3.5"
        fill="var(--oxidant-accent)"
        className={glide ? "oxidant-anim-shuttle" : ""}
        style={{ transformBox: "fill-box", transformOrigin: "center" }}
      />
    </svg>
  );
}
