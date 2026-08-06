/** Near-invisible woven backdrop (warp + oxidant hairlines) for hero / closing bands. Decorative. */
export default function WovenField({ dense = false }: { dense?: boolean }) {
  return (
    <div
      aria-hidden
      className={`pointer-events-none absolute inset-0 woven-field ${dense ? "woven-field--dense" : ""}`}
    />
  );
}
