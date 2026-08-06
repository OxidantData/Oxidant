interface Feature {
  title: string;
  body: string;
}

/**
 * Features hung off a single vertical warp thread — alternating indent, each connected by a short
 * oxidant tick + orange pickup node. Deliberately asymmetric so it never reads as a 2×2 feature grid.
 */
export default function WarpFeatureRail({ features }: { features: Feature[] }) {
  return (
    <div className="relative pl-6 sm:pl-10">
      {/* the warp thread */}
      <div className="absolute bottom-0 left-2 top-0 w-px bg-hairline sm:left-3" aria-hidden />
      <ul className="space-y-7">
        {features.map((f, i) => (
          <li
            key={f.title}
            className={`group relative ${i % 2 === 1 ? "sm:ml-16" : ""} transition-transform duration-150 hover:translate-x-0.5`}
          >
            {/* oxidant tick + pickup node */}
            <span
              aria-hidden
              className="absolute -left-4 top-3 h-px w-4 bg-hairline transition-colors group-hover:bg-accent sm:-left-7 sm:w-7"
            />
            <span
              aria-hidden
              className="absolute top-[9px] h-2 w-2 rounded-full border border-hairline bg-bg transition-colors group-hover:border-accent group-hover:bg-accent"
              style={{ left: "-1.30rem" }}
            />
            <h3 className="text-base font-semibold">{f.title}</h3>
            <p className="mt-1 max-w-md text-sm leading-relaxed text-muted">{f.body}</p>
          </li>
        ))}
      </ul>
    </div>
  );
}
