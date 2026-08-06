const REPO = "https://github.com/OxidantData/Oxidant";
/** No live Marketplace listing yet (see docs/marketplace.md) — point at the standalone-AMI quickstart. */
export const AMI_URL = `${REPO}/blob/main/deploy/packer/files/QUICKSTART.md`;
export const CONTACT_URL = "mailto:hello@oxidantdata.com";

function Check() {
  return (
    <svg
      aria-hidden
      viewBox="0 0 16 16"
      className="mt-0.5 h-4 w-4 shrink-0 text-accent"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M3 8.5l3.2 3L13 4.5" />
    </svg>
  );
}

interface Tier {
  name: string;
  price: string;
  priceNote: string;
  tagline: string;
  badge?: string;
  highlight?: boolean;
  features: string[];
  cta: { label: string; href: string; primary?: boolean; disabled?: boolean };
}

const TIERS: Tier[] = [
  {
    name: "Community",
    price: "$0",
    priceNote: "forever · AGPLv3",
    tagline: "The full engine, free and open source.",
    highlight: true,
    features: [
      "Full Oxidant engine — nothing gated",
      "Free AWS Marketplace AMI + GHCR image",
      "Spark Connect API for stock PySpark",
      "Distributed driver/worker mode",
      "Community support (GitHub)",
    ],
    cta: { label: "Get the AMI", href: AMI_URL, primary: true },
  },
  {
    name: "Business",
    price: "~$1.50/hr",
    priceNote: "or ~$9K/yr annual",
    tagline: "Oxidant Platform — the control plane for teams.",
    badge: "Coming soon",
    features: [
      "Notebooks + SQL editor",
      "Workflows and job scheduling",
      "Autoscaling with auto-termination",
      "RBAC and audit logs",
      "Air-gapped — runs in your VPC",
    ],
    cta: { label: "Coming soon", href: "#", disabled: true },
  },
  {
    name: "Enterprise",
    price: "$25–60K/yr",
    priceNote: "BYOL · annual contract",
    tagline: "Commercial terms for organizations that need them.",
    features: [
      "SSO / SAML",
      "Support SLA",
      "Indemnification",
      "Commercial license (no AGPLv3 obligations)",
    ],
    cta: { label: "Talk to us", href: CONTACT_URL },
  },
];

const METERED = [
  {
    name: "Databricks",
    figure: "~$0.15–0.70 / DBU",
    note: "Charged per Databricks Unit, on top of your AWS bill. Busy cluster, bigger invoice.",
  },
  {
    name: "Snowflake",
    figure: "~$2–4 / credit",
    note: "The meter runs every second the warehouse is up, whether a query returns or not.",
  },
  {
    name: "Oxidant",
    figure: "Flat or free",
    note: "Unlimited queries on your own hardware. Your bill stops growing with your data.",
    accent: true,
  },
];

function TierCard({ tier }: { tier: Tier }) {
  return (
    <div
      className={`relative flex flex-col rounded-oxidant border p-6 ${
        tier.highlight ? "border-accent bg-surface shadow-sm" : "border-hairline bg-surface"
      }`}
    >
      {tier.badge && (
        <span className="absolute right-4 top-4 rounded-full border border-hairline bg-bg-subtle px-2.5 py-0.5 text-[11px] font-semibold uppercase tracking-wide text-muted">
          {tier.badge}
        </span>
      )}
      <h3 className="text-lg font-semibold tracking-tight">{tier.name}</h3>
      <p className="mt-1 text-sm text-muted">{tier.tagline}</p>
      <div className="mt-4">
        <span className="text-3xl font-bold tracking-tight">{tier.price}</span>
        <span className="ml-2 text-sm text-muted">{tier.priceNote}</span>
      </div>
      <ul className="mt-5 flex-1 space-y-2.5 text-sm">
        {tier.features.map((f) => (
          <li key={f} className="flex items-start gap-2">
            <Check />
            <span>{f}</span>
          </li>
        ))}
      </ul>
      {tier.cta.disabled ? (
        <span className="oxidant-btn-ghost mt-6 cursor-not-allowed opacity-60">{tier.cta.label}</span>
      ) : (
        <a
          href={tier.cta.href}
          className={`mt-6 ${tier.cta.primary ? "oxidant-btn-primary" : "oxidant-btn-ghost"}`}
        >
          {tier.cta.label}
        </a>
      )}
    </div>
  );
}

/** Pricing — the aha moment: a free, faster engine vs metered DBU/credit pricing. */
export default function PricingSection() {
  return (
    <section id="pricing" className="scroll-mt-20 border-b border-hairline bg-bg-subtle">
      <div className="oxidant-container py-16 sm:py-20">
        <div className="mx-auto mb-12 max-w-2xl text-center">
          <span className="oxidant-eyebrow">Pricing</span>
          <h2 className="mt-2 text-2xl font-bold tracking-tight sm:text-3xl">
            Your bill stops growing with your data.
          </h2>
          <p className="mt-3 text-muted">
            The incumbents meter every query. Oxidant runs on your hardware, free or flat — scan a
            petabyte or ten, the license cost doesn't move.
          </p>
        </div>

        <div className="grid gap-5 lg:grid-cols-3">
          {TIERS.map((t) => (
            <TierCard key={t.name} tier={t} />
          ))}
        </div>

        {/* Comparison anchor — the metered alternative */}
        <div className="mt-10 overflow-hidden rounded-oxidant border border-hairline bg-surface">
          <div className="border-b border-hairline bg-bg-subtle px-5 py-3 text-xs font-semibold uppercase tracking-wider text-muted">
            The metered alternative
          </div>
          <div className="grid divide-y divide-hairline sm:grid-cols-3 sm:divide-x sm:divide-y-0">
            {METERED.map((m) => (
              <div key={m.name} className={`px-5 py-5 ${m.accent ? "bg-accent/5" : ""}`}>
                <div className="text-sm font-semibold">{m.name}</div>
                <div className={`mt-1 font-mono text-lg font-semibold ${m.accent ? "text-accent" : ""}`}>
                  {m.figure}
                </div>
                <p className="mt-2 text-sm text-muted">{m.note}</p>
              </div>
            ))}
          </div>
        </div>

        <p className="mt-6 text-center text-sm text-muted">
          Self-hosting Community costs you only the EC2 it runs on — the AMI carries no per-hour
          software charge.
        </p>
      </div>
    </section>
  );
}
