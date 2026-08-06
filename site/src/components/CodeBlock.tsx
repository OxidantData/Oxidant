import { useState } from "react";

interface Line {
  text: string;
  comment?: boolean;
}

/** Dark terminal-style code block (dark even in light mode, per the theme). */
export default function CodeBlock({ lines, copy }: { lines: Line[]; copy?: string }) {
  const [copied, setCopied] = useState(false);
  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(copy ?? lines.map((l) => l.text).join("\n"));
      setCopied(true);
      setTimeout(() => setCopied(false), 1400);
    } catch {
      /* clipboard blocked — ignore */
    }
  };
  return (
    <div className="relative overflow-hidden rounded-oxidant border border-hairline bg-code-bg">
      <button
        onClick={onCopy}
        className="oxidant-code-dim absolute right-2 top-2 rounded-oxidant-sm border border-white/10 px-2 py-1 text-xs transition-colors hover:bg-white/10"
      >
        {copied ? "copied" : "copy"}
      </button>
      <pre className="overflow-x-auto px-4 py-3.5 font-mono text-[13px] leading-relaxed oxidant-code-line">
        {lines.map((l, i) => (
          <div key={i} className={l.comment ? "oxidant-code-dim" : ""}>
            {l.text || " "}
          </div>
        ))}
      </pre>
    </div>
  );
}
