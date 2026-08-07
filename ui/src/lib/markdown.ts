/**
 * Tiny hand-rolled Markdown renderer (headings, bold/italic, inline code, code
 * fences, lists, links). No external libraries — the embedded SPA twin must keep
 * working air-gapped, and the React twin mirrors it. Output is HTML intended for
 * `dangerouslySetInnerHTML`; all source text is escaped before formatting.
 */

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function inline(s: string): string {
  let out = escapeHtml(s);
  // Pull inline code spans out first so their contents get no further formatting.
  const codes: string[] = [];
  out = out.replace(/`([^`]+)`/g, (_m, c: string) => {
    codes.push(c);
    return `\u0000${codes.length - 1}\u0000`;
  });
  out = out.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  out = out.replace(/\*([^*\s][^*]*)\*/g, "<em>$1</em>");
  out = out.replace(
    /\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g,
    '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>'
  );
  out = out.replace(
    /\u0000(\d+)\u0000/g,
    (_m, i: string) => `<code>${codes[Number(i)]}</code>`
  );
  return out;
}

export function renderMarkdown(src: string): string {
  const out: string[] = [];
  let inFence = false;
  let fence: string[] = [];
  let inList = false;
  const closeList = () => {
    if (inList) {
      out.push("</ul>");
      inList = false;
    }
  };
  for (const line of src.split("\n")) {
    if (line.trim().startsWith("```")) {
      if (inFence) {
        out.push(`<pre>${escapeHtml(fence.join("\n"))}</pre>`);
        fence = [];
        inFence = false;
      } else {
        closeList();
        inFence = true;
      }
      continue;
    }
    if (inFence) {
      fence.push(line);
      continue;
    }
    const heading = line.match(/^(#{1,6})\s+(.*)$/);
    if (heading) {
      closeList();
      const level = heading[1].length;
      out.push(`<h${level}>${inline(heading[2])}</h${level}>`);
      continue;
    }
    const item = line.match(/^\s*[-*]\s+(.*)$/);
    if (item) {
      if (!inList) {
        out.push("<ul>");
        inList = true;
      }
      out.push(`<li>${inline(item[1])}</li>`);
      continue;
    }
    closeList();
    if (!line.trim()) continue;
    out.push(`<p>${inline(line)}</p>`);
  }
  closeList();
  if (inFence) out.push(`<pre>${escapeHtml(fence.join("\n"))}</pre>`);
  return out.join("\n");
}
