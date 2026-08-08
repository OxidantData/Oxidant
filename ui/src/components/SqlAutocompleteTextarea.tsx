import {
  useEffect,
  useRef,
  useState,
  forwardRef,
  useImperativeHandle,
  type TextareaHTMLAttributes,
  type KeyboardEvent,
} from "react";
import { api, type AutocompleteSuggestion } from "@/lib/api";

interface SqlAutocompleteTextareaProps
  extends Omit<TextareaHTMLAttributes<HTMLTextAreaElement>, "ref"> {}

export interface SqlAutocompleteTextareaHandle {
  insertText: (text: string) => void;
  focus: () => void;
}

const SEPARATORS = /[\s,;()=<>!+\-*/'"{}\[\]]/;
const MIN_QUERY_LEN = 1;
const MAX_SUGGESTIONS = 8;

export default forwardRef<
  SqlAutocompleteTextareaHandle,
  SqlAutocompleteTextareaProps
>(function SqlAutocompleteTextarea(props, ref) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const popupRef = useRef<HTMLDivElement>(null);
  const [suggestions, setSuggestions] = useState<AutocompleteSuggestion[]>([]);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [queryPrefix, setQueryPrefix] = useState("");
  const [popupPos, setPopupPos] = useState<{ top: number; left: number } | null>(
    null
  );
  const [open, setOpen] = useState(false);
  const debounceRef = useRef<number | null>(null);

  useImperativeHandle(ref, () => ({
    insertText: (text: string) => {
      const el = textareaRef.current;
      if (!el) return;
      const start = el.selectionStart ?? 0;
      const end = el.selectionEnd ?? 0;
      const before = el.value.slice(0, start);
      const after = el.value.slice(end);
      const insert = maybeAddLeadingSpace(before, text);
      const newValue = before + insert + after;
      el.value = newValue;
      el.selectionStart = el.selectionEnd = start + insert.length;
      el.focus();
      props.onChange?.({
        target: el,
        currentTarget: el,
      } as React.ChangeEvent<HTMLTextAreaElement>);
    },
    focus: () => textareaRef.current?.focus(),
  }));

  function maybeAddLeadingSpace(before: string, text: string): string {
    if (!before.length) return text;
    const last = before.slice(-1);
    if (SEPARATORS.test(last)) return text;
    return `.${text}`;
  }

  function extractPrefix(value: string, caret: number): string | null {
    const before = value.slice(0, caret);
    // Find the start of the current identifier chain by scanning backward.
    let i = before.length - 1;
    while (i >= 0) {
      const ch = before[i];
      if (SEPARATORS.test(ch)) break;
      i--;
    }
    const prefix = before.slice(i + 1);
    if (prefix.length < MIN_QUERY_LEN) return null;
    return prefix;
  }

  function updatePopupPosition() {
    const el = textareaRef.current;
    if (!el) return;
    const coords = getCaretCoordinates(el, el.selectionStart ?? 0);
    const rect = el.getBoundingClientRect();
    const top = rect.top + coords.top - el.scrollTop + 18;
    const left = rect.left + coords.left - el.scrollLeft;
    setPopupPos({ top, left });
  }

  function fetchSuggestions(prefix: string) {
    if (debounceRef.current) window.clearTimeout(debounceRef.current);
    debounceRef.current = window.setTimeout(async () => {
      try {
        const { suggestions: list } = await api.catalogs.autocomplete(prefix);
        const filtered = list.slice(0, MAX_SUGGESTIONS);
        if (filtered.length) {
          setSuggestions(filtered);
          setSelectedIndex(0);
          setQueryPrefix(prefix);
          setOpen(true);
          updatePopupPosition();
        } else {
          setOpen(false);
        }
      } catch {
        setOpen(false);
      }
    }, 120);
  }

  function handleInput(e: React.FormEvent<HTMLTextAreaElement>) {
    const el = e.currentTarget;
    const prefix = extractPrefix(el.value, el.selectionStart ?? 0);
    if (prefix) {
      fetchSuggestions(prefix);
    } else {
      setOpen(false);
    }
    props.onChange?.(e as unknown as React.ChangeEvent<HTMLTextAreaElement>);
    props.onInput?.(e);
  }

  function acceptSuggestion(s: AutocompleteSuggestion) {
    const el = textareaRef.current;
    if (!el) return;
    const caret = el.selectionStart ?? 0;
    const before = el.value.slice(0, caret);
    const after = el.value.slice(caret);
    const newBefore = before.slice(0, -queryPrefix.length) + s.qualified;
    const newValue = newBefore + after;
    el.value = newValue;
    const newCaret = newBefore.length;
    el.selectionStart = el.selectionEnd = newCaret;
    el.focus();
    setOpen(false);
    props.onChange?.({
      target: el,
      currentTarget: el,
    } as React.ChangeEvent<HTMLTextAreaElement>);
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (!open) {
      if (e.ctrlKey && e.key === " ") {
        e.preventDefault();
        const el = e.currentTarget;
        const prefix = extractPrefix(el.value, el.selectionStart ?? 0) ?? "";
        fetchSuggestions(prefix);
        return;
      }
      props.onKeyDown?.(e);
      return;
    }

    switch (e.key) {
      case "ArrowDown":
        e.preventDefault();
        setSelectedIndex((i) => (i + 1) % suggestions.length);
        break;
      case "ArrowUp":
        e.preventDefault();
        setSelectedIndex((i) =>
          i === 0 ? suggestions.length - 1 : i - 1
        );
        break;
      case "Tab":
      case "Enter":
        e.preventDefault();
        acceptSuggestion(suggestions[selectedIndex]);
        break;
      case "Escape":
        e.preventDefault();
        setOpen(false);
        break;
      default:
        props.onKeyDown?.(e);
    }
  }

  useEffect(() => {
    function onScroll() {
      if (open) updatePopupPosition();
    }
    function onClick(e: MouseEvent) {
      if (!popupRef.current?.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("click", onClick);
    return () => {
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("click", onClick);
    };
  }, [open]);

  return (
    <div className="relative">
      <textarea
        {...props}
        ref={textareaRef}
        onInput={handleInput}
        onKeyDown={handleKeyDown}
      />
      {open && popupPos && (
        <div
          ref={popupRef}
          style={{ top: popupPos.top, left: popupPos.left }}
          className="fixed z-50 min-w-[220px] max-w-xs overflow-hidden rounded-md border border-border bg-surface shadow-lg"
        >
          <div className="border-b border-border px-2 py-1 text-[10px] text-muted">
            Catalog autocomplete
          </div>
          {suggestions.map((s, i) => (
            <button
              key={`${s.kind}-${s.qualified}`}
              onMouseDown={(e) => {
                e.preventDefault();
                acceptSuggestion(s);
              }}
              onMouseEnter={() => setSelectedIndex(i)}
              className={`flex w-full items-center justify-between px-2.5 py-1.5 text-left text-xs ${
                i === selectedIndex ? "bg-accent/20 text-accent" : "text-text"
              }`}
            >
              <span className="truncate" title={s.qualified}>
                {s.name}
              </span>
              <span
                className={`ml-2 shrink-0 rounded px-1 py-0.5 text-[10px] ${
                  kindBadgeClass(s.kind)
                }`}
              >
                {s.kind}
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
});

function kindBadgeClass(kind: AutocompleteSuggestion["kind"]): string {
  switch (kind) {
    case "catalog":
      return "bg-warning/20 text-warning";
    case "namespace":
      return "bg-success/20 text-success";
    case "table":
      return "bg-accent/20 text-accent";
    case "column":
      return "bg-muted/20 text-muted";
    default:
      return "bg-muted/20 text-muted";
  }
}

/**
 * Compute the x/y coordinates of the caret inside a textarea by mirroring its
 * layout into a hidden <div>. Adapted from the standard textarea-caret-position
 * algorithm (MIT). Values are relative to the textarea's top-left corner.
 */
function getCaretCoordinates(
  textarea: HTMLTextAreaElement,
  position: number
): { top: number; left: number } {
  const div = document.createElement("div");
  const style = getComputedStyle(textarea);
  const properties: (keyof CSSStyleDeclaration)[] = [
    "fontFamily",
    "fontSize",
    "fontWeight",
    "fontStyle",
    "letterSpacing",
    "textTransform",
    "wordSpacing",
    "textIndent",
    "lineHeight",
    "paddingTop",
    "paddingRight",
    "paddingBottom",
    "paddingLeft",
    "borderTopWidth",
    "borderRightWidth",
    "borderBottomWidth",
    "borderLeftWidth",
    "boxSizing",
    "whiteSpace",
  ];
  for (const prop of properties) {
    const value = style[prop];
    if (typeof value === "string") {
      (div.style as unknown as Record<string, string>)[String(prop)] = value;
    }
  }
  div.style.position = "absolute";
  div.style.visibility = "hidden";
  div.style.whiteSpace = "pre-wrap";
  div.style.wordWrap = "break-word";
  div.style.overflowWrap = "break-word";
  div.style.width = `${textarea.clientWidth}px`;
  div.style.height = "auto";

  const text = textarea.value.slice(0, position);
  const after = textarea.value.slice(position);
  div.textContent = text;
  const span = document.createElement("span");
  span.textContent = after ? after[0] : ".";
  div.appendChild(span);

  document.body.appendChild(div);
  const rect = span.getBoundingClientRect();
  const divRect = div.getBoundingClientRect();
  document.body.removeChild(div);
  return {
    top: rect.top - divRect.top,
    left: rect.left - divRect.left,
  };
}
