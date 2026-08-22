import { useCallback, useEffect, useState } from "react";

export type Theme = "light" | "dark";

/** Shared with index.html's pre-paint script and the embedded single-file UI. */
const STORAGE_KEY = "oxidant-theme";

function readInitial(): Theme {
  if (typeof document === "undefined") return "dark";
  return document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
}

function apply(theme: Theme) {
  const root = document.documentElement;
  if (theme === "light") root.setAttribute("data-theme", "light");
  else root.removeAttribute("data-theme");
}

/** Dark-mode-primary theme with a persisted light toggle (data-theme on :root). */
export function useTheme() {
  const [theme, setTheme] = useState<Theme>(readInitial);

  useEffect(() => {
    apply(theme);
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch {
      // ignore storage failures (private mode, etc.)
    }
  }, [theme]);

  const toggle = useCallback(() => {
    setTheme((t) => (t === "dark" ? "light" : "dark"));
  }, []);

  return { theme, toggle };
}

/**
 * The theme currently applied to `<html>`, tracked by observing the attribute rather than by
 * sharing `useTheme`'s state. The toggle lives in one component and there is no theme context;
 * anything that has to *react* to a switch (the charts, which bake their palette in at init)
 * subscribes here instead of duplicating the toggle's state.
 */
export function useThemeMode(): Theme {
  const [theme, setTheme] = useState<Theme>(readInitial);

  useEffect(() => {
    if (typeof MutationObserver !== "function") return;
    const root = document.documentElement;
    const observer = new MutationObserver(() => setTheme(readInitial()));
    observer.observe(root, { attributes: true, attributeFilter: ["data-theme"] });
    setTheme(readInitial());
    return () => observer.disconnect();
  }, []);

  return theme;
}
