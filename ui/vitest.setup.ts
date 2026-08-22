import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

afterEach(() => cleanup());

/**
 * jsdom implements neither observer the grid depends on. `useContainerWidth` constructs a
 * ResizeObserver unconditionally, and the theme hook uses MutationObserver (which jsdom does
 * have). Stub the missing one rather than branching the component on `typeof` checks that
 * only a test would ever take.
 */
if (typeof globalThis.ResizeObserver === "undefined") {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
}

/**
 * jsdom has no layout engine, so every element measures 0×0 and react-grid-layout would place
 * every card at the same spot. Fix a size on the prototype so the grid has something to divide
 * into columns.
 */
for (const [prop, value] of [
  ["clientWidth", 1200],
  ["clientHeight", 800],
  ["offsetWidth", 1200],
  ["offsetHeight", 800],
] as const) {
  Object.defineProperty(window.HTMLElement.prototype, prop, {
    configurable: true,
    value,
  });
}

/** `window.matchMedia` is missing in jsdom; ECharts and Tailwind-adjacent code probe it. */
if (typeof window.matchMedia !== "function") {
  window.matchMedia = vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));
}

/**
 * ECharts measures text through a canvas even in server-side-rendering mode, and jsdom logs a
 * "not implemented" line for every probe. A measuring stub keeps the output readable and makes
 * text metrics deterministic; nothing under test depends on the measurement being accurate.
 */
const CHAR_WIDTH = 7;
window.HTMLCanvasElement.prototype.getContext = vi.fn(() => ({
  measureText: (text: string) => ({ width: String(text).length * CHAR_WIDTH }),
  font: "",
})) as unknown as typeof window.HTMLCanvasElement.prototype.getContext;
