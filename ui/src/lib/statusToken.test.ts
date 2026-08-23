/**
 * The status token this app holds, and what the Cluster page says when the driver refuses.
 *
 * `/api/v1/logs` is gated (it carries every logged field, and the server sets a permissive CORS
 * policy), so this app has to be able to authenticate and — when it cannot — to say which
 * refusal it hit rather than showing an empty log pane.
 */
import { beforeEach, describe, expect, it } from "vitest";
import {
  STATUS_TOKEN_KEY,
  logBufferNotice,
  setStatusToken,
  statusAuthHeaders,
  statusToken,
} from "@/lib/statusToken";

describe("the stored driver token", () => {
  beforeEach(() => localStorage.clear());

  it("is the same key the embedded console uses, so one paste serves both", () => {
    expect(STATUS_TOKEN_KEY).toBe("oxidant.statusToken");
    localStorage.setItem(STATUS_TOKEN_KEY, "from-the-other-console");
    expect(statusToken()).toBe("from-the-other-console");
    expect(statusAuthHeaders()).toEqual({
      Authorization: "Bearer from-the-other-console",
    });
  });

  it("sends no header at all when there is no token", () => {
    expect(statusToken()).toBe("");
    expect(statusAuthHeaders()).toEqual({});
  });

  it("trims what it stores, and clearing means removing", () => {
    setStatusToken("  abc  ");
    expect(localStorage.getItem(STATUS_TOKEN_KEY)).toBe("abc");
    setStatusToken("   ");
    expect(localStorage.getItem(STATUS_TOKEN_KEY)).toBeNull();
    expect(statusAuthHeaders()).toEqual({});
  });
});

describe("what the log pane says when it is refused", () => {
  it("says nothing when nothing went wrong", () => {
    expect(logBufferNotice(null)).toBeNull();
  });

  it("names the token for a 404 — the answer when the driver has none configured", () => {
    const notice = logBufferNotice("404 /api/v1/logs")!;
    expect(notice.needsToken).toBe(true);
    expect(notice.message).toContain("OXIDANT_STATUS_TOKEN");
    expect(notice.message).toContain("history server");
  });

  it("distinguishes a rejected credential from an absent route", () => {
    for (const code of ["401", "403"]) {
      const notice = logBufferNotice(`${code} /api/v1/logs`)!;
      expect(notice.needsToken).toBe(true);
      expect(notice.message).toContain("rejected");
    }
  });

  it("passes anything else through rather than guessing", () => {
    const notice = logBufferNotice("Failed to fetch")!;
    expect(notice.needsToken).toBe(false);
    expect(notice.message).toContain("Failed to fetch");
  });
});
