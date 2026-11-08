import { describe, expect, it } from "vitest";
import { chunkSuggestionIds, defaultWindow, runNotice, windowError, type SuggestionRun } from "./historySuggestions";

const run = (error_code: string | null): SuggestionRun => ({
  id: "run", status: "completed", from_date: "2026-08-01", to_date: "2026-10-28", eligible_count: 0, selected_count: 0,
  processed_count: 0, memory_count: 0, rule_count: 0, ai_count: 0, unchanged_count: 0, rejected_before_count: 0,
  ai_calls: 0, error_code, model: null, prompt_version: null, created_at: "2026-10-28T05:00:00Z", finished_at: null,
});

describe("history suggestions helpers", () => {
  it("defaults to the last three months in local dates", () => {
    expect(defaultWindow(new Date(2026, 9, 28))).toEqual({ from: "2026-07-28", to: "2026-10-28" });
  });

  it("validates windows like the backend", () => {
    const today = new Date(2026, 9, 28);
    expect(windowError("2026-10-01", "2026-10-28", today)).toBeNull();
    expect(windowError("2026-10-10", "2026-10-01", today)).toBe("order");
    expect(windowError("2026-10-01", "2026-10-29", today)).toBe("future");
    expect(windowError("2025-10-27", "2026-10-28", today)).toBe("tooLong");
    expect(windowError("2025-10-28", "2026-10-28", today)).toBeNull();
    expect(windowError("", "2026-10-28", today)).toBe("empty");
  });

  it("names run outcomes by key and keeps unknown codes visible", () => {
    expect(runNotice(run(null))).toBeNull();
    expect(runNotice(run("ai_disabled"))).toEqual({ key: "aiDisabled", code: "ai_disabled" });
    expect(runNotice(run("provider_timeout"))).toEqual({ key: "provider", code: "provider_timeout" });
    expect(runNotice(run("run_interrupted"))).toEqual({ key: "interrupted", code: "run_interrupted" });
    expect(runNotice(run("run_failed"))).toEqual({ key: "failed", code: "run_failed" });
    expect(runNotice(run("something_new"))).toEqual({ key: "other", code: "something_new" });
  });

  it("splits decisions into batches of 200", () => {
    const ids = Array.from({ length: 401 }, (_, index) => `s-${index}`);
    expect(chunkSuggestionIds(ids).map((chunk) => chunk.length)).toEqual([200, 200, 1]);
  });
});
