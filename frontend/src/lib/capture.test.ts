import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/api", () => ({ api: { post: vi.fn() } }));

import { api } from "@/lib/api";
import {
  CAPTURE_QUESTION_TYPES, amountRefused, answerCapture, deviceTime, newId, parseCapture, submitCapture,
  undoCapture, undoStillOpen, type Capture, type CapturePreview,
} from "./capture";

const preview = (over: Partial<CapturePreview> = {}): CapturePreview => ({
  amount: 5000, problems: [], kind: "expense", description: "parkir", occurred_at: null, time_phrase: null,
  time_options: [], account_hint: null, category_hint: null, ...over,
});

afterEach(() => {
  vi.mocked(api.post).mockReset();
  vi.useRealTimers();
});

describe("capture client", () => {
  it("writes the device time with its offset", () => {
    expect(deviceTime()).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$/);
  });

  it("makes a new id each time", () => {
    const first = newId();
    expect(first).toMatch(/^[0-9a-f-]{36}$/);
    expect(newId()).not.toBe(first);
  });

  it("keeps undo open only before its deadline", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-11-03T07:00:00Z"));
    expect(undoStillOpen(null)).toBe(false);
    expect(undoStillOpen("2026-11-03T07:00:05Z")).toBe(true);
    expect(undoStillOpen("2026-11-03T06:59:59Z")).toBe(false);
  });

  it("refuses a text without exactly one amount", () => {
    expect(amountRefused(null)).toBe(false);
    expect(amountRefused(preview())).toBe(false);
    expect(amountRefused(preview({ amount: null, problems: ["missing_amount"] }))).toBe(true);
    expect(amountRefused(preview({ amount: null, problems: ["several_amounts"] }))).toBe(true);
  });

  it("sends each call to its capture route with the agreed fields", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    const controller = new AbortController();
    await parseCapture("parkir 5rb", controller.signal);
    await submitCapture("parkir 5rb", "client-1");
    const capture = { id: "cap-1", question: { question_id: "q-1" } } as Capture;
    await answerCapture(capture, { option_id: "o-1" });
    await undoCapture("cap-1");
    const calls = vi.mocked(api.post).mock.calls;
    expect(calls.map(([path]) => path)).toEqual(["/capture/parse", "/capture/text", "/capture/cap-1/answer", "/capture/cap-1/undo"]);
    expect(calls[0][1]).toMatchObject({ text: "parkir 5rb" });
    expect(calls[0][2]).toBe(controller.signal);
    expect(calls[1][1]).toMatchObject({ client_capture_id: "client-1", text: "parkir 5rb", question_types: CAPTURE_QUESTION_TYPES });
    expect(calls[2][1]).toMatchObject({ question_id: "q-1", option_id: "o-1" });
    expect((calls[2][1] as { reply_id: string }).reply_id).toMatch(/^[0-9a-f-]{36}$/);
  });
});
