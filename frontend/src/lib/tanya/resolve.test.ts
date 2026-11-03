import { describe, expect, it, vi } from "vitest";
import { ApiError } from "@/lib/api";
import type { Capture, CapturePreview } from "@/lib/capture";
import { ASK_AMOUNT_TEXT, HELP_TEXT, SEVERAL_AMOUNTS_TEXT, UNKNOWN_TEXT } from "./replies";
import { NOTICE_PARSE_FAILED, NOTICE_RATE_LIMITED, SUGGESTION_CHIPS, runTurn, type TurnDeps } from "./resolve";
import type { CardData } from "./types";

const preview = (over: Partial<CapturePreview> = {}): CapturePreview => ({
  amount: 5000, problems: [], kind: "expense", description: "parkir", occurred_at: "2026-11-03T07:00:00Z", time_phrase: "jam 2",
  time_options: [], account_hint: null, category_hint: null, ...over,
});

const recorded = (over: Partial<Capture> = {}): Capture => ({
  id: "cap-1", status: "recorded", question: null, summary: { amount: 5000, description: "parkir", time_label: "14.00" },
  transaction: { id: "tx-1", type: "expense", amount: 5000, date: "2026-11-03T07:00:00Z", account_name: "Tunai", merchant: null, notes: "parkir",
    category_name: "Transportasi", kakeibo: "need", review_status: "reviewed" },
  undo_until: "2026-11-03T07:00:10Z", ...over,
});

const cardData: CardData = { kind: "report", report: { ready: true, period: { label: "Oktober", end_date: "2026-10-31" }, url: "/laporan" } };

function deps(over: Partial<TurnDeps> = {}) {
  const value = {
    parse: vi.fn(async () => ({ preview: preview() })),
    submit: vi.fn(async () => ({ capture: recorded() })),
    loadCard: vi.fn(async () => cardData),
    now: () => new Date("2026-11-03T07:03:00Z"),
    ...over,
  };
  return value as typeof value & TurnDeps;
}

describe("resolver chain", () => {
  it.each([
    ["Ringkasan siklus ini", "cycle"],
    ["kakeibo", "kakeibo"],
    ["tagihan minggu ini", "bills"],
    ["menunggu jawaban", "waiting"],
    ["laporan terbaru", "report"],
  ])("answers %s with a %s card and sends no capture request", async (text, kind) => {
    const d = deps();
    const outcome = await runTurn(text, "client-1", d);
    expect(outcome.route).toBe("intent");
    expect(outcome.reply?.card).toMatchObject({ kind, status: "ready", asOf: "2026-11-03T07:03:00.000Z" });
    expect(d.loadCard).toHaveBeenCalledWith(kind);
    expect(d.parse).not.toHaveBeenCalled();
    expect(d.submit).not.toHaveBeenCalled();
  });

  it("keeps the week range on the bills card", async () => {
    const outcome = await runTurn("tagihan minggu ini", "client-1", deps());
    expect(outcome.reply?.card?.range).toBe("week");
  });

  it("answers help with text and the chips, loading nothing", async () => {
    const d = deps();
    const outcome = await runTurn("bantuan", "client-1", d);
    expect(outcome.reply).toMatchObject({ text: HELP_TEXT, card: null, chips: SUGGESTION_CHIPS });
    expect(d.loadCard).not.toHaveBeenCalled();
  });

  it("turns a failed card fetch into an error card, never a figure", async () => {
    const outcome = await runTurn("ringkasan", "client-1", deps({ loadCard: vi.fn(async () => { throw new TypeError("offline"); }) }));
    expect(outcome.reply?.card).toMatchObject({ kind: "cycle", status: "error", data: null });
  });

  it("records a text with one amount once, with the message's client id", async () => {
    const d = deps();
    const outcome = await runTurn("parkir 5rb jam 2", "client-7", d);
    expect(outcome.route).toBe("capture");
    expect(d.parse).toHaveBeenCalledTimes(1);
    expect(d.submit).toHaveBeenCalledTimes(1);
    expect(d.submit).toHaveBeenCalledWith("parkir 5rb jam 2", "client-7");
    expect(outcome.reply?.capture?.id).toBe("cap-1");
    expect(outcome.reply?.text).toBe("Udah aku catat.");
  });

  it("says when the recorded row went to Tinjau", async () => {
    const capture = recorded();
    capture.transaction!.review_status = "needs_review";
    const outcome = await runTurn("parkir 5rb", "client-1", deps({ submit: vi.fn(async () => ({ capture })) }));
    expect(outcome.reply?.text).toMatch(/Tinjau/);
  });

  it("keeps a follow-up question on the capture for the thread to ask", async () => {
    const capture = recorded({ status: "needs_detail", question: { question_id: "q-1", type: "account", prompt: "Dari rekening mana?", options: [], allows_text: false } });
    const outcome = await runTurn("parkir 5rb", "client-1", deps({ submit: vi.fn(async () => ({ capture })) }));
    expect(outcome.reply?.capture?.question?.prompt).toBe("Dari rekening mana?");
    expect(outcome.reply?.text).toBeNull();
  });

  it.each(["berapa keluar 2 hari ini?", "berapa keluar 2 hari ini", "how much 20k", "parkir 5rb?"])("never records the question %s", async (text) => {
    const d = deps();
    const outcome = await runTurn(text, "client-1", d);
    expect(outcome.route).toBe("unknown");
    expect(d.parse).not.toHaveBeenCalled();
    expect(d.submit).not.toHaveBeenCalled();
  });

  it("asks for the amount when the parser read a transaction without one, and never submits", async () => {
    const d = deps({ parse: vi.fn(async () => ({ preview: preview({ amount: null, problems: ["missing_amount"], time_phrase: "tadi" }) })) });
    const outcome = await runTurn("parkir tadi", "client-1", d);
    expect(outcome).toMatchObject({ route: "ask_amount", reply: { text: ASK_AMOUNT_TEXT } });
    expect(d.submit).not.toHaveBeenCalled();
  });

  it("asks for the amount on an account or category hint too", async () => {
    const hint = deps({ parse: vi.fn(async () => ({ preview: preview({ amount: null, problems: ["missing_amount"], time_phrase: null, occurred_at: null,
      category_hint: { category_id: "c-1", label: "Transportasi", pillar: "need" } }) })) });
    expect((await runTurn("bensin", "client-1", hint)).route).toBe("ask_amount");
  });

  it("asks for one amount when there are several and never submits", async () => {
    const d = deps({ parse: vi.fn(async () => ({ preview: preview({ amount: null, problems: ["several_amounts"], time_phrase: null, occurred_at: null }) })) });
    const outcome = await runTurn("kopi 18rb roti 12rb", "client-1", d);
    expect(outcome.reply?.text).toBe(SEVERAL_AMOUNTS_TEXT);
    expect(d.submit).not.toHaveBeenCalled();
  });

  it("answers unknown text with the chips after exactly one parse call and nothing else", async () => {
    // What the server parser really returns for "halo": an expense described as itself, with nothing else found.
    const d = deps({ parse: vi.fn(async () => ({ preview: preview({ amount: null, problems: ["missing_amount"], description: "Halo", time_phrase: null, occurred_at: null }) })) });
    const outcome = await runTurn("halo", "client-1", d);
    expect(outcome.reply).toMatchObject({ text: UNKNOWN_TEXT, chips: SUGGESTION_CHIPS, card: null, capture: null });
    expect(d.parse).toHaveBeenCalledTimes(1);
    expect(d.submit).not.toHaveBeenCalled();
    expect(d.loadCard).not.toHaveBeenCalled();
  });

  it("offers Catat sebagai transaksi for a question that also holds a number", async () => {
    const d = deps();
    const outcome = await runTurn("tagihan listrik 200rb", "client-1", d);
    expect(outcome.reply?.chips).toEqual(["capture"]);
    expect(outcome.reply?.captureText).toBe("tagihan listrik 200rb");
    expect(d.submit).not.toHaveBeenCalled();
    const forced = await runTurn("tagihan listrik 200rb", "client-2", d, { forceCapture: true });
    expect(forced.route).toBe("capture");
    expect(d.submit).toHaveBeenCalledWith("tagihan listrik 200rb", "client-2");
  });

  it("keeps the text and records nothing when parse is rate limited or fails", async () => {
    const limited = deps({ parse: vi.fn(async () => { throw new ApiError(429, { code: "capture_rate_limited" }); }) });
    expect(await runTurn("parkir 5rb", "client-1", limited)).toMatchObject({ restoreText: true, reply: { notice: NOTICE_RATE_LIMITED } });
    const offline = deps({ parse: vi.fn(async () => { throw new TypeError("Failed to fetch"); }) });
    expect(await runTurn("parkir 5rb", "client-1", offline)).toMatchObject({ restoreText: true, reply: { notice: NOTICE_PARSE_FAILED } });
    expect(limited.submit).not.toHaveBeenCalled();
    expect(offline.submit).not.toHaveBeenCalled();
  });

  it("marks the message failed on a network error, and a resend reuses the same client id", async () => {
    const submit = vi.fn()
      .mockRejectedValueOnce(new TypeError("Failed to fetch"))
      .mockResolvedValueOnce({ capture: recorded() });
    const d = deps({ submit });
    expect(await runTurn("parkir 5rb", "client-9", d)).toMatchObject({ sendFailed: true, reply: null });
    expect((await runTurn("parkir 5rb", "client-9", d)).route).toBe("capture");
    expect(submit.mock.calls.map((call) => call[1])).toEqual(["client-9", "client-9"]);
  });

  it("keeps the text when recording is rate limited", async () => {
    const d = deps({ submit: vi.fn(async () => { throw new ApiError(429, { code: "capture_rate_limited" }); }) });
    expect(await runTurn("parkir 5rb", "client-1", d)).toMatchObject({ restoreText: true, reply: { notice: NOTICE_RATE_LIMITED } });
  });

  it("sends nothing for empty text", async () => {
    const d = deps();
    expect((await runTurn("   ", "client-1", d)).reply).toBeNull();
    expect(d.parse).not.toHaveBeenCalled();
  });
});
