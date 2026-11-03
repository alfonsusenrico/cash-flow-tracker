import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "@/components/workspace/overlays";
import { ShortcutProvider } from "@/components/workspace/shortcuts";
import { bindThreadUser, clearThread } from "@/lib/tanya/thread";
import { TanyaView } from "./TanyaView";

/**
 * No model call (add-tanya-conversation D12): a whole scripted session through the real API client with
 * `fetch` stubbed. Every request must be a same-origin `/api/` path on the allowlist; no absolute URL and
 * no model provider's host may appear.
 */

vi.mock("next/navigation", () => ({ usePathname: () => "/tanya", useRouter: () => ({ push: vi.fn(), replace: vi.fn() }) }));
vi.mock("@/components/transactions/TransactionDrawer", () => ({ TransactionDrawer: () => null }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ user: { id: "user-1", name: "Raka" }, hideBalances: false, reviewAttention: 2, bal: (amount: number) => `Rp ${amount}` }),
}));

const ALLOWED = [
  /^\/api\/dashboard\/cycle-summary$/,
  /^\/api\/review\/summary$/,
  /^\/api\/review\/queue$/,
  /^\/api\/reports\/monthly\/latest$/,
  /^\/api\/auth\/me$/,
  /^\/api\/capture\/parse$/,
  /^\/api\/capture\/text$/,
  /^\/api\/capture\/[\w-]+\/answer$/,
  /^\/api\/capture\/[\w-]+\/undo$/,
];
const MODEL_HOSTS = /openai|anthropic|claude|gemini|googleapis|generativelanguage|mistral|cohere|groq|ollama|deepseek|together|huggingface|openrouter/i;

const summary = {
  ok: true, as_of: "", period: { basis: "cycle", status: "open", cycle_status: "open", start_date: "2026-10-25", today: "2026-11-03", day_index: 10, label: "Siklus sejak 25 Okt", cycle_offset_basis: "cycle" },
  expected_salary: { status: "estimated", date: "2026-11-25", basis: null, receipt_dates: [], reason: null },
  keluar: { total: 100, offsets_total: 0, lines: [{ key: "need", label: "Kebutuhan", amount: 100 }] },
  masuk: { total: 500, lines: [] },
  waiting: { count: 0, total: 0, in_keluar: 0, not_in_keluar: 0, lines: [] },
  scheduled: { total: 0, window_start: "2026-11-03", window_end: "2026-11-25", window_basis: "expected_salary", lines: [] },
  remaining: { total: 400, lines: [] },
  safe_to_spend: { status: "available", reason: null, detail: null, per_day: 20, days_left: 20, rounding: 1, over_by: null },
  net_worth: { total: 1000, liquid: 1000, investment: 0, debts: 0, lines: [] },
  comparison: { status: "no_previous", compared_days: 0, label: null, previous_label: null, keluar: { current: 100, previous: null, change: null }, masuk: { current: 500, previous: null, change: null }, belanja: { current: 100, previous: null, change: null } },
};
const capture = (status: string) => ({
  id: "cap-1", status, summary: { amount: 5000, description: "Parkir", time_label: "14.00" }, undo_until: new Date(Date.now() + 10_000).toISOString(),
  question: status === "needs_detail" ? { question_id: "q-1", type: "account", prompt: "Dari rekening mana?", options: [{ id: "a-1", label: "Tunai" }], allows_text: false } : null,
  transaction: status === "recorded" ? { id: "tx-1", type: "expense", amount: 5000, date: "", account_name: "Tunai", merchant: null, notes: "Parkir", category_name: null, kakeibo: null, review_status: "reviewed" } : null,
});

let requested: string[];
let failSummary = false;

function respond(url: string, init?: RequestInit): Response {
  const path = new URL(url, "http://localhost").pathname;
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
  if (path === "/api/dashboard/cycle-summary") return failSummary ? json({ detail: "x" }, 500) : json(summary);
  if (path === "/api/review/summary") return json({ ok: true, attention: 0 });
  if (path === "/api/review/queue") return json({ ok: true, truncated: false, counts: { attention: 0, unreviewed: 0, questions: 0 }, questions: [], groups: [], awaiting: [], ambiguous: [] });
  if (path === "/api/reports/monthly/latest") return json({ ready: false, period: null, url: null });
  if (path === "/api/capture/parse") {
    const text = JSON.parse(String(init?.body)).text as string;
    const amount = /\d/.test(text) ? 5000 : null;
    return json({ ok: true, preview: { amount, problems: amount ? [] : ["missing_amount"], kind: "expense", description: text, occurred_at: null, time_phrase: null, time_options: [], account_hint: null, category_hint: null } });
  }
  if (path === "/api/capture/text") return json({ ok: true, capture: capture("needs_detail") });
  if (path === "/api/capture/cap-1/answer") return json({ ok: true, capture: capture("recorded") });
  if (path === "/api/capture/cap-1/undo") return json({ ok: true, capture: capture("undone") });
  return json({ detail: "unexpected" }, 404);
}

beforeEach(() => {
  requested = [];
  failSummary = false;
  bindThreadUser("user-1");
  clearThread();
  vi.stubGlobal("IntersectionObserver", undefined);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    requested.push(url);
    return respond(url, init);
  }));
});

afterEach(() => vi.unstubAllGlobals());

describe("Tanya network", () => {
  it("only ever calls same-origin /api/ paths on the allowlist, and no model host", async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={queryClient}>
        <ShortcutProvider>
          <ToastProvider>
            <TanyaView />
          </ToastProvider>
        </ShortcutProvider>
      </QueryClientProvider>,
    );
    const user = userEvent.setup();
    const composer = screen.getByRole("textbox", { name: "Tulis transaksi atau pertanyaan" });
    const log = screen.getByRole("log", { name: "Percakapan" });
    const turns = ["ringkasan", "kakeibo", "tagihan minggu ini", "menunggu jawaban", "laporan terbaru", "bantuan", "halo", "berapa keluar 2 hari ini?"];
    for (const [index, text] of turns.entries()) {
      await user.type(composer, `${text}{Enter}`);
      await waitFor(() => expect(within(log).getAllByRole("listitem").length).toBe((index + 1) * 2));
    }
    await user.type(composer, "parkir 5rb{Enter}");
    await user.click(await within(log).findByRole("button", { name: "Tunai" }));
    await user.click(await within(log).findByRole("button", { name: "Urungkan" }));
    await within(log).findByText("Dibatalkan");
    failSummary = true;
    await user.click(within(log).getAllByRole("button", { name: "Perbarui" })[0]);
    await within(log).findByText("Data ini tidak dapat dimuat.");

    expect(requested.length).toBeGreaterThan(10);
    for (const url of requested) {
      expect(url.startsWith("/api/"), url).toBe(true);
      expect(MODEL_HOSTS.test(url), url).toBe(false);
      expect(ALLOWED.some((pattern) => pattern.test(url)), url).toBe(true);
    }
    const paths = new Set(requested.map((url) => url.replace(/cap-1/, "{id}")));
    expect([...paths].sort()).toEqual([
      "/api/capture/parse", "/api/capture/text", "/api/capture/{id}/answer", "/api/capture/{id}/undo",
      "/api/dashboard/cycle-summary", "/api/reports/monthly/latest", "/api/review/queue", "/api/review/summary",
    ]);
  });
});
