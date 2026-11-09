import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Language } from "@/i18n/language";
import { renderWithIntl } from "@/lib/i18n/testing";
import { configureApiMessages } from "@/lib/i18n/runtime";
import en from "@/messages/en.json";
import id from "@/messages/id.json";
import { MovementEditDialog } from "./MovementEditDialog";
import type { LedgerRowDetail } from "./types";

vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ hideBalances: false, bal: (amount: number) => `Rp ${amount}` }) }));

let accounts: Record<string, unknown>[];
const requests: { method: string; path: string; body: unknown }[] = [];
let movementResponse: { status: number; body: unknown } = { status: 200, body: { ok: true } };

function row(overrides: Partial<LedgerRowDetail> = {}): LedgerRowDetail {
  return {
    id: "out-1", type: "expense", amount: 500, account_id: "account-source", account_name: "Cash", category_id: null, category_name: "Transfer",
    category_system_key: "internal_movement", notes: "Move savings", date: "2026-09-20T12:00:27.000Z", movement_id: "movement-1", movement_role: "outbound",
    transfer_target_account_id: "account-target", transfer_target_account_name: "Bank", goal_id: null, goal_name: null, obligation_id: null,
    obligation_name: null, receipt_path: null, created_at: "2026-09-20T12:00:27.000Z", ...overrides,
  } as LedgerRowDetail;
}

function show(tx: LedgerRowDetail, language: Language = "id", handlers: { onSaved?: () => void; onSplit?: () => void } = {}) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  renderWithIntl(
    <QueryClientProvider client={queryClient}>
      <MovementEditDialog row={tx} route={{ source: "Cash", target: "Bank" }} onClose={vi.fn()} onSaved={handlers.onSaved ?? vi.fn()} onSplit={handlers.onSplit ?? vi.fn()} />
    </QueryClientProvider>,
    { language },
  );
  return userEvent.setup();
}

beforeEach(() => {
  requests.length = 0;
  movementResponse = { status: 200, body: { ok: true } };
  accounts = [
    { id: "account-source", name: "Cash", type: "cash", balance: 1_000 },
    { id: "account-target", name: "Bank", type: "bank", balance: 0 },
    { id: "investment-position", name: "Stock", type: "investment", balance: 0 },
  ];
  // The real client normalizes error codes, so the dialog is tested through the same path as production.
  vi.stubGlobal("fetch", vi.fn(async (url: string, init: RequestInit) => {
    const path = url.replace(/^\/api/, "");
    requests.push({ method: init.method ?? "GET", path, body: init.body ? JSON.parse(String(init.body)) : undefined });
    if (path === "/accounts") return new Response(JSON.stringify({ accounts }), { status: 200 });
    return new Response(JSON.stringify(movementResponse.body), { status: movementResponse.status });
  }));
});

afterEach(() => {
  vi.unstubAllGlobals();
  configureApiMessages("id", id);
});

describe("Ubah pindah saldo (D16)", () => {
  it("updates both legs through the movement contract with seconds, and offers no investment position", async () => {
    const onSaved = vi.fn();
    const user = show(row(), "id", { onSaved });
    const dialog = await screen.findByRole("dialog", { name: "Ubah pindah saldo" });
    await waitFor(() => expect(within(dialog).getByRole("combobox", { name: "Dari rekening" })).toHaveValue("account-source"));
    expect(within(dialog).queryByRole("option", { name: /Stock/ })).not.toBeInTheDocument();
    const date = within(dialog).getByLabelText("Tanggal dan waktu") as HTMLInputElement;
    expect(date).toHaveAttribute("step", "1");
    expect(date.value).toMatch(/:27(?:\.000)?$/);
    await user.clear(within(dialog).getByRole("textbox", { name: "Nominal (IDR)" }));
    await user.type(within(dialog).getByRole("textbox", { name: "Nominal (IDR)" }), "750");
    await user.clear(within(dialog).getByRole("textbox", { name: "Catatan" }));
    await user.type(within(dialog).getByRole("textbox", { name: "Catatan" }), "Move reserve");
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const patch = requests.find((request) => request.method === "PATCH");
    expect(patch).toMatchObject({ path: "/movements/movement-1", body: { source_account_id: "account-source", target_account_id: "account-target", amount: 750, notes: "Move reserve" } });
    expect(new Date((patch!.body as { date: string }).date).toISOString()).toBe("2026-09-20T12:00:27.000Z");
  });

  it("splits a wrongly paired movement after a confirmation in the footer", async () => {
    const onSplit = vi.fn();
    const user = show(row(), "id", { onSplit });
    const dialog = await screen.findByRole("dialog", { name: "Ubah pindah saldo" });
    await user.click(within(dialog).getByRole("button", { name: "Pisahkan jadi dua transaksi" }));
    expect(requests.some((request) => request.path.endsWith("/split"))).toBe(false);
    expect(dialog).toHaveTextContent("Pisahkan jadi dua transaksi? Kedua catatan tetap ada.");
    await waitFor(() => expect(within(dialog).getByRole("button", { name: "Batal" })).toHaveFocus());
    await user.click(within(dialog).getByRole("button", { name: "Ya, pisahkan" }));
    await waitFor(() => expect(onSplit).toHaveBeenCalled());
    expect(requests.find((request) => request.path === "/movements/movement-1/split")?.method).toBe("POST");
    expect(requests.some((request) => request.method === "DELETE")).toBe(false);
  });

  it("keeps a credit payment's target, sends no fee, and never pairs two credit accounts", async () => {
    accounts = [
      { id: "bayar-kredit", name: "Bayar Kredit", type: "bank", balance: 30_000 },
      { id: "spaylater", name: "SPayLater", type: "credit", balance: 0, credit_owed: 0 },
      { id: "kredivo", name: "Kredivo", type: "credit", balance: -100_000, credit_owed: 100_000 },
    ];
    const user = show(row({ id: "payment-1", movement_id: "payment-1", account_id: "bayar-kredit", transfer_target_account_id: "spaylater", amount: 470_000, notes: null }));
    const dialog = await screen.findByRole("dialog", { name: "Ubah pindah saldo" });
    const target = within(dialog).getByRole("combobox", { name: "Ke rekening" });
    await waitFor(() => expect(target).toHaveValue("spaylater"));
    expect(within(target).getByRole("group", { name: "Kredit" })).toBeInTheDocument();
    expect(within(dialog).queryByRole("textbox", { name: /Biaya/ })).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(requests.find((request) => request.method === "PATCH")).toBeDefined());
    const body = requests.find((request) => request.method === "PATCH")!.body as Record<string, unknown>;
    expect(body).toMatchObject({ source_account_id: "bayar-kredit", target_account_id: "spaylater", amount: 470_000 });
    expect(body).not.toHaveProperty("fee");
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Dari rekening" }), "kredivo");
    expect(within(within(dialog).getByRole("combobox", { name: "Ke rekening" })).queryByRole("group", { name: "Kredit" })).not.toBeInTheDocument();
    await waitFor(() => expect(within(dialog).getByRole("combobox", { name: "Ke rekening" })).toHaveValue("bayar-kredit"));
  });

  it.each([
    ["id", "Rekening sumber dan tujuan harus berbeda."],
    ["en", "Source and target accounts must be different."],
  ] as const)("shows the server's same-account refusal from its code in %s", async (language, text) => {
    configureApiMessages(language, language === "en" ? en : id);
    movementResponse = { status: 400, body: { detail: { code: "same_account", params: {}, message: "Source and target accounts must be different." } } };
    const user = show(row(), language);
    const dialog = await screen.findByRole("dialog", { name: language === "en" ? "Edit transfer" : "Ubah pindah saldo" });
    await waitFor(() => expect(within(dialog).getByRole("combobox", { name: language === "en" ? "From account" : "Dari rekening" })).toHaveValue("account-source"));
    await user.click(within(dialog).getByRole("button", { name: language === "en" ? "Save" : "Simpan" }));
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(text);
    if (language === "id") expect(dialog).not.toHaveTextContent("Source and target accounts must be different.");
  });

  it("checks the form in the interface language before sending anything", async () => {
    const user = show(row(), "en");
    const dialog = await screen.findByRole("dialog", { name: "Edit transfer" });
    await waitFor(() => expect(within(dialog).getByRole("combobox", { name: "From account" })).toHaveValue("account-source"));
    fireEvent.change(within(dialog).getByLabelText("Date and time"), { target: { value: "" } });
    await user.click(within(dialog).getByRole("button", { name: "Save" }));
    expect(await within(dialog).findByRole("alert")).toHaveTextContent("Choose the time of the transfer");
    expect(requests.some((request) => request.method === "PATCH")).toBe(false);
  });
});
