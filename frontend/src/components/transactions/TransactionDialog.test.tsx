import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import type { Language } from "@/i18n/language";
import { ApiError, api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { TransactionDialog } from "./TransactionDialog";
import type { CreditBlock, CreditSettledItem, LedgerRowDetail } from "./types";

vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  api: { get: vi.fn(), post: vi.fn(), del: vi.fn() },
}));

let hideBalances = false;
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ hideBalances, bal: (amount: number) => (hideBalances ? "Rp ••••••" : `Rp ${amount}`) }),
}));

function settled(overrides: Partial<CreditSettledItem>): CreditSettledItem {
  return {
    kind: "purchase", transaction_id: "purchase-1", title: "Kopi Senja", installment: { number: 1, count: 1 },
    amount: 300_000, item_amount: 300_000, due_date: "2026-12-11", ...overrides,
  };
}

function payment(credit: Partial<CreditBlock>): LedgerRowDetail {
  return {
    id: "payment-1", type: "income", amount: 500_000, account_id: "spaylater", account_name: "SPayLater", category_id: null, category_name: "Transfer",
    category_system_key: "internal_movement", notes: "Bayar SPayLater", date: "2026-11-07T05:00:00.000Z", movement_id: "movement-1", movement_role: "inbound",
    transfer_target_account_id: "jago", transfer_target_account_name: "Jago", goal_id: null, goal_name: null, obligation_id: null, obligation_name: null,
    receipt_path: null, created_at: "2026-11-07T05:00:00.000Z", review_status: "reviewed",
    credit: { role: "payment", account_id: "spaylater", account_name: "SPayLater", ...credit },
  } as LedgerRowDetail;
}

const purchase = {
  id: "purchase-1", type: "expense", amount: 300_000, account_id: "spaylater", account_name: "SPayLater", category_id: null, category_name: "Makan",
  merchant: "Kopi Senja", notes: null, date: "2026-11-05T05:00:00.000Z", goal_id: null, goal_name: null, obligation_id: null, obligation_name: null,
  receipt_path: null, created_at: "2026-11-05T05:00:00.000Z", review_status: "reviewed",
  credit: { role: "purchase", account_id: "spaylater", account_name: "SPayLater", installment_count: 1, settled_on: "2026-11-07", days_to_settle: 2 },
} as LedgerRowDetail;

function serve(row: LedgerRowDetail) {
  vi.mocked(api.get).mockImplementation(async (path) => {
    if (path === "/accounts") return { accounts: [] };
    if (path === `/transactions/${row.id}`) return { transaction: row };
    if (path === "/transactions/purchase-1") return { transaction: purchase };
    throw new Error(`Unexpected GET ${path}`);
  });
}

function dialogFor(id = "payment-1") {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return <QueryClientProvider client={queryClient}><TransactionDialog transactionId={id} onClose={vi.fn()} record={vi.fn()} /></QueryClientProvider>;
}

async function show(row: LedgerRowDetail, language?: Language) {
  serve(row);
  if (language) renderWithIntl(dialogFor(), { language });
  else render(dialogFor());
  return screen.findByRole("dialog", { name: "Bayar SPayLater" });
}

describe("What a payment paid (show-credit-status-and-payment-settlement)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    hideBalances = false;
  });

  it("lists whole items, a partial share, an instalment, opening debt and the part kept for later", async () => {
    const dialog = await show(payment({
      settled_items: [
        settled({}),
        settled({ transaction_id: "purchase-2", title: "Sepatu lari", installment: { number: 3, count: 6 }, amount: 100_000, item_amount: 100_000 }),
        settled({ transaction_id: "purchase-3", title: "Tas kerja", amount: 70_000, item_amount: 200_000 }),
        settled({ kind: "opening", transaction_id: null, title: null, installment: null, amount: 30_000, item_amount: 30_000 }),
      ],
      kept_for_later: 30_000,
    }));
    const list = within(await within(dialog).findByRole("region", { name: "Dibayar oleh pembayaran ini" }));
    expect(list.getByRole("button", { name: /Kopi Senja.*Jatuh tempo 11 Des.*Rp\s?300\.000/ })).toBeInTheDocument();
    expect(list.getByRole("button", { name: /Sepatu lari.*Cicilan 3\/6.*jatuh tempo 11 Des.*Rp\s?100\.000/ })).toBeInTheDocument();
    expect(list.getByRole("button", { name: /Tas kerja.*Rp\s?70\.000.*dari Rp\s?200\.000/ })).toBeInTheDocument();
    expect(list.getAllByRole("button")).toHaveLength(3);
    expect(list.getByText("Saldo awal")).toBeInTheDocument();
    expect(list.getByText("Disimpan untuk tagihan berikutnya").closest("li")).toHaveTextContent(/Rp\s?30\.000/);
    expect((await axe(dialog)).violations).toHaveLength(0);
  });

  it("speaks English in the English interface", async () => {
    const dialog = await show(payment({
      settled_items: [
        settled({ amount: 100_000 }),
        settled({ kind: "opening", transaction_id: null, title: null, installment: null, amount: 30_000, item_amount: 30_000 }),
      ],
      kept_for_later: 20_000,
    }), "en");
    const list = within(await within(dialog).findByRole("region", { name: "Paid by this payment" }));
    expect(list.getByRole("button", { name: /Kopi Senja.*Due 11 Dec/ })).toHaveTextContent(/of Rp\s?300\.000/);
    expect(list.getByText("Opening balance")).toBeInTheDocument();
    expect(list.getByText("Kept for later")).toBeInTheDocument();
  });

  it("names a fee and a cash advance by their kind when they have no words of their own", async () => {
    const dialog = await show(payment({
      settled_items: [
        settled({ kind: "fee", transaction_id: "fee-1", title: null, installment: null, amount: 5_000, item_amount: 5_000 }),
        settled({ kind: "cash_advance", transaction_id: "advance-1", title: "Pindah saldo", installment: null, amount: 40_000, item_amount: 40_000 }),
        settled({ kind: "cash_advance", transaction_id: "advance-2", title: "Tarik untuk kontrakan", installment: null, amount: 40_000, item_amount: 40_000 }),
        settled({ transaction_id: "purchase-4", title: null, amount: 40_000, item_amount: 40_000 }),
      ],
      kept_for_later: 0,
    }));
    const list = within(await within(dialog).findByRole("region", { name: "Dibayar oleh pembayaran ini" }));
    expect(list.getByRole("button", { name: /^Biaya kredit/ })).toBeInTheDocument();
    expect(list.getByRole("button", { name: /^Tarik tunai/ })).toBeInTheDocument();
    expect(list.getByRole("button", { name: /^Tarik untuk kontrakan/ })).toBeInTheDocument();
    expect(list.getByRole("button", { name: /^Tanpa keterangan/ })).toBeInTheDocument();
  });

  it("opens the purchase from a row and moves focus to its detail", async () => {
    const user = userEvent.setup();
    const dialog = await show(payment({ settled_items: [settled({})], kept_for_later: 0 }));
    await user.click(await within(dialog).findByRole("button", { name: /Kopi Senja/ }));
    expect(await screen.findByRole("dialog", { name: "Kopi Senja" })).toBeInTheDocument();
    await waitFor(() => expect(document.activeElement).toBe(dialog.querySelector(".dlg-b")));
    expect(api.get).toHaveBeenCalledWith("/transactions/purchase-1");
  });

  it.each([["Enter", "{Enter}"], ["Space", " "]])("opens a row with %s", async (_name, key) => {
    const user = userEvent.setup();
    const dialog = await show(payment({ settled_items: [settled({})], kept_for_later: 0 }));
    const row = await within(dialog).findByRole("button", { name: /Kopi Senja/ });
    row.focus();
    expect(row).toHaveFocus();
    await user.keyboard(key);
    expect(await screen.findByRole("dialog", { name: "Kopi Senja" })).toBeInTheDocument();
    await waitFor(() => expect(document.activeElement).toBe(dialog.querySelector(".dlg-b")));
  });

  it("does not carry a focus move over to the next opening when the row is gone", async () => {
    const user = userEvent.setup();
    const row = payment({ settled_items: [settled({})], kept_for_later: 0 });
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [] };
      if (path === "/transactions/payment-1") return { transaction: row };
      throw new ApiError(404, "not found");
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const dialogWith = (id: string | null) => (
      <QueryClientProvider client={queryClient}><TransactionDialog transactionId={id} onClose={vi.fn()} record={vi.fn()} /></QueryClientProvider>
    );
    const view = render(dialogWith("payment-1"));
    const dialog = await screen.findByRole("dialog", { name: "Bayar SPayLater" });
    await user.click(await within(dialog).findByRole("button", { name: /Kopi Senja/ }));
    await within(dialog).findByRole("alert");
    view.rerender(dialogWith(null));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    view.rerender(dialogWith("payment-1"));
    const reopened = await screen.findByRole("dialog", { name: "Bayar SPayLater" });
    await within(reopened).findByRole("button", { name: /Kopi Senja/ });
    expect(document.activeElement).not.toBe(reopened.querySelector(".dlg-b"));
  });

  it("hides every amount with Sembunyikan saldo", async () => {
    hideBalances = true;
    const dialog = await show(payment({ settled_items: [settled({ amount: 100_000 })], kept_for_later: 25_000 }));
    const region = await within(dialog).findByRole("region", { name: "Dibayar oleh pembayaran ini" });
    expect(region).not.toHaveTextContent(/\d{2,3}[.,]\d{3}/);
    expect(within(region).getAllByRole("img", { name: "Disembunyikan" })).toHaveLength(3);
  });

  it("shows nothing extra when the account has no payment day", async () => {
    const dialog = await show(payment({ settled_items: [], kept_for_later: 0, due_dates_unavailable: true }));
    expect(within(dialog).queryByRole("region", { name: "Dibayar oleh pembayaran ini" })).not.toBeInTheDocument();
  });

  it("shows a payment that covered nothing yet as kept for later only", async () => {
    const dialog = await show(payment({ settled_items: [], kept_for_later: 500_000 }));
    const region = await within(dialog).findByRole("region", { name: "Dibayar oleh pembayaran ini" });
    expect(within(region).queryAllByRole("button")).toHaveLength(0);
    expect(region).toHaveTextContent(/Rp\s?500\.000/);
  });
});
