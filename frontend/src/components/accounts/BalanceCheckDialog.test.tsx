import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { BalanceCheckDialog, canCheckBalance, type BalanceCheckResult, type BalanceCause } from "./BalanceCheckDialog";
import type { AccountItem } from "./types";

vi.mock("@/lib/api", () => ({ api: { post: vi.fn() } }));
vi.mock("next/link", () => ({ default: ({ href, children, ...rest }: { href: string; children: React.ReactNode }) => <a href={href} {...rest}>{children}</a> }));

const money = (value: number) => `Rp ${value.toLocaleString("id-ID")}`;
const pocket = { id: "main", name: "Kantong Utama", type: "bank", parent_id: "bank", balance: 900_000, initial_balance: 0 } as AccountItem;

const duplicate: BalanceCause = {
  key: "tx:dup", kind: "duplicate", effect: -500_000, amount: 500_000, direction: "income", at: "2026-11-01T01:39:02+07:00",
  label: "Transfer masuk", account: "Bank Contoh · Kantong Utama", app: "Bank Jago",
  link: { kind: "transaction", id: "dup" },
  twin: { transaction_id: "kept", account: "Dompet Contoh", app: "GoPay", at: "2026-11-01T01:39:00+07:00" },
};
const unpaired: BalanceCause = {
  key: "tx:leg", kind: "unpaired", effect: 75_000, amount: 75_000, direction: "expense", at: "2026-10-30T09:00:00+07:00",
  label: "Pindah saldo", account: "Bank Contoh · Kantong Utama", app: null, link: { kind: "transaction", id: "leg" }, twin: null,
};

function result(overrides: Partial<BalanceCheckResult>): BalanceCheckResult {
  return {
    account: { id: "main", name: "Kantong Utama", is_parent: false },
    actual_balance: 900_000, recorded_balance: 900_000, difference: 0,
    since: { at: "2026-10-05T00:00:00+07:00", basis: "window" },
    candidates: [], unattributed: [], explanation: null, last_report_at: "2026-11-04T15:15:00+07:00", capture_gap: false, can_adjust: true,
    ...overrides,
  };
}

function show(account: AccountItem = pocket) {
  const qc = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  const onAdjust = vi.fn();
  const view = render(
    <QueryClientProvider client={qc}>
      <BalanceCheckDialog account={account} money={money} onClose={vi.fn()} onAdjust={onAdjust} />
    </QueryClientProvider>,
  );
  return { ...view, onAdjust };
}

async function checkWith(user: ReturnType<typeof userEvent.setup>, value: string) {
  await user.type(screen.getByLabelText("Saldo di aplikasi bank (IDR)"), value);
  await user.click(screen.getByRole("button", { name: "Periksa" }));
}

describe("Cocokkan saldo", () => {
  beforeEach(() => vi.clearAllMocks());

  it("starts empty, asks for a balance, and stores a matched point for an exact match", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post)
      .mockResolvedValueOnce(result({}))
      .mockResolvedValueOnce({ matched_at: "2026-11-04T16:30:00+07:00", balance: 900_000 });
    const { container } = show();
    expect(screen.getByLabelText("Saldo di aplikasi bank (IDR)")).toHaveValue("");
    await user.click(screen.getByRole("button", { name: "Periksa" }));
    expect(screen.getByText("Masukkan saldo yang tampil di aplikasi bank.")).toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalled();

    await checkWith(user, "900000");
    expect(api.post).toHaveBeenCalledWith("/accounts/main/balance-check", { actual_balance: 900_000 });
    const facts = await screen.findByRole("region", { name: "Hasil pemeriksaan" });
    expect(within(facts).getByText("Cocok")).toBeInTheDocument();
    expect(within(facts).getByText("30 hari terakhir")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Saldo cocok" }));
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/accounts/main/balance-matches", { actual_balance: 900_000 }));
    expect(await screen.findByText("Saldo cocok disimpan. Pemeriksaan berikutnya dimulai dari sini.")).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("names the cause that explains the difference and links to its row", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockResolvedValueOnce(result({
      actual_balance: 400_000, recorded_balance: 900_000, difference: -500_000,
      since: { at: "2026-11-02T10:00:00+07:00", basis: "matched" },
      candidates: [duplicate, unpaired], explanation: ["tx:dup"],
    }));
    show();
    await checkWith(user, "400000");

    const explain = (await screen.findByText(/Selisih ini sama dengan penyebab berikut/)).parentElement as HTMLElement;
    expect(within(explain).getByText("Kemungkinan tercatat dua kali")).toBeInTheDocument();
    expect(within(explain).getByText(/juga tercatat di Dompet Contoh, GoPay/)).toBeInTheDocument();
    expect(within(explain).getByRole("link", { name: "Buka transaksi" })).toHaveAttribute("href", "/ledger?edit=dup");
    expect(screen.getByRole("heading", { name: "Kemungkinan penyebab lain" })).toBeInTheDocument();
    expect(screen.getByText("Pindah saldo tanpa pasangan")).toBeInTheDocument();
    expect(screen.getByText("−Rp 500.000")).toBeInTheDocument();
    expect(screen.getByText(/^Sejak saldo cocok/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Saldo cocok" })).not.toBeInTheDocument();
  });

  it("shows an unexplained difference without an explanation line", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockResolvedValueOnce(result({
      actual_balance: 875_000, recorded_balance: 900_000, difference: -25_000, candidates: [unpaired], capture_gap: true,
    }));
    show();
    await checkWith(user, "875000");

    expect(await screen.findByRole("heading", { name: "Kemungkinan penyebab" })).toBeInTheDocument();
    expect(screen.queryByText(/Selisih ini sama dengan/)).not.toBeInTheDocument();
    expect(screen.getByText(/Notifikasi terakhir dari HP diterima/)).toBeInTheDocument();
    expect(screen.getByText("−Rp 25.000")).toBeInTheDocument();
  });

  it("offers no adjustment for a parent, which is checked with its pockets", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockResolvedValueOnce(result({
      account: { id: "bank", name: "Bank Contoh", is_parent: true }, can_adjust: false,
      actual_balance: 950_000, recorded_balance: 1_000_000, difference: -50_000,
    }));
    show({ ...pocket, id: "bank", name: "Bank Contoh", parent_id: null, children: [pocket] });
    await checkWith(user, "950000");

    expect(await screen.findByText(/termasuk semua kantong/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Sesuaikan saldo" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Periksa lagi" })).toBeInTheDocument();
  });

  it("records a matched point and checks again after Sesuaikan saldo succeeds", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post)
      .mockResolvedValueOnce(result({ actual_balance: 920_000, recorded_balance: 900_000, difference: 20_000 }))
      .mockResolvedValueOnce({ matched_at: "2026-11-04T16:40:00+07:00", balance: 920_000 })
      .mockResolvedValueOnce(result({ actual_balance: 920_000, recorded_balance: 920_000, difference: 0, since: { at: "2026-11-04T16:40:00+07:00", basis: "matched" } }));
    const { onAdjust } = show();
    await checkWith(user, "920000");
    await user.click(await screen.findByRole("button", { name: "Sesuaikan saldo" }));

    expect(onAdjust).toHaveBeenCalledWith(920_000, expect.any(Function));
    onAdjust.mock.calls[0][1](920_000);
    await waitFor(() => expect(api.post).toHaveBeenNthCalledWith(2, "/accounts/main/balance-matches", { actual_balance: 920_000 }));
    await waitFor(() => expect(api.post).toHaveBeenNthCalledWith(3, "/accounts/main/balance-check", { actual_balance: 920_000 }));
    expect(await screen.findByText("Cocok")).toBeInTheDocument();
  });
});

describe("Cocokkan saldo in English", () => {
  beforeEach(() => vi.clearAllMocks());

  it("reads the result and causes in English and keeps the requests", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockResolvedValueOnce(result({
      actual_balance: 875_000, recorded_balance: 900_000, difference: -25_000, candidates: [unpaired],
    }));
    const qc = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
    const onAdjust = vi.fn();
    renderWithIntl(
      <QueryClientProvider client={qc}>
        <BalanceCheckDialog account={pocket} money={money} onClose={vi.fn()} onAdjust={onAdjust} />
      </QueryClientProvider>,
      { language: "en" },
    );
    expect(screen.getByRole("dialog", { name: "Check balance" })).toHaveTextContent("Compare Kantong Utama with the balance in the bank app");
    await user.type(screen.getByLabelText("Balance in the bank app (IDR)"), "875000");
    await user.click(screen.getByRole("button", { name: "Check" }));
    expect(api.post).toHaveBeenCalledWith("/accounts/main/balance-check", { actual_balance: 875_000 });
    const region = await screen.findByRole("region", { name: "Check result" });
    expect(region).toHaveTextContent("difference: the bank app shows less");
    expect(within(region).getByText("Transfer without its other side")).toBeInTheDocument();
    expect(within(region).getByRole("link", { name: "Open transaction" })).toHaveAttribute("href", "/ledger?edit=leg");
    await user.click(screen.getByRole("button", { name: "Adjust balance" }));
    expect(onAdjust).toHaveBeenCalledWith(875_000, expect.any(Function));
  });
});

describe("canCheckBalance", () => {
  it("allows liquid accounts and pockets, and parents whose pockets are all liquid", () => {
    expect(canCheckBalance(pocket)).toBe(true);
    expect(canCheckBalance({ ...pocket, children: [pocket] })).toBe(true);
    expect(canCheckBalance({ ...pocket, type: "investment" } as AccountItem)).toBe(false);
    expect(canCheckBalance({ ...pocket, investment_tracking_mode: "amount" })).toBe(false);
    expect(canCheckBalance({ ...pocket, children: [{ ...pocket, id: "fund", type: "investment", instrument_type: "mutual_fund" } as AccountItem] })).toBe(false);
  });

  it("leaves credit accounts out, because their balance is what is owed", () => {
    const credit = { ...pocket, id: "spaylater", name: "SPayLater", type: "credit", parent_id: null, balance: -470_000, credit_owed: 470_000 } as AccountItem;
    expect(canCheckBalance(credit)).toBe(false);
  });
});
