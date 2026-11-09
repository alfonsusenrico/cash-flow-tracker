import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactElement } from "react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import type { InvestmentScheduleDraft } from "@/lib/investmentTopups";
import type { ExpensePlanDraft } from "@/types/recurring";
import { DuePlanActions } from "./DuePlanActions";
import { PlansDialog } from "./PlansDialog";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() },
}));

const weeklyExpense = {
  id: "rule-1",
  name: "Belanja mingguan",
  type: "expense",
  amount: 50_000,
  source_account_id: "account-1",
  source_account_name: "BCA",
  target_account_id: null,
  category_id: "category-expense",
  category_name: "Makan",
  obligation_id: null,
  schedule_type: "weekly",
  schedule_day: 1,
  is_payroll_allocation: false,
  auto_post: false,
  is_active: true,
  next_due_date: "2026-09-28",
  notes: null,
};

const monthlyTransfer = {
  ...weeklyExpense,
  id: "rule-2",
  name: "Tabungan bulanan",
  type: "transfer",
  amount: 1_000_000,
  target_account_id: "account-2",
  target_account_name: "Jago",
  category_id: null,
  category_name: null,
  schedule_type: "monthly_day",
  schedule_day: 25,
  next_due_date: "2099-01-25",
};

const candidate = (id: string, amount: number) => ({
  id, amount, date: "2026-10-01T03:00:00+00:00", notes: null, merchant: "Listrik Contoh",
  source: "notification", account_name: "BCA", exact_amount: amount === 250_000,
});

const dueBill = {
  ...weeklyExpense,
  id: "bill-rule", name: "Listrik bulanan", amount: 250_000, schedule_type: "monthly_day", schedule_day: 1,
  next_due_date: "2026-10-01", window_days: 7, match_state: "single" as const,
  candidates: [candidate("tx-1", 250_000), candidate("tx-3", 90_000)],
};

let pending: unknown[] = [];

function client() {
  return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
}

function renderRules(
  { investmentDraft, expenseDraft, planId }: { investmentDraft?: InvestmentScheduleDraft; expenseDraft?: ExpensePlanDraft; planId?: string } = {},
  draw: (ui: ReactElement) => ReturnType<typeof render> = render,
) {
  return draw(
    <QueryClientProvider client={client()}>
      <PlansDialog open onClose={() => {}} investmentDraft={investmentDraft} expenseDraft={expenseDraft} planId={planId} />
    </QueryClientProvider>,
  );
}

describe("PlansDialog", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    pending = [];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/recurring") return { ok: true, rules: [weeklyExpense, monthlyTransfer] };
      if (path === "/recurring/pending") return { ok: true, rules: pending };
      if (path === "/accounts") return { accounts: [
        { id: "account-1", name: "BCA", type: "bank", balance: 100_000 },
        { id: "account-2", name: "Jago", type: "bank", balance: 25_000 },
        { id: "position-1", name: "BBCA", type: "investment", balance: 50_000 },
        { id: "fund-1", name: "Bibit A", type: "investment", instrument_type: "mutual_fund", units: null },
        { id: "fund-2", name: "Bibit B", type: "investment", instrument_type: "mutual_fund", units: null },
        { id: "fund-units", name: "Fund with units", type: "investment", instrument_type: "mutual_fund", units: 10 },
      ] };
      if (path === "/categories") return { categories: [
        { id: "category-expense", name: "Makan", kind: "expense" },
        { id: "category-income", name: "Gaji", kind: "income" },
      ] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
  });

  it("lists plans with their status and schedule beside an empty detail, and reads only the active debts", async () => {
    const { container } = renderRules();
    const list = await screen.findByRole("list", { name: "Daftar rencana" });
    expect(within(list).getByRole("button", { name: /Belanja mingguan.*terlewat · 28 Sep · Mingguan/ })).toBeInTheDocument();
    expect(within(list).getByRole("button", { name: /Tabungan bulanan.*terjadwal · 25 Jan · Tiap tgl 25/ })).toBeInTheDocument();
    expect(screen.getByText("2 rencana · pengingat, bukan pembayaran otomatis kecuali ditandai")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Pilih rencana" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Simpan" })).not.toBeInTheDocument();
    await waitFor(() => expect(api.get).toHaveBeenCalledWith("/obligations"));
    expect(api.get).not.toHaveBeenCalledWith(expect.stringContaining("include_archived"));
    await userEvent.setup().click(screen.getByRole("radio", { name: "Cicilan" }));
    expect(screen.getByText("Tidak ada rencana untuk saringan ini.")).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("creates a weekly liquid transfer plan without posting or payroll options", async () => {
    const user = userEvent.setup();
    renderRules();
    await screen.findByRole("list", { name: "Daftar rencana" });
    await user.click(screen.getAllByRole("button", { name: "Tambah rencana" })[0]);

    const source = screen.getByRole("combobox", { name: "Dari rekening" });
    await waitFor(() => expect(within(source).getByRole("option", { name: "BCA" })).toBeInTheDocument());
    await user.selectOptions(source, "account-1");
    const target = screen.getByRole("combobox", { name: "Ke rekening / kantong tujuan" });
    await user.selectOptions(target, "account-2");
    expect(screen.queryByRole("option", { name: "BBCA" })).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox", { name: /alokasi gaji|otomatis/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("option", { name: /gajian/i })).not.toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "Nama rencana" }), "Pindah ke Jago");
    await user.type(screen.getByRole("textbox", { name: "Nominal" }), "10000");
    await user.selectOptions(screen.getByRole("combobox", { name: "Jadwal pengulangan" }), "weekly");
    await user.selectOptions(screen.getByRole("combobox", { name: "Hari pengulangan" }), "5");
    await user.click(screen.getByRole("button", { name: "Simpan" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring", expect.objectContaining({
      type: "transfer",
      amount: 10_000,
      target_account_id: "account-2",
      category_id: null,
      schedule_type: "weekly",
      schedule_day: 5,
    })));
    const payload = vi.mocked(api.post).mock.calls[0][1] as Record<string, unknown>;
    expect(payload).not.toHaveProperty("auto_post");
    expect(payload).not.toHaveProperty("is_payroll_allocation");
    expect(await screen.findByRole("heading", { name: "Pilih rencana" })).toBeInTheDocument();
  });

  it("prefills a detected recurring charge and saves it only on confirmation", async () => {
    const user = userEvent.setup();
    renderRules({ expenseDraft: {
      name: "Musik Contoh", type: "expense", amount: 54_990, source_account_id: "account-2",
      category_id: "category-expense", schedule_type: "monthly_day", schedule_day: 28,
    } });
    const name = await screen.findByRole("textbox", { name: "Nama rencana" });
    expect(name).toHaveValue("Musik Contoh");
    expect(screen.getByRole("textbox", { name: "Nominal" })).toHaveValue("54.990");
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("account-2"));
    expect(api.post).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring", expect.objectContaining({
      type: "expense", amount: 54_990, source_account_id: "account-2", category_id: "category-expense",
      schedule_type: "monthly_day", schedule_day: 28,
    })));
    expect(vi.mocked(api.post).mock.calls[0][1]).not.toHaveProperty("auto_post");
  });

  it("explains a refused delete of a plan with history", async () => {
    const user = userEvent.setup();
    vi.mocked(api.del).mockRejectedValue(new Error("Rencana ini punya riwayat. Nonaktifkan saja agar riwayat tetap tersimpan."));
    renderRules();
    await user.click(await screen.findByRole("button", { name: /^Belanja mingguan/ }));
    await user.click(screen.getByRole("button", { name: "Hapus rencana Belanja mingguan" }));
    await user.click(await screen.findByRole("button", { name: "Ya, lanjutkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/recurring/rule-1"));
    expect(await screen.findByText(/Nonaktifkan saja agar riwayat tetap tersimpan/)).toBeInTheDocument();
  });

  it("edits the weekday of an existing weekly expense and toggles it with today's request", async () => {
    const user = userEvent.setup();
    renderRules();
    const row = await screen.findByRole("button", { name: /^Belanja mingguan/ });
    await user.click(row);
    expect(row).toHaveAttribute("aria-current", "true");
    expect(screen.getByRole("heading", { name: "Belanja mingguan" })).toBeInTheDocument();

    expect(screen.getByRole("combobox", { name: "Hari pengulangan" })).toHaveValue("1");
    await user.selectOptions(screen.getByRole("combobox", { name: "Hari pengulangan" }), "5");
    await user.click(screen.getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/recurring/rule-1", expect.objectContaining({
      category_id: "category-expense",
      schedule_type: "weekly",
      schedule_day: 5,
    })));
    await user.click(screen.getByRole("button", { name: "Nonaktifkan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/rule-1/toggle"));
  });

  it("creates only a non-posting monthly plan from the selected product shortcut", async () => {
    const user = userEvent.setup();
    renderRules({ investmentDraft: { sourceAccountId: "account-1", targetAccountId: "fund-2", amount: 112_590, productName: "Bibit B" } });
    const target = await screen.findByRole("combobox", { name: "Produk reksadana tujuan" });
    await waitFor(() => expect(target).toHaveValue("fund-2"));
    expect(screen.getByRole("textbox", { name: "Nominal" })).toHaveValue("112.590");
    expect(screen.queryByRole("option", { name: "Fund with units" })).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox", { name: /otomatis|alokasi gaji/i })).not.toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalled();
    await user.clear(screen.getByRole("spinbutton", { name: "Tanggal setiap bulan (1–31)" }));
    await user.type(screen.getByRole("spinbutton", { name: "Tanggal setiap bulan (1–31)" }), "10");
    await user.click(screen.getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring", expect.objectContaining({
      type: "investment_topup", source_account_id: "account-1", target_account_id: "fund-2",
      amount: 112_590, category_id: null,
      schedule_type: "monthly_day", schedule_day: 10,
    })));
    expect(api.post).toHaveBeenCalledTimes(1);
  });

  it("clears an expense category before saving a switched transfer", async () => {
    const user = userEvent.setup();
    renderRules();
    await user.click(await screen.findByRole("button", { name: /^Belanja mingguan/ }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Jenis rencana" }), "transfer");
    expect(screen.queryByRole("combobox", { name: "Kategori pengeluaran" })).not.toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "Ke rekening / kantong tujuan" }), "account-2");
    await user.click(screen.getByRole("button", { name: "Simpan" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/recurring/rule-1", expect.objectContaining({
      type: "transfer",
      category_id: null,
      obligation_id: null,
    })));
  });

  it("offers credit accounts for a recurring expense only, without a debt link", async () => {
    const getDefault = vi.mocked(api.get).getMockImplementation()!;
    vi.mocked(api.get).mockImplementation(async (path, signal) => {
      if (path === "/accounts") return { accounts: [
        { id: "account-1", name: "BCA", type: "bank", balance: 100_000 },
        { id: "account-2", name: "Jago", type: "bank", balance: 25_000 },
        { id: "spaylater", name: "SPayLater", type: "credit", balance: 0, credit_owed: 0 },
      ] };
      if (path === "/obligations") return { obligations: [{ id: "debt-1", name: "Cicilan Contoh", remaining_amount: 900_000, is_archived: false }] };
      return getDefault(path, signal);
    });
    const user = userEvent.setup();
    renderRules();
    await user.click(screen.getAllByRole("button", { name: "Tambah rencana" })[0]);
    const source = screen.getByRole("combobox", { name: "Dari rekening" });
    await waitFor(() => expect(within(source).getByRole("option", { name: "BCA" })).toBeInTheDocument());
    expect(within(source).queryByRole("group", { name: "Kredit" })).not.toBeInTheDocument();

    await user.selectOptions(screen.getByRole("combobox", { name: "Jenis rencana" }), "expense");
    expect(within(source).getByRole("group", { name: "Kredit" })).toBeInTheDocument();
    expect(await screen.findByRole("combobox", { name: /tagihan|utang|cicilan/i })).toBeInTheDocument();
    await user.selectOptions(source, "spaylater");
    expect(screen.queryByRole("combobox", { name: /tagihan|utang|cicilan/i })).not.toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "Nama rencana" }), "Langganan musik");
    await user.type(screen.getByRole("textbox", { name: "Nominal" }), "55000");
    await user.selectOptions(screen.getByRole("combobox", { name: "Kategori pengeluaran" }), "category-expense");
    await user.click(screen.getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring", expect.objectContaining({
      type: "expense", source_account_id: "spaylater", obligation_id: null,
    })));

    await user.click(screen.getAllByRole("button", { name: "Tambah rencana" })[0]);
    await user.selectOptions(screen.getByRole("combobox", { name: "Jenis rencana" }), "income");
    const incomeSource = screen.getByRole("combobox", { name: "Masuk ke rekening" });
    expect(within(incomeSource).queryByRole("group", { name: "Kredit" })).not.toBeInTheDocument();
  });

  it("opens with the plan a caller names and shows its due occurrence with today's requests", async () => {
    pending = [dueBill];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/recurring") return { ok: true, rules: [weeklyExpense, dueBill] };
      if (path === "/recurring/pending") return { ok: true, rules: pending };
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 100_000 }] };
      if (path === "/categories") return { categories: [{ id: "category-expense", name: "Makan", kind: "expense" }] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    const user = userEvent.setup();
    const { container } = renderRules({ planId: "bill-rule" });
    expect(await screen.findByRole("heading", { name: "Listrik bulanan" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^Listrik bulanan/ })).toHaveAttribute("aria-current", "true");
    expect(screen.getByRole("textbox", { name: "Nama rencana" })).toHaveValue("Listrik bulanan");
    expect(await screen.findByRole("heading", { name: "Rencana jatuh tempo" })).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Bukan salah satunya, catat manual" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/bill-rule/occurrences/record", {
      scheduled_for: "2026-10-01", acknowledged_candidate_ids: ["tx-1"],
    }));
    expect(api.post).toHaveBeenCalledTimes(1);
  });

  it("reads in English without an Indonesian notice or region", async () => {
    const { container } = renderRules({}, (ui) => renderWithIntl(ui, { language: "en" }));
    expect(await screen.findByRole("heading", { name: "Plans & reminders" })).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: /Tabungan bulanan.*scheduled · 25 Jan · Every month on day 25/ })).toBeInTheDocument();
    expect(screen.getByText("2 plans · reminders, not automatic payments unless marked")).toBeInTheDocument();
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
    expect(container.ownerDocument.querySelector('[lang="id"]')).toBeNull();
  });
});

describe("DuePlanActions", () => {
  function renderActions(rule: typeof dueBill) {
    return render(
      <QueryClientProvider client={client()}>
        <DuePlanActions rule={rule as never} />
      </QueryClientProvider>,
    );
  }

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.post).mockResolvedValue({ ok: true });
  });

  it("links the chosen capture, skips, and records with the same requests as Rencana jatuh tempo", async () => {
    const user = userEvent.setup();
    renderActions(dueBill);
    await user.click(screen.getByRole("button", { name: /^Kaitkan .*Rp 250.000.* ke Listrik bulanan$/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/bill-rule/occurrences/link", {
      scheduled_for: "2026-10-01", transaction_id: "tx-1",
    }));
    await user.click(screen.getByRole("button", { name: "Lewati kali ini" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/bill-rule/occurrences/skip", { scheduled_for: "2026-10-01" }));
    await user.click(screen.getByRole("button", { name: "Bukan salah satunya, catat manual" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/bill-rule/occurrences/record", {
      scheduled_for: "2026-10-01", acknowledged_candidate_ids: ["tx-1"],
    }));
    expect(api.post).toHaveBeenCalledTimes(3);
  });

  it("shows a refusal next to the plan", async () => {
    vi.mocked(api.post).mockRejectedValue(new Error("Kejadian ini sudah dicatat, dikaitkan, atau dilewati."));
    const user = userEvent.setup();
    renderActions({ ...dueBill, match_state: "none" as never, candidates: [] });
    await user.click(screen.getByRole("button", { name: "Lewati kali ini" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("sudah dicatat, dikaitkan, atau dilewati");
  });
});
