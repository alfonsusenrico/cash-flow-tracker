import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { api } from "@/lib/api";
import { InvestmentTopupModal } from "./InvestmentTopupModal";

vi.mock("@/lib/api", () => ({ api: { post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
const product = { id: "fund", name: "Bibit Fund", type: "investment", instrument_type: "mutual_fund", units: null, parent_id: "bibit", balance: 1_050_000 };
const accounts = [product, { id: "bibit", name: "Bibit", type: "investment", default_funding_account_id: "rdn" }, { id: "rdn", name: "BCA RDN", type: "bank", balance: 500_000 }];
function show(overrides: Partial<React.ComponentProps<typeof InvestmentTopupModal>> = {}) {
  const qc = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  const close = vi.fn();
  const view = render(<QueryClientProvider client={qc}><InvestmentTopupModal open product={product} allAccounts={accounts} onClose={close} {...overrides} /></QueryClientProvider>);
  return { ...view, close, qc };
}

describe("amount investment top-ups", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    vi.mocked(api.del).mockResolvedValue({ ok: true });
  });

  it("defaults parent funding and records only the rupiah amount", async () => {
    const user = userEvent.setup();
    const { close } = show();
    expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("rdn");
    expect(screen.queryByLabelText(/Unit Penyertaan|NAB/)).not.toBeInTheDocument();
    await user.type(screen.getByLabelText("Nominal top up (IDR)"), "112590");
    await user.click(screen.getByRole("button", { name: "Catat top up" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/investment-topups", expect.objectContaining({ amount: 112590, source_account_id: "rdn", target_account_id: "fund", date: expect.any(String), idempotency_key: expect.any(String) })));
    expect(close).toHaveBeenCalledOnce();
  });

  it("prefers the product funding account when it is eligible", () => {
    show({ product: { ...product, default_funding_account_id: "other" }, allAccounts: [...accounts, { id: "other", name: "Other funding", type: "bank" }] });
    expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("other");
  });

  it("retains inputs and its retry identifier after a lost response", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockRejectedValueOnce(new Error("Connection lost"));
    const { close } = show();
    await user.type(screen.getByLabelText("Nominal top up (IDR)"), "112590");
    await user.click(screen.getByRole("button", { name: "Catat top up" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Connection lost");
    expect(screen.getByLabelText("Nominal top up (IDR)")).toHaveValue("112.590");
    expect(close).not.toHaveBeenCalled();
    const first = vi.mocked(api.post).mock.calls[0][1];
    await user.click(screen.getByRole("button", { name: "Catat top up" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledTimes(2));
    expect(vi.mocked(api.post).mock.calls[1][1]).toEqual(first);
  });

  it("passes a separate monthly draft without posting a debit", async () => {
    const user = userEvent.setup();
    const schedule = vi.fn();
    show({ onSchedule: schedule });
    await user.type(screen.getByLabelText("Nominal top up (IDR)"), "112590");
    await user.click(screen.getByRole("button", { name: "Jadwalkan bulanan" }));
    expect(schedule).toHaveBeenCalledWith({ sourceAccountId: "rdn", targetAccountId: "fund", amount: 112590, productName: "Bibit Fund" });
    expect(api.post).not.toHaveBeenCalled();
  });

  it("corrects a contribution through its own endpoint without changing accounts", async () => {
    const user = userEvent.setup();
    show({ editingTopup: { id: "topup", source_account_id: "rdn", target_account_id: "fund", amount: 112590, date: "2026-10-01T00:00:27Z", notes: "DCA" } });
    fireEvent.change(screen.getByLabelText("Nominal top up (IDR)"), { target: { value: "100000" } });
    await user.click(screen.getByRole("button", { name: "Simpan perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/investment-topups/topup", expect.objectContaining({ amount: 100000, notes: "DCA" })));
    expect(vi.mocked(api.patch).mock.calls[0][1]).not.toHaveProperty("source_account_id");
  });

  it("uses the contribution reversal endpoint on confirmed deletion", async () => {
    const user = userEvent.setup();
    show({ editingTopup: { id: "topup", source_account_id: "rdn", target_account_id: "fund", amount: 112590, date: "2026-10-01T00:00:27Z", notes: "DCA" } });
    await user.click(screen.getByRole("button", { name: "Hapus top up" }));
    await user.click(screen.getByRole("button", { name: "Ya, lanjutkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/investment-topups/topup"));
  });

  it("has labeled controls and a keyboard-operable dialog", async () => {
    show();
    const dialog = screen.getByRole("dialog", { name: "Top up Investasi" });
    expect((await axe(dialog)).violations).toEqual([]);
  });
});
