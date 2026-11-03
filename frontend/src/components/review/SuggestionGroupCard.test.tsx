import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { describe, expect, it, vi } from "vitest";
import type { HistorySuggestion, SuggestionGroup } from "@/lib/historySuggestions";
import { SuggestionGroupCard } from "./SuggestionGroupCard";

vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

const suggestion = (id: string, overrides: Partial<HistorySuggestion> = {}): HistorySuggestion => ({
  id, transaction_id: `tx-${id}`, type: "income", amount: 57_000, date: "2026-10-02T05:00:00Z", account_name: "BCA Harian",
  source: "rule", confidence: null, reason: "Transfer masuk menyebut patungan atau ganti uang: teman membayar kembali, bukan pemasukan",
  before: { merchant: null, notes: "Transfer masuk", category_id: "category-other", category_name: "Pendapatan Lain", kakeibo: null },
  after: {
    merchant: null, notes: "Ganti uang patungan", category_id: "category-repay", category_name: "Patungan & Ganti Uang",
    kakeibo: null, offset_category_id: "category-food", offset_category_name: "Makanan & Minuman",
  },
  ...overrides,
});

const group: SuggestionGroup = {
  key: "income|notes:ganti uang patungan|category-repay", type: "income", label: "Ganti uang patungan", merchant: null,
  category: { id: "category-repay", name: "Patungan & Ganti Uang" }, kakeibo: null, offset_category_names: ["Makanan & Minuman"],
  sources: ["rule"], before: [{ label: "Transfer masuk", category_name: "Pendapatan Lain", count: 2 }],
  count: 2, total: 114_000, first_date: "2026-10-01T05:00:00Z", last_date: "2026-10-02T05:00:00Z",
  suggestions: [suggestion("s-3"), suggestion("s-4", { date: "2026-10-01T05:00:00Z" })],
};

describe("Saran dari riwayat item", () => {
  it("shows before and after values and accepts the group by keyboard", async () => {
    const onDecide = vi.fn().mockResolvedValue(true);
    const user = userEvent.setup();
    const { container } = render(<main><SuggestionGroupCard group={group} headingId="h" pending={false} onDecide={onDecide} /></main>);
    expect(screen.getByRole("heading", { name: "Ganti uang patungan" })).toBeInTheDocument();
    expect(screen.getByText("Transfer masuk · Pendapatan Lain")).toBeInTheDocument();
    expect(screen.getByText("Ganti uang patungan · Patungan & Ganti Uang · mengurangi Makanan & Minuman", { selector: "dd" })).toBeInTheDocument();
    expect(screen.getByText(/Opsional\. Tidak ada yang berubah sebelum diterima\./)).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
    const accept = screen.getByRole("button", { name: /Terima saran \(2\)/ });
    expect(accept).toHaveAttribute("data-primary-choice");
    accept.focus();
    await user.keyboard("{Enter}");
    expect(onDecide).toHaveBeenCalledWith({ decision: "accept", ids: ["s-3", "s-4"], label: "Ganti uang patungan" });
  });

  it("Reject one row", async () => {
    const onDecide = vi.fn().mockResolvedValue(true);
    const user = userEvent.setup();
    render(<SuggestionGroupCard group={group} headingId="h" pending={false} onDecide={onDecide} />);
    const rows = screen.getByRole("list", { name: "Saran untuk Ganti uang patungan" });
    await user.click(within(rows).getAllByRole("button", { name: /Tolak saran untuk Transfer masuk/ })[0]);
    expect(onDecide).toHaveBeenCalledWith({ decision: "reject", ids: ["s-3"], label: "Ganti uang patungan" });
  });
});
