import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { MobileLedgerFeed, type LedgerTxItem } from "./MobileLedgerFeed";

const payment: LedgerTxItem = {
  id: "payment-1",
  account_id: "bank-1",
  account_name: "BCA",
  category_id: null,
  category_name: null,
  category_icon: null,
  category_color: null,
  goal_id: null,
  goal_name: null,
  obligation_id: null,
  obligation_name: null,
  obligation_allocations: [
    { obligation_id: "a", obligation_name: "Kartu A", amount: 60_000 },
    { obligation_id: "b", obligation_name: "Kartu B", amount: 40_000 },
  ],
  type: "expense",
  amount: 100_000,
  notes: "Bayar dua kartu",
  date: "2026-09-24T08:00:00Z",
  receipt_path: null,
  created_at: "2026-09-24T08:00:00Z",
};

describe("MobileLedgerFeed", () => {
  it("shows one cash row with the allocation breakdown and opens it by keyboard", async () => {
    const onOpenEdit = vi.fn();
    render(<MobileLedgerFeed transactions={[payment]} onOpenEdit={onOpenEdit} bal={(amount) => `Rp ${amount.toLocaleString("id-ID")}`} />);

    const transaction = screen.getByRole("button", { name: /Bayar dua kartu/ });
    expect(screen.getByText(/Kartu A Rp 60.000 · Kartu B Rp 40.000/)).toBeInTheDocument();
    expect(screen.getAllByRole("button")).toHaveLength(1);
    expect(screen.getByText(/Rp 100.000/)).toBeInTheDocument();
    transaction.focus();
    await userEvent.keyboard("{Enter}");
    expect(onOpenEdit).toHaveBeenCalledWith(payment);
    expect(onOpenEdit).toHaveBeenCalledTimes(1);
  });
});
