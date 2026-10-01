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

const holdHandlers = {
  onHoldSelect: vi.fn(),
  onHoldPointerDown: vi.fn(),
  onHoldPointerMove: vi.fn(),
  onHoldPointerEnd: vi.fn(),
  onHoldContextMenu: vi.fn(),
};

describe("MobileLedgerFeed", () => {
  it("shows one investment top-up row and opens its contribution", async () => {
    const topup = { ...payment, investment_topup_id: "topup-1", movement_id: "topup-1", is_consolidated_transfer: true };
    const onOpenEdit = vi.fn();
    render(<MobileLedgerFeed transactions={[topup]} onOpenEdit={onOpenEdit} {...holdHandlers} bal={(amount) => `Rp ${amount.toLocaleString("id-ID")}`} />);
    expect(screen.getByText("Top up Investasi")).toBeInTheDocument();
    expect(screen.getAllByRole("button")).toHaveLength(1);
    await userEvent.click(screen.getByRole("button"));
    expect(onOpenEdit).toHaveBeenCalledWith(topup);
  });
  it("shows one cash row with the allocation breakdown and opens it by keyboard", async () => {
    const onOpenEdit = vi.fn();
    render(<MobileLedgerFeed transactions={[payment]} onOpenEdit={onOpenEdit} {...holdHandlers} bal={(amount) => `Rp ${amount.toLocaleString("id-ID")}`} />);

    const transaction = screen.getByRole("button", { name: /Bayar dua kartu/ });
    expect(screen.getByText(/Kartu A Rp 60.000 · Kartu B Rp 40.000/)).toBeInTheDocument();
    expect(screen.getAllByRole("button")).toHaveLength(1);
    expect(screen.getByText(/Rp 100.000/)).toBeInTheDocument();
    transaction.focus();
    await userEvent.keyboard("{Enter}");
    expect(onOpenEdit).toHaveBeenCalledWith(payment);
    expect(onOpenEdit).toHaveBeenCalledTimes(1);
  });

  it("exposes selected state and lets a keyboard user choose an unlinked row", async () => {
    const onSelect = vi.fn();
    render(
      <MobileLedgerFeed
        transactions={[payment]}
        onOpenEdit={onSelect}
        {...holdHandlers}
        selectionMode
        selectedIds={[payment.id]}
        bal={(amount) => `Rp ${amount.toLocaleString("id-ID")}`}
      />,
    );

    const row = screen.getByRole("button", { name: /Bayar dua kartu/ });
    expect(row).toHaveAttribute("aria-pressed", "true");
    row.focus();
    await userEvent.keyboard("{Enter}");
    expect(onSelect).toHaveBeenCalledWith(payment);
  });

  it("marks an existing movement as linked instead of offering selection", () => {
    render(
      <MobileLedgerFeed
        transactions={[{ ...payment, movement_id: "movement-1", is_consolidated_transfer: true }]}
        onOpenEdit={vi.fn()}
        {...holdHandlers}
        selectionMode
        selectedIds={[]}
        bal={(amount) => `Rp ${amount.toLocaleString("id-ID")}`}
      />,
    );

    expect(screen.getByText("Tergabung")).toBeInTheDocument();
    expect(screen.getByRole("button")).toBeDisabled();
  });

  it("keeps an inferred movement row selected without unlinked-status copy", () => {
    render(
      <MobileLedgerFeed
        transactions={[{ ...payment, is_inferred_transfer: true, partner_id: "incoming-1" }]}
        onOpenEdit={vi.fn()}
        {...holdHandlers}
        selectionMode
        selectedIds={[payment.id, "incoming-1"]}
        bal={(amount) => `Rp ${amount.toLocaleString("id-ID")}`}
      />,
    );

    expect(screen.getByText("Transfer")).toBeInTheDocument();
    expect(screen.queryByText(/perkiraan|belum tertaut/i)).not.toBeInTheDocument();
    expect(screen.getByRole("button")).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button")).toBeEnabled();
  });
});
