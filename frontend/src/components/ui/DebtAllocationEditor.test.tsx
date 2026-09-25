import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { DebtAllocationEditor, allocationError } from "./DebtAllocationEditor";

const debts = [
  { id: "a", name: "Kartu A", remaining_amount: 80_000, is_archived: false },
  { id: "b", name: "Kartu B", remaining_amount: 50_000, is_archived: false },
];

describe("DebtAllocationEditor", () => {
  it("defaults selected debts to their outstanding balances and hides partial inputs", async () => {
    const onChange = vi.fn();
    const { rerender } = render(<DebtAllocationEditor idPrefix="test" debts={debts} rows={[]} onChange={onChange} total={0} currency={(value) => `Rp ${value}`} />);
    await userEvent.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
    const firstRow = onChange.mock.lastCall?.[0];
    rerender(<DebtAllocationEditor idPrefix="test" debts={debts} rows={firstRow} onChange={onChange} total={0} currency={(value) => `Rp ${value}`} />);
    await userEvent.selectOptions(screen.getByRole("combobox", { name: "Tagihan 1" }), "a");
    const firstSelected = onChange.mock.lastCall?.[0];
    expect(firstSelected[0].amount).toBe("80.000");
    rerender(<DebtAllocationEditor idPrefix="test" debts={debts} rows={firstSelected} onChange={onChange} total={80_000} currency={(value) => `Rp ${value}`} />);
    expect(screen.getByText("Dibayar Rp 80000")).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: /Dibayar untuk tagihan/ })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
    const secondRow = onChange.mock.lastCall?.[0];
    rerender(<DebtAllocationEditor idPrefix="test" debts={debts} rows={secondRow} onChange={onChange} total={80_000} currency={(value) => `Rp ${value}`} />);
    await userEvent.selectOptions(screen.getByRole("combobox", { name: "Tagihan 2" }), "b");
    const bothSelected = onChange.mock.lastCall?.[0];
    expect(bothSelected[1].amount).toBe("50.000");
    rerender(<DebtAllocationEditor idPrefix="test" debts={debts} rows={bothSelected} onChange={onChange} total={130_000} currency={(value) => `Rp ${value}`} />);
    expect(screen.getByText("Total bayar Rp 130000")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Ubah jumlah" }));
    expect(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" })).toHaveValue("80.000");
    expect(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 2 (IDR)" })).toHaveValue("50.000");
  });

  it("rejects mismatched and over-capacity splits", () => {
    const rows = [{ obligation_id: "a", amount: "60.000" }, { obligation_id: "b", amount: "20.000" }];
    expect(allocationError(rows, 100_000, debts)).toMatch(/sama/);
    expect(allocationError([{ ...rows[0], amount: "90.000" }, { ...rows[1], amount: "10.000" }], 100_000, debts)).toMatch(/melebihi/);
    expect(allocationError([{ ...rows[0], amount: "70.000" }, { ...rows[1], amount: "30.000" }], 100_000, debts)).toBeNull();
    expect(allocationError([{ obligation_id: "a", amount: "90.000" }], 90_000, debts)).toMatch(/melebihi/);
  });

  it("keeps a saved archived debt in its selector", () => {
    render(<DebtAllocationEditor idPrefix="test" debts={[{ ...debts[0], is_archived: true, remaining_amount: 0 }]} rows={[{ obligation_id: "a", amount: "100.000" }]} onChange={() => {}} total={100_000} currency={(value) => `Rp ${value}`} />);
    expect(screen.getByRole("option", { name: "Kartu A (tersimpan)" })).toBeInTheDocument();
  });

  it("has no automated accessibility violations in split mode", async () => {
    const { container } = render(
      <DebtAllocationEditor
        idPrefix="test"
        debts={debts}
        rows={[{ obligation_id: "a", amount: "60.000" }, { obligation_id: "b", amount: "40.000" }]}
        onChange={() => {}}
        total={100_000}
        currency={(value) => `Rp ${value}`}
      />,
    );
    expect((await axe(container)).violations).toHaveLength(0);
  });
});
