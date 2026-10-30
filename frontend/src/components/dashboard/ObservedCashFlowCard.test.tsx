import { render, screen } from "@testing-library/react";
import { axe } from "vitest-axe";
import { expect, it } from "vitest";
import { ObservedCashFlowCard } from "./ObservedCashFlowCard";

const bal = (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`;

it("shows observed figures, budget and dated plans without a daily allowance", async () => {
  const { container } = render(
    <ObservedCashFlowCard
      periodLabel="Siklus sejak 25 Okt · berlanjut"
      inflow={5_000_000}
      outflow={1_200_000}
      net={3_800_000}
      budget={3_000_000}
      budgetRemaining={1_800_000}
      commitments={{
        window_days: 30,
        total: 750_000,
        items: [
          { id: "plan-1", name: "Listrik Contoh", amount: 250_000, due_date: "2026-10-20", overdue: true },
          { id: "plan-2", name: "Cicilan Contoh", amount: 500_000, due_date: "2026-11-05", overdue: false },
        ],
      }}
      bal={bal}
    />,
  );
  expect(screen.getByText("Siklus sejak 25 Okt · berlanjut")).toBeInTheDocument();
  expect(screen.getByText("Rp 3.800.000")).toBeInTheDocument();
  expect(screen.getByText(/Sisa anggaran periode ini/)).toHaveTextContent("Rp 1.800.000");
  expect(screen.getByText("Rencana 30 hari ke depan")).toBeInTheDocument();
  expect(screen.getByText(/lewat 20 Okt/)).toBeInTheDocument();
  expect(screen.getByText(/bukan pembayaran atau dana yang pasti tersedia/)).toBeInTheDocument();
  expect(screen.queryByText(/hari ini|\/ hari/i)).not.toBeInTheDocument();
  expect((await axe(container)).violations).toHaveLength(0);
});

it("keeps unknown figures distinct from zero", () => {
  render(<ObservedCashFlowCard bal={bal} />);
  expect(screen.getAllByText("Tidak tersedia")).toHaveLength(3);
  expect(screen.getByText("Tidak tersedia.")).toBeInTheDocument();
  expect(screen.getByText("Periode belum tersedia")).toBeInTheDocument();
  expect(screen.getByText("Anggaran bulanan belum diatur.")).toBeInTheDocument();
});
