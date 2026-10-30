import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { describe, expect, it } from "vitest";
import {
  ALL_LIQUID_NOTICE,
  EmergencyCoverageBasis,
  coverageText,
  essentialBasisText,
  type EmergencyRunway,
} from "./EmergencyCoverage";

const bal = (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`;

const period = (label: string, start: string, end: string, amount: number) => ({
  kind: "month" as const, label, start, end, start_date: start.slice(0, 10), end_date: end.slice(0, 10), days: 30,
  gross_need: amount, reimbursement_offset: 0, amount,
});

const known: EmergencyRunway = {
  runway_days: 61, runway_months: 2, coverage_months: 2, status: "critical",
  emergency_fund_balance: 4_000_000, balance_basis: "emergency_goals", emergency_goal_names: ["Cadangan Contoh"],
  monthly_primary_expense: 2_500_000, target_months: 6,
  essential_cost: {
    amount: 2_500_000, living_amount: 2_000_000, debt_minimum: 500_000, method: "median_need_periods", basis: "history",
    period_count: 2, limited_history: true,
    periods_used: [
      period("September 2026", "2026-09-01T00:00:00+07:00", "2026-09-30T23:59:59.999999+07:00", 2_000_000),
      period("Agustus 2026", "2026-08-01T00:00:00+07:00", "2026-08-31T23:59:59.999999+07:00", 2_100_000),
    ],
  },
};

describe("Ketahanan Dana basis", () => {
  it("states the basis, limited history and links each period to its need rows", async () => {
    const user = userEvent.setup();
    const { container } = render(<EmergencyCoverageBasis runway={known} bal={bal} />);
    expect(screen.getByText("2x biaya pokok · 61 hari")).toBeInTheDocument();
    expect(screen.getByText("Median 2 periode, kebutuhan (need) saja + cicilan minimum")).toBeInTheDocument();
    expect(screen.getByText("Riwayat terbatas (2 periode)")).toBeInTheDocument();
    expect(screen.getByText("Dari target dana darurat: Cadangan Contoh")).toBeInTheDocument();
    await user.click(screen.getByText("Lihat 2 periode yang dipakai"));
    const link = screen.getByRole("link", { name: "Rp 2.000.000, lihat transaksi kebutuhan September 2026" });
    const href = new URL(link.getAttribute("href") ?? "", "https://example.test");
    expect(href.pathname).toBe("/ledger");
    expect(Object.fromEntries(href.searchParams)).toEqual({
      kakeibo: "need", type: "expense", from: "2026-09-01T00:00:00+07:00", to: "2026-09-30T23:59:59.999999+07:00",
    });
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("never shows an invented coverage and names the all-liquid fallback", async () => {
    const unknown: EmergencyRunway = {
      runway_days: null, runway_months: null, coverage_months: null, status: "unknown",
      emergency_fund_balance: 19_000_000, balance_basis: "all_liquid", emergency_goal_names: [],
      monthly_primary_expense: null,
      essential_cost: {
        amount: null, living_amount: null, debt_minimum: 0, method: "unknown", basis: "unknown",
        period_count: 0, limited_history: false, periods_used: [],
      },
    };
    const { container } = render(<EmergencyCoverageBasis runway={unknown} bal={bal} />);
    expect(screen.getByText("Belum dapat dihitung")).toBeInTheDocument();
    expect(screen.getByText("Belum diketahui")).toBeInTheDocument();
    expect(screen.getByText(ALL_LIQUID_NOTICE)).toBeInTheDocument();
    expect(screen.queryByText(/999|> 12x|> 1 Thn/)).not.toBeInTheDocument();
    expect(screen.queryByText(/periode yang dipakai/)).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("describes the budget basis and a zero balance", () => {
    expect(essentialBasisText({ ...known.essential_cost!, method: "need_budget", basis: "budget", debt_minimum: 0 }))
      .toBe("Dari anggaran kategori Kebutuhan (belum ada periode lengkap)");
    expect(coverageText({ ...known, status: "zero", runway_days: 0, coverage_months: 0, runway_months: 0 })).toBe("0 hari");
  });
});
