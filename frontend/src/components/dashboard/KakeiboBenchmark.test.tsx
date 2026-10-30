import { render, screen, within } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import type { KakeiboBreakdown } from "@/lib/kakeibo";
import { KakeiboBenchmark } from "./KakeiboBenchmark";

vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }) }));

const values: KakeiboBreakdown = {
  need_spent: 450_000, need_pct: 45, want_spent: 100_000, want_pct: 10,
  culture_spent: 50_000, culture_pct: 5, unexpected_spent: 20_000, unexpected_pct: 2,
  saving_spent: 300_000, saving_pct: 30, kakeibo_status: "healthy",
};

it("shows all five recorded figures without a health judgment", async () => {
  const { container } = render(<KakeiboBenchmark kakeibo={values} pillarHref={(pillar) => `/ledger?kakeibo=${pillar}`} />);
  const rows = within(screen.getAllByRole("list")[0]).getAllByRole("listitem");
  expect(rows).toHaveLength(5);
  expect(within(rows[2]).getByText("Rp 50.000")).toBeInTheDocument();
  expect(within(rows[3]).getByText("Rp 20.000")).toBeInTheDocument();
  expect(within(rows[4]).getByText("Rp 300.000")).toBeInTheDocument();
  expect(within(rows[4]).queryByRole("link")).not.toBeInTheDocument();
  expect(screen.queryByText("Kakeibo sehat")).not.toBeInTheDocument();
  expect(within(rows[2]).getByText("5%")).toBeInTheDocument();
  expect(within(rows[0]).getByRole("link", { name: "Kebutuhan Rp 450.000, lihat transaksinya" })).toHaveAttribute("href", "/ledger?kakeibo=need");
  expect(screen.getByText(/surplus arus kas, bukan bukti uang sudah disisihkan/i)).toBeInTheDocument();
  expect(screen.getByText(/bukan penilaian kesehatan keuangan/i)).toBeInTheDocument();
  expect(screen.queryByText(/sesuai target|melebihi target|kakeibo sehat/i)).not.toBeInTheDocument();
  expect((await axe(container)).violations).toHaveLength(0);
});

it("explains the saving components and the optional grouping with rounding", () => {
  render(<KakeiboBenchmark kakeibo={{ ...values, saving_expenses: 40_000, net_saving_transfers: 260_000, essentials_pct: 47, lifestyle_pct: 15.25 }} />);
  expect(screen.getByText(/pengeluaran berklasifikasi tabungan/)).toHaveTextContent("Rp 40.000");
  expect(screen.getByText(/pengeluaran berklasifikasi tabungan/)).toHaveTextContent("Rp 260.000");
  expect(screen.getByText("Kebutuhan pokok: 47% (acuan 50%)")).toBeInTheDocument();
  expect(screen.getByText("Gaya hidup: 15,3% (acuan 30%)")).toBeInTheDocument();
});

it("shows an empty state instead of a verdict for an empty period", () => {
  const empty: KakeiboBreakdown = {
    need_spent: 0, need_pct: 0, want_spent: 0, want_pct: 0, saving_spent: 0, saving_pct: 0,
    total_allocated: 0, kakeibo_status: "healthy",
  };
  render(<KakeiboBenchmark kakeibo={empty} />);
  expect(screen.getByText("Belum ada pengeluaran atau tabungan tercatat pada periode ini.")).toBeInTheDocument();
  expect(screen.queryByRole("listitem")).not.toBeInTheDocument();
});

it("does not present missing data as zero", () => {
  render(<KakeiboBenchmark />);
  expect(screen.getByText("Data belum tersedia.")).toBeInTheDocument();
});
