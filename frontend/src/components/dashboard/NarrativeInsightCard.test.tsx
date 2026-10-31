import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import type { PeriodComparisonData } from "@/components/insights/ObservedInsights";
import { NarrativeInsightCard } from "./NarrativeInsightCard";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn() } }));
const ctx = vi.hoisted(() => ({ timeframe: "cycle", cycleOffset: 0 }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ ...ctx, bal: (n: number) => `Rp ${n.toLocaleString("id-ID")}` }),
}));

const span = (start: string, end: string, outflow: number) => ({
  start: `${start}T00:00:00+07:00`, end: `${end}T23:59:59+07:00`, start_date: start, end_date: end, inflow: 0, outflow, net: -outflow,
});
const change = (key: string, name: string, previous: number, current: number) => ({
  key, id: key, name, current, previous, change: current - previous, is_uncategorized: false,
});
const noChanges = { increases: [], decreases: [] };

function comparison(overrides: Partial<PeriodComparisonData>): PeriodComparisonData {
  const current = overrides.current ?? span("2026-10-01", "2026-10-10", 1_200_000);
  return {
    label: "Siklus 25 Okt 2026", compared_days: 10, current_truncated: false,
    current, current_to_date: current, previous: span("2026-09-01", "2026-09-10", 1_000_000),
    previous_label: "September 2026", previous_has_data: true, pillars: [], categories: noChanges, merchants: noChanges,
    ...overrides,
  };
}

function show(data: PeriodComparisonData | Error) {
  if (data instanceof Error) vi.mocked(api.get).mockRejectedValue(data);
  else vi.mocked(api.get).mockResolvedValue(data);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><NarrativeInsightCard /></QueryClientProvider>);
}

describe("Beranda insight card", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    ctx.timeframe = "cycle";
    ctx.cycleOffset = 0;
  });

  it("waits for three days instead of headlining day one of a cycle", async () => {
    const { container } = show(comparison({
      compared_days: 1,
      current: span("2026-10-25", "2026-10-25", 18_000),
      previous: span("2026-09-25", "2026-09-25", 4_000_000),
      categories: { increases: [], decreases: [change("food", "Makanan & Minuman", 250_000, 1_000)] },
    }));
    const card = await screen.findByRole("region", { name: "Dibanding periode lalu" });
    expect(await within(card).findByText(/Baru 1 hari berjalan/)).toHaveTextContent("setelah 3 hari");
    expect(card).toHaveTextContent("Rp 18.000");
    expect(card).not.toHaveTextContent("%");
    expect(card).not.toHaveTextContent(/HEMAT|Makanan & Minuman|lebih rendah/);
    expect(api.get).toHaveBeenCalledWith("/dashboard/period-comparison?timeframe=cycle&cycle_offset=0");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("states the change as amounts over the same elapsed days with category movers", async () => {
    ctx.timeframe = "month";
    ctx.cycleOffset = -1;
    const { container } = show(comparison({
      label: "Oktober 2026",
      categories: {
        increases: [change("food", "Makanan & Minuman", 300_000, 550_000)],
        decreases: [change("transport", "Transportasi", 200_000, 150_000)],
      },
    }));
    const card = await screen.findByRole("region", { name: "Dibanding periode lalu" });
    expect(await within(card).findByText(/Dalam 10 hari pertama/)).toHaveTextContent(
      "Dalam 10 hari pertama, uang keluar Rp 1.200.000, Rp 200.000 lebih tinggi dari periode lalu pada hari yang sama.",
    );
    expect(within(card).getByText(/^Pembanding:/)).toHaveTextContent("Pembanding: September 2026, 1 Sep – 10 Sep");
    expect(card).toHaveTextContent("Oktober 2026");
    expect(within(card).getByRole("heading", { name: "Naik paling banyak" })).toBeInTheDocument();
    expect(card).toHaveTextContent("+Rp 250.000");
    expect(card).toHaveTextContent("−Rp 50.000");
    expect(card).toHaveTextContent("Rp 300.000 → Rp 550.000");
    expect(card).not.toHaveTextContent("%");
    expect(api.get).toHaveBeenCalledWith("/dashboard/period-comparison?timeframe=month&cycle_offset=-1");
    expect(within(card).getByRole("link", { name: "Lihat perbandingan lengkap di Analisis" })).toHaveAttribute("href", "/insights");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("says lower spending in words and notes a shortened comparison", async () => {
    show(comparison({ current: span("2026-10-01", "2026-10-10", 700_000), current_truncated: true }));
    expect(await screen.findByText(/lebih rendah/)).toHaveTextContent("Rp 300.000 lebih rendah dari periode lalu");
    expect(screen.getByText(/juga dipotong agar adil/)).toBeInTheDocument();
    expect(screen.getByText("Tidak ada perubahan per kategori.")).toBeInTheDocument();
  });

  it("says when the previous period has no data", async () => {
    show(comparison({ previous_has_data: false }));
    expect(await screen.findByText(/belum ada pembanding/)).toHaveTextContent("Uang keluar sejauh ini Rp 1.200.000");
    expect(screen.queryByText(/lebih tinggi|lebih rendah/)).not.toBeInTheDocument();
  });

  it("counts the days a shortened comparison leaves out in the spending so far", async () => {
    // The 31st after a 30-day month: the comparison window ends on the 30th, today's spending is after it.
    show(comparison({
      label: "Oktober 2026", compared_days: 30, current_truncated: true, previous_has_data: false,
      current: span("2026-10-01", "2026-10-30", 0), current_to_date: span("2026-10-01", "2026-10-31", 3_600_000),
    }));
    expect(await screen.findByText(/belum ada pembanding/)).toHaveTextContent("Uang keluar sejauh ini Rp 3.600.000");
  });

  it("uses the spending so far when the previous period is too short", async () => {
    show(comparison({
      compared_days: 2, current_truncated: true,
      current: span("2026-10-01", "2026-10-02", 50_000), current_to_date: span("2026-10-01", "2026-10-10", 1_200_000),
    }));
    expect(await screen.findByText(/hanya 2 hari, terlalu pendek/)).toHaveTextContent("Uang keluar sejauh ini Rp 1.200.000");
  });

  it("says when the previous period is too short to compare", async () => {
    show(comparison({ compared_days: 2, current_truncated: true }));
    expect(await screen.findByText(/hanya 2 hari, terlalu pendek/)).toBeInTheDocument();
  });

  it("shows no figures when the comparison fails", async () => {
    show(new Error("offline"));
    const card = await screen.findByRole("region", { name: "Dibanding periode lalu" });
    expect(await within(card).findByText("Perbandingan belum dapat dimuat.")).toBeInTheDocument();
    expect(card).not.toHaveTextContent("Rp");
  });
});
