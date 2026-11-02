import { render, screen } from "@testing-library/react";
import { axe } from "vitest-axe";
import { describe, expect, it, vi } from "vitest";
import { CategoryDonutChart, type CategoryBreakdownItem } from "./CategoryDonutChart";

vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

const category = (id: string, name: string, spent: number, share: number): CategoryBreakdownItem => ({
  id, name, icon: null, color: null, spent, percentage_of_total: share, budget: null, percentage_used: null,
});

describe("CategoryDonutChart", () => {
  it("names the donut and summarises every category it draws", async () => {
    // jsdom has no ResizeObserver, which recharts' ResponsiveContainer needs.
    vi.stubGlobal("ResizeObserver", class { observe() {} unobserve() {} disconnect() {} });
    const { container } = render(
      <CategoryDonutChart
        categories={[category("food", "Makanan", 60_000, 60), category("bus", "Transportasi", 40_000, 40), category("idle", "Hobi", 0, 0)]}
      />,
    );

    const figure = screen.getByRole("figure", { name: "Diagram donat Distribusi Pengeluaran" });
    expect(figure).toHaveAccessibleDescription(
      "Total Rp 100.000 dalam 2 kategori: Makanan: Rp 60.000 (60%), Transportasi: Rp 40.000 (40%).",
    );
    expect((await axe(container)).violations).toHaveLength(0);
  });
});
