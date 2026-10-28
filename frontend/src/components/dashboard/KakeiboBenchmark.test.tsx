import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import type { KakeiboBreakdown } from "@/lib/kakeibo";
import { KakeiboBenchmark } from "./KakeiboBenchmark";

vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

const base: KakeiboBreakdown = {
  need_spent: 450_000, need_pct: 45,
  want_spent: 100_000, want_pct: 10,
  culture_spent: 50_000, culture_pct: 5,
  unexpected_spent: 0, unexpected_pct: 0,
  saving_spent: 300_000, saving_pct: 30,
  essentials_pct: 45, lifestyle_pct: 15,
  kakeibo_status: "healthy",
};

describe("KakeiboBenchmark", () => {
  it("shows the healthy status when the backend reports healthy", () => {
    render(<KakeiboBenchmark kakeibo={base} />);
    expect(screen.getByText("Kakeibo sehat")).toBeInTheDocument();
  });

  it.each([
    ["warning", "Gaya hidup di atas 30%"],
    ["elevated_needs", "Kebutuhan di atas 50%"],
  ])("names the problem for %s instead of a generic warning", (status, label) => {
    render(<KakeiboBenchmark kakeibo={{ ...base, kakeibo_status: status }} />);
    expect(screen.getByText(label)).toBeInTheDocument();
    expect(screen.queryByText("Kakeibo sehat")).not.toBeInTheDocument();
  });

  it("describes all five pillars in the legend and the bar's accessible name", () => {
    render(<KakeiboBenchmark kakeibo={base} />);
    const bar = screen.getByRole("img", { name: /Alokasi pilar/ });
    expect(bar).toHaveAccessibleName("Alokasi pilar: Kebutuhan 45%, Keinginan 10%, Budaya 5%, Tak Terduga 0%, Tabungan 30%");
    const legend = screen.getAllByRole("list")[0];
    expect(within(legend).getAllByRole("listitem")).toHaveLength(5);
  });

  it("shows the three benchmark groups with their member pillars", () => {
    render(<KakeiboBenchmark kakeibo={base} />);
    expect(screen.getByRole("heading", { name: "Kebutuhan pokok" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Gaya hidup" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Tabungan" })).toBeInTheDocument();
    expect(screen.getByText("15% dari total uang masuk")).toBeInTheDocument();
    expect(screen.getByText("Budaya")).toBeInTheDocument();
  });

  it("has no detectable accessibility violations", async () => {
    const { container } = render(<KakeiboBenchmark kakeibo={base} />);
    expect((await axe(container)).violations).toHaveLength(0);
  });
});
