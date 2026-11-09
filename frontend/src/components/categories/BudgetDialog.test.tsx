import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { BudgetDialog } from "./BudgetDialog";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), patch: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

// Synthetic categories; the owner renamed both system categories, then created an ordinary "Investasi".
const categories = [
  { id: "food", name: "Makanan & Minuman", kind: "expense", monthly_budget: 1_500_000, is_archived: false, is_excluded_from_budget: false, system_key: null },
  { id: "investments", name: "Investments", kind: "expense", monthly_budget: null, is_archived: false, is_excluded_from_budget: false, system_key: "investment" },
  { id: "transfer", name: "Transfer", kind: "expense", monthly_budget: null, is_archived: false, is_excluded_from_budget: true, system_key: "internal_movement" },
  { id: "later", name: "Investasi", kind: "expense", monthly_budget: null, is_archived: false, is_excluded_from_budget: false, system_key: null },
];

describe("budget editor", () => {
  it("offers a budget only on categories reports count, whatever the system categories are named", async () => {
    vi.mocked(api.get).mockResolvedValue({ categories });
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <BudgetDialog open onClose={() => {}} />
      </QueryClientProvider>,
    );
    expect(await screen.findByLabelText("Makanan & Minuman")).toHaveValue("1.500.000");
    expect(screen.getByLabelText("Investasi")).toHaveValue("");
    expect(screen.queryByLabelText("Investments")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Transfer")).not.toBeInTheDocument();
    expect(api.get).toHaveBeenCalledWith("/categories?kind=expense");
  });
});
