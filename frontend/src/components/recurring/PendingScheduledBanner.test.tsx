import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { PendingScheduledBanner } from "./PendingScheduledBanner";

vi.mock("@/lib/api", () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
  },
}));

describe("PendingScheduledBanner", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockResolvedValue({ ok: true, pending_count: 0, rules: [] });
  });

  it("reads manual confirmations without triggering server-owned automatic execution", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });

    render(
      <QueryClientProvider client={queryClient}>
        <PendingScheduledBanner />
      </QueryClientProvider>,
    );

    await waitFor(() => expect(api.get).toHaveBeenCalledWith("/recurring/pending"));
    expect(api.post).not.toHaveBeenCalled();
  });

  it("confirms the selected investment occurrence and shows partial failures", async () => {
    const rule = { id: "fund-rule", name: "Monthly Bibit", type: "investment_topup", source_account_name: "BCA RDN", target_account_name: "Fund", amount: 112590, next_due_date: "2026-10-01" };
    vi.mocked(api.get).mockResolvedValue({ rules: [rule] });
    vi.mocked(api.post).mockResolvedValue({ results: [{ rule_id: rule.id, status: "failed", error_code: "insufficient_funds" }] });
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    render(<QueryClientProvider client={qc}><PendingScheduledBanner /></QueryClientProvider>);
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Catat Monthly Bibit" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/execute", { rule_ids: [rule.id], scheduled_dates: { "fund-rule": "2026-10-01" } }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Saldo sumber tidak mencukupi");
    expect(screen.getByText("BCA RDN → Fund")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Catat Monthly Bibit" })).toBeEnabled();
  });
});
