import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { PendingScheduledBanner } from "./PendingScheduledBanner";

vi.mock("@/lib/api", () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
  },
}));

const candidate = (id: string, amount: number) => ({
  id, amount, date: "2026-10-01T03:00:00+00:00", notes: null, merchant: "Listrik Contoh",
  source: "notification", account_name: "BCA", exact_amount: amount === 250_000,
});

const bill = {
  id: "bill-rule", name: "Listrik bulanan", type: "expense", amount: 250_000, source_account_name: "BCA",
  next_due_date: "2026-10-01", obligation_name: null, window_days: 7,
};

function renderBanner() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <PendingScheduledBanner />
    </QueryClientProvider>,
  );
}

describe("PendingScheduledBanner", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockResolvedValue({ ok: true, pending_count: 0, rules: [] });
    vi.mocked(api.post).mockResolvedValue({ ok: true });
  });

  it("only reads due plans and never posts by itself", async () => {
    renderBanner();
    await waitFor(() => expect(api.get).toHaveBeenCalledWith("/recurring/pending"));
    expect(api.post).not.toHaveBeenCalled();
  });

  it("shows ambiguous matches for review and links only the chosen transaction", async () => {
    vi.mocked(api.get).mockResolvedValue({
      rules: [{ ...bill, match_state: "ambiguous", candidates: [candidate("tx-1", 250_000), candidate("tx-2", 250_000)] }],
    });
    const { container } = renderBanner();
    const user = userEvent.setup();
    expect(await screen.findByText(/tidak ada yang dikaitkan otomatis/)).toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalled();
    const buttons = screen.getAllByRole("button", { name: /^Kaitkan .* ke Listrik bulanan$/ });
    await user.click(buttons[1]);
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/bill-rule/occurrences/link", {
      scheduled_for: "2026-10-01", transaction_id: "tx-2",
    }));
    expect(api.post).toHaveBeenCalledTimes(1);
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("records a missed payment only after naming the same-amount captures it is not", async () => {
    vi.mocked(api.get).mockResolvedValue({
      rules: [{ ...bill, match_state: "single", candidates: [candidate("tx-1", 250_000), candidate("tx-3", 90_000)] }],
    });
    renderBanner();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Bukan salah satunya, catat manual" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/bill-rule/occurrences/record", {
      scheduled_for: "2026-10-01", acknowledged_candidate_ids: ["tx-1"],
    }));
  });

  it("skips an occurrence and shows a server refusal next to the plan", async () => {
    vi.mocked(api.get).mockResolvedValue({ rules: [{ ...bill, match_state: "none", candidates: [] }] });
    vi.mocked(api.post).mockRejectedValue(new Error("Kejadian ini sudah dicatat, dikaitkan, atau dilewati."));
    renderBanner();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Lewati kali ini" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/bill-rule/occurrences/skip", { scheduled_for: "2026-10-01" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("sudah dicatat, dikaitkan, atau dilewati");
  });
});
