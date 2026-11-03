import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { DuePlansNotice, duePlansText, planDay } from "./DuePlansNotice";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }) }));

const money = (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`;
const plan = (id: string, name: string, amount: number, due: string) => ({
  id, name, type: "expense", amount, source_account_name: "Dompet Digital", next_due_date: due, obligation_name: null,
  window_days: 7, match_state: "none", candidates: [],
});

function renderNotice(onOpen = vi.fn()) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const view = render(
    <QueryClientProvider client={queryClient}>
      <DuePlansNotice onOpen={onOpen} />
    </QueryClientProvider>,
  );
  return { ...view, onOpen };
}

describe("Due plans notice on Transaksi", () => {
  beforeEach(() => vi.clearAllMocks());

  it("names one plan with its amount and date in one line, and opens the detail", async () => {
    vi.mocked(api.get).mockResolvedValue({ ok: true, rules: [plan("p1", "Streamio", 65_000, "2026-11-03")], recent_auto_links: [] });
    const user = userEvent.setup();
    const { container, onOpen } = renderNotice();
    const notice = await screen.findByRole("region", { name: "Rencana jatuh tempo" });
    expect(notice).toHaveTextContent("1 rencana jatuh tempo · Streamio Rp 65.000 · 3 Nov");
    expect(notice).not.toHaveTextContent("2026-11-03");
    await user.click(screen.getByRole("button", { name: "Lihat rencana" }));
    expect(onOpen).toHaveBeenCalledOnce();
    expect(api.post).not.toHaveBeenCalled();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("counts several plans with their total and the earliest date", () => {
    const rules = [plan("p1", "Streamio", 65_000, "2026-11-10"), plan("p2", "Cicilan Laptop", 600_000, "2026-11-05")];
    expect(duePlansText(rules as never, [], money)).toBe("2 rencana jatuh tempo · total Rp 665.000 · paling awal 5 Nov");
  });

  it("mentions automatic links and shows nothing when nothing is due", async () => {
    expect(duePlansText([], [{ rule_id: "r", scheduled_for: "2026-11-03" }] as never, money)).toBe("1 ditautkan otomatis");
    expect(duePlansText([], [], money)).toBeNull();
    vi.mocked(api.get).mockResolvedValue({ ok: true, rules: [], recent_auto_links: [] });
    const { container } = renderNotice();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(container).toBeEmptyDOMElement();
  });

  it("formats plan dates like the rest of the app", () => {
    expect(planDay("2026-11-03")).toBe("3 Nov");
    expect(planDay("2026-12-31")).toBe("31 Des");
  });
});
