import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { api } from "@/lib/api";
import { AmountTrackingSwitchModal } from "./AmountTrackingSwitchModal";

vi.mock("@/lib/api", () => ({ api: { post: vi.fn() } }));
const product = {
  id: "fund", name: "Sucorinvest Money Market Fund", type: "investment", instrument_type: "mutual_fund",
  units: 2871.1295, balance: 5_725_549, cost_basis: 5_518_820,
};

function show() {
  const qc = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  const close = vi.fn();
  const view = render(<QueryClientProvider client={qc}><AmountTrackingSwitchModal product={product} onClose={close} /></QueryClientProvider>);
  return { ...view, close };
}

describe("switching a unit fund to amount tracking", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.post).mockResolvedValue({ ok: true });
  });

  it("states the kept value and cost and that the change is permanent", async () => {
    show();
    expect(screen.getByRole("dialog", { name: "Ubah ke pelacakan nominal" })).toBeInTheDocument();
    expect(screen.getByText(/5\.725\.549/)).toBeInTheDocument();
    expect(screen.getByText(/5\.518\.820/)).toBeInTheDocument();
    expect(screen.getByText("Perubahan ini tidak dapat dibatalkan.")).toBeInTheDocument();
    expect((await axe(screen.getByRole("dialog"))).violations).toEqual([]);
  });

  it("switches the product and closes", async () => {
    const user = userEvent.setup();
    const { close } = show();
    await user.click(screen.getByRole("button", { name: "Ubah ke nominal" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/fund/amount-tracking"));
    expect(close).toHaveBeenCalledOnce();
  });

  it("keeps the dialog open with the error announced on failure", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockRejectedValueOnce(new Error("Produk ini sudah dicatat berdasarkan nominal."));
    const { close } = show();
    await user.click(screen.getByRole("button", { name: "Ubah ke nominal" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("sudah dicatat berdasarkan nominal");
    expect(close).not.toHaveBeenCalled();
  });
});
