import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { AccountLinks } from "./AccountLinks";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), del: vi.fn() } }));

const LINK = {
  id: "link-1", account_id: "acc-gopay", account_label: "GoPay", institution: "jago", institution_label: "Bank Jago",
  source: "confirmation" as const, created_at: "2026-11-04T09:00:00Z", skipped_count: 2, last_skipped_at: null,
};

function renderLinks() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <AccountLinks accountId="acc-gopay" accountName="GoPay" />
    </QueryClientProvider>,
  );
}

describe("Juga dilaporkan in the Rekening detail", () => {
  beforeEach(() => {
    vi.mocked(api.get).mockReset();
    vi.mocked(api.del).mockReset();
  });

  it("lists the apps that also report the account and lets a link go after a second confirmation", async () => {
    vi.mocked(api.get).mockResolvedValueOnce({ ok: true, links: [LINK] }).mockResolvedValue({ ok: true, links: [] });
    vi.mocked(api.del).mockResolvedValue({ ok: true });
    const user = userEvent.setup();
    const { container } = renderLinks();
    const section = await screen.findByRole("region", { name: "Juga dilaporkan" });
    expect(within(section).getByText("Bank Jago")).toBeInTheDocument();
    expect(api.get).toHaveBeenCalledWith("/ingest/account-links?account_id=acc-gopay");
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(within(section).getByRole("button", { name: "Lepaskan tautan Bank Jago" }));
    expect(api.del).not.toHaveBeenCalled();
    await user.click(within(section).getByRole("button", { name: "Ya, lepaskan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/ingest/account-links/link-1"));
    expect(await screen.findByRole("status")).toHaveTextContent(
      "Tautan dilepas. Laporan Bank Jago untuk GoPay akan ditanyakan lagi di Tinjau.");
  });

  it("shows nothing for an account no other app reports", async () => {
    vi.mocked(api.get).mockResolvedValue({ ok: true, links: [] });
    const { container } = renderLinks();
    await waitFor(() => expect(api.get).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  it("says when the list cannot be loaded", async () => {
    vi.mocked(api.get).mockRejectedValue(new Error("x"));
    renderLinks();
    expect(await screen.findByRole("alert")).toHaveTextContent("gagal dimuat");
  });
});
