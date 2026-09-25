import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, waitFor } from "@testing-library/react";
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
});
