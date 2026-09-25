import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, createMovement, updateMovement } from "@/lib/api";
import { InternalMovementModal } from "./InternalMovementModal";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn() },
  createMovement: vi.fn(),
  deleteMovement: vi.fn(),
  updateMovement: vi.fn(),
}));

describe("InternalMovementModal", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockResolvedValue({
      accounts: [
        { id: "account-source", name: "Cash", type: "cash", balance: 1_000 },
        { id: "account-target", name: "Bank", type: "bank", balance: 0 },
        { id: "investment-position", name: "Stock", type: "investment", balance: 0 },
      ],
    });
    vi.mocked(updateMovement).mockResolvedValue({
      ok: true,
      movement_id: "movement-1",
      expense_transaction_id: "out-1",
      income_transaction_id: "in-1",
    });
    vi.mocked(createMovement).mockResolvedValue({
      ok: true,
      movement_id: "movement-2",
      expense_transaction_id: "out-2",
      income_transaction_id: "in-2",
    });
  });

  it("uses the canonical update contract and excludes investment positions", async () => {
    const user = userEvent.setup();
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });

    render(
      <QueryClientProvider client={queryClient}>
        <InternalMovementModal
          open
          onClose={() => {}}
          editingMovement={{
            id: "movement-1",
            sourceAccountId: "account-source",
            targetAccountId: "account-target",
            amount: 500,
            notes: "Move savings",
            date: "2026-09-20T12:00:27.000Z",
          }}
        />
      </QueryClientProvider>,
    );

    expect(await screen.findByRole("dialog", { name: "Ubah pindah saldo" })).toBeVisible();
    expect(screen.queryByRole("option", { name: /Stock/ })).not.toBeInTheDocument();
    expect(screen.getByLabelText("Waktu Pemindahan")).toHaveAttribute("step", "1");
    expect((screen.getByLabelText("Waktu Pemindahan") as HTMLInputElement).value).toMatch(/:27(?:\.000)?$/);

    await user.clear(screen.getByRole("textbox", { name: "Nominal (IDR)" }));
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "750");
    await user.clear(screen.getByRole("textbox", { name: "Catatan (Opsional)" }));
    await user.type(screen.getByRole("textbox", { name: "Catatan (Opsional)" }), "Move reserve");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));

    await waitFor(() => expect(updateMovement).toHaveBeenCalledTimes(1));
    expect(updateMovement).toHaveBeenCalledWith(
      "movement-1",
      expect.objectContaining({
        source_account_id: "account-source",
        target_account_id: "account-target",
        amount: 750,
        notes: "Move reserve",
      }),
    );
    const [, payload] = vi.mocked(updateMovement).mock.calls[0];
    expect(payload.date).toBeDefined();
    expect(new Date(payload.date!).toISOString()).toBe("2026-09-20T12:00:27.000Z");
  });

  it("submits the same movement payload when an entry point provides account defaults", async () => {
    const user = userEvent.setup();
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });

    render(
      <QueryClientProvider client={queryClient}>
        <InternalMovementModal
          open
          onClose={() => {}}
          defaultSourceAccountId="account-source"
          defaultTargetAccountId="account-target"
        />
      </QueryClientProvider>,
    );

    expect(await screen.findByRole("combobox", { name: "Dari rekening" })).toHaveValue("account-source");
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("account-target"));
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "300");
    await user.click(screen.getByRole("button", { name: "Pindahkan Saldo" }));

    await waitFor(() => expect(createMovement).toHaveBeenCalledWith(expect.objectContaining({
      source_account_id: "account-source",
      target_account_id: "account-target",
      amount: 300,
    })));
  });
});
