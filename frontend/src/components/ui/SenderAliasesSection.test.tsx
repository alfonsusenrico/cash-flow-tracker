import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { deleteSenderAlias, getSenderAliases, updateSenderAlias } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { SenderAliasesSection } from "./SenderAliasesSection";

vi.mock("@/lib/api", () => ({
  getSenderAliases: vi.fn(), updateSenderAlias: vi.fn(), deleteSenderAlias: vi.fn(),
}));

const alias = {
  id: "sender-1", institution: "bca", mask: "AN**RA**", name: "Andra",
  state: "active" as const, account_id: "account-1", account: "BCA · Harian",
};

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const invalidate = vi.spyOn(client, "invalidateQueries");
  render(<QueryClientProvider client={client}><SenderAliasesSection enabled /></QueryClientProvider>);
  return { invalidate, user: userEvent.setup() };
}

describe("sender memory", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getSenderAliases).mockResolvedValue({ ok: true, aliases: [alias] });
    vi.mocked(updateSenderAlias).mockResolvedValue({ ok: true });
    vi.mocked(deleteSenderAlias).mockResolvedValue({ ok: true });
  });

  it("shows loading, empty and list failure states", async () => {
    vi.mocked(getSenderAliases).mockResolvedValueOnce({ ok: true, aliases: [] });
    show();
    expect(screen.getByRole("status")).toHaveTextContent("Memuat pengirim");
    expect(await screen.findByText("Belum ada nama pengirim tersimpan.")).toBeInTheDocument();
  });

  it("shows list failure without inventing empty memory", async () => {
    vi.mocked(getSenderAliases).mockRejectedValue(new Error("offline"));
    show();
    expect(await screen.findByRole("alert")).toHaveTextContent("Daftar pengirim gagal dimuat");
  });

  it("corrects a name and restores focus while refreshing future memory", async () => {
    const { user, invalidate } = show();
    const edit = await screen.findByRole("button", { name: "Ubah nama pengirim AN**RA**" });
    await user.click(edit);
    const input = screen.getByRole("textbox", { name: "Nama pengirim" });
    expect(input).toHaveFocus();
    await user.clear(input);
    await user.type(input, " Bima ");
    await user.click(screen.getByRole("button", { name: "Simpan nama" }));
    await waitFor(() => expect(updateSenderAlias).toHaveBeenCalledWith("sender-1", "Bima"));
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.senderAliases });
    await waitFor(() => expect(edit).toHaveFocus());
  });

  it("keeps input on failure and blocks blank correction", async () => {
    vi.mocked(updateSenderAlias).mockRejectedValue(new Error("offline"));
    const { user } = show();
    await user.click(await screen.findByRole("button", { name: "Ubah nama pengirim AN**RA**" }));
    const input = screen.getByRole("textbox", { name: "Nama pengirim" });
    await user.clear(input);
    await user.click(screen.getByRole("button", { name: "Simpan nama" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Isi nama pengirim");
    expect(updateSenderAlias).not.toHaveBeenCalled();
    await user.type(input, "Bima");
    await user.click(screen.getByRole("button", { name: "Simpan nama" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("gagal disimpan");
    expect(input).toHaveValue("Bima");
  });

  it("makes ambiguity visible and supports removal", async () => {
    vi.mocked(getSenderAliases).mockResolvedValue({ ok: true, aliases: [{ ...alias, state: "ambiguous" }] });
    const { user, invalidate } = show();
    expect(await screen.findByText(/Nama berbeda pernah dikonfirmasi/)).toBeInTheDocument();
    expect((await axe(screen.getByRole("region", { name: "Nama pengirim" }))).violations).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Hapus nama pengirim AN**RA**" }));
    await waitFor(() => expect(deleteSenderAlias).toHaveBeenCalledWith("sender-1"));
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.senderAliases });
  });

  it("reports failed removal and preserves memory", async () => {
    vi.mocked(deleteSenderAlias).mockRejectedValue(new Error("offline"));
    const { user } = show();
    await user.click(await screen.findByRole("button", { name: "Hapus nama pengirim AN**RA**" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("gagal dihapus");
    expect(screen.getByText(/BCA · Harian/)).toBeInTheDocument();
  });
});
