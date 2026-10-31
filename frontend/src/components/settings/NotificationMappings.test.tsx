import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { deleteNotificationAlias, getNotificationAliases } from "@/lib/api";
import { NotificationMappings } from "./NotificationMappings";

vi.mock("@/lib/api", () => ({ getNotificationAliases: vi.fn(), deleteNotificationAlias: vi.fn() }));

const aliases = [
  { id: "alias-1", institution: "jago", name: "GoPay Tabungan", account_id: "acc-1", account: "GoPay", source: "ai" as const, created_at: "2026-09-29T10:00:00Z" },
  { id: "alias-2", institution: "jago", name: "Tabungan Liburan", account_id: "acc-2", account: "Bank Jago · Liburan", source: "owner" as const, created_at: "2026-09-29T10:00:00Z" },
];

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  render(<QueryClientProvider client={client}><NotificationMappings /></QueryClientProvider>);
  return userEvent.setup();
}

describe("Pemetaan notifikasi", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getNotificationAliases).mockResolvedValue({ ok: true, aliases });
    vi.mocked(deleteNotificationAlias).mockResolvedValue({ ok: true });
  });

  it("lists learned names and removes a wrong one only after confirmation", async () => {
    const user = show();
    const section = await screen.findByRole("region", { name: "Pemetaan notifikasi" });
    expect(await within(section).findByText(/Dipetakan otomatis/)).toBeInTheDocument();
    expect(within(section).getByText(/Dikonfirmasi Anda/)).toBeInTheDocument();
    expect((await axe(section)).violations).toHaveLength(0);

    await user.click(within(section).getByRole("button", { name: "Hapus pemetaan GoPay Tabungan" }));
    expect(deleteNotificationAlias).not.toHaveBeenCalled();
    await user.click(within(section).getByRole("button", { name: "Ya, hapus" }));
    await waitFor(() => expect(deleteNotificationAlias).toHaveBeenCalledWith("alias-1"));
    await waitFor(() => expect(getNotificationAliases).toHaveBeenCalledTimes(2));
  });

  it("cancels with Escape and returns focus without removing", async () => {
    const user = show();
    const remove = await screen.findByRole("button", { name: "Hapus pemetaan Tabungan Liburan" });
    await user.click(remove);
    expect(screen.getByRole("group", { name: "Hapus pemetaan Tabungan Liburan?" })).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(screen.getByRole("button", { name: "Hapus pemetaan Tabungan Liburan" })).toHaveFocus();
    expect(deleteNotificationAlias).not.toHaveBeenCalled();
  });

  it("states an empty list truthfully", async () => {
    vi.mocked(getNotificationAliases).mockResolvedValueOnce({ ok: true, aliases: [] });
    show();
    expect(await screen.findByText("Belum ada pemetaan.")).toBeInTheDocument();
    expect(screen.queryByRole("list")).not.toBeInTheDocument();
  });

  it("reports a failed load instead of an empty list", async () => {
    vi.mocked(getNotificationAliases).mockRejectedValue(new Error("offline"));
    show();
    expect(await screen.findByRole("alert")).toHaveTextContent("Daftar pemetaan gagal dimuat.");
    expect(screen.queryByText("Belum ada pemetaan.")).not.toBeInTheDocument();
  });

  it("reports a failed removal and keeps the row", async () => {
    vi.mocked(deleteNotificationAlias).mockRejectedValue(new Error("Gagal menghapus pemetaan"));
    const user = show();
    await user.click(await screen.findByRole("button", { name: "Hapus pemetaan GoPay Tabungan" }));
    await user.click(screen.getByRole("button", { name: "Ya, hapus" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Gagal menghapus pemetaan");
    expect(screen.getByText(/GoPay Tabungan/)).toBeInTheDocument();
  });
});
