import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { ProfileSettings } from "./ProfileSettings";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), patch: vi.fn() } }));

const owner = {
  username: "owner", name: "Owner", name_aliases: ["Owner Full Name"], currency: "USD",
  emergency_fund_multiplier: 9, monthly_spending_budget: 5_000_000,
};

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const invalidate = vi.spyOn(client, "invalidateQueries");
  const view = render(<QueryClientProvider client={client}><ProfileSettings /></QueryClientProvider>);
  return { ...view, invalidate, user: userEvent.setup() };
}

describe("Profil settings", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/auth/me") return { ok: true, user: owner };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
  });

  it("saves only the display name and aliases and refreshes the profile", async () => {
    const { user, invalidate, container } = show();
    const name = await screen.findByRole("textbox", { name: "Nama tampilan" });
    await waitFor(() => expect(name).toHaveValue("Owner"));
    expect(screen.getByText("@owner")).toBeInTheDocument();
    expect(name).toHaveAccessibleDescription("Dipakai untuk sapaan di Beranda.");
    expect((await axe(container)).violations).toHaveLength(0);

    await user.clear(name);
    await user.type(name, "Updated Owner");
    expect(screen.getByRole("status")).toHaveTextContent("Ada perubahan yang belum disimpan.");
    await user.click(screen.getByRole("button", { name: "Simpan profil" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledTimes(1));
    expect(api.patch).toHaveBeenCalledWith("/auth/settings", { name: "Updated Owner", name_aliases: ["Owner Full Name"] });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.auth.all });
    expect(invalidate).not.toHaveBeenCalledWith({ queryKey: queryKeys.dashboard.all });
    expect(await screen.findByText("Tersimpan.")).toBeInTheDocument();
  });

  it("saves bank-notification name aliases as a trimmed list", async () => {
    const { user } = show();
    const aliases = await screen.findByRole("textbox", { name: "Nama di notifikasi bank" });
    await waitFor(() => expect(aliases).toHaveValue("Owner Full Name"));
    expect(aliases).toHaveAccessibleDescription(/pisahkan dengan koma/i);
    await user.clear(aliases);
    await user.type(aliases, " Owner Full Name ,  OWNER F N,, ");
    await user.click(screen.getByRole("button", { name: "Simpan profil" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/auth/settings", {
      name: "Owner", name_aliases: ["Owner Full Name", "OWNER F N"],
    }));
  });

  it("rejects aliases shorter than three characters and an empty name without saving", async () => {
    const { user } = show();
    const aliases = await screen.findByRole("textbox", { name: "Nama di notifikasi bank" });
    await waitFor(() => expect(aliases).toHaveValue("Owner Full Name"));
    await user.type(aliases, ", AB");
    await user.click(screen.getByRole("button", { name: "Simpan profil" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Setiap nama alias harus 3–150 karakter");

    await user.clear(screen.getByRole("textbox", { name: "Nama tampilan" }));
    await user.click(screen.getByRole("button", { name: "Simpan profil" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Nama tampilan wajib diisi");
    expect(api.patch).not.toHaveBeenCalled();
  });

  it("shows the server refusal and keeps the typed values", async () => {
    vi.mocked(api.patch).mockRejectedValue(new Error("Each alias must be 3-150 characters"));
    const { user } = show();
    const name = await screen.findByRole("textbox", { name: "Nama tampilan" });
    await waitFor(() => expect(name).toHaveValue("Owner"));
    await user.type(name, " Baru");
    await user.click(screen.getByRole("button", { name: "Simpan profil" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Each alias must be 3-150 characters");
    expect(name).toHaveValue("Owner Baru");
  });
});
