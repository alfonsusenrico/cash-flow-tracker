import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithIntl } from "@/lib/i18n/testing";
import SettingsLayout from "./layout";

const navigation = vi.hoisted(() => ({ pathname: "/settings" }));
const workspace = vi.hoisted(() => ({ logout: vi.fn() }));
vi.mock("next/navigation", () => ({ usePathname: () => navigation.pathname }));
vi.mock("@/components/workspace/context", () => ({ useWorkspace: () => workspace }));

const probe = (
  <section aria-labelledby="probe-title">
    <h2 id="probe-title" data-settings-heading tabIndex={-1}>
      Isi bagian
    </h2>
  </section>
);

function renderAt(pathname: string) {
  navigation.pathname = pathname;
  return render(<SettingsLayout>{probe}</SettingsLayout>);
}

const TITLES = [
  "HP & perangkat", "Kebiasaan", "Tampilan", "Profil", "Gaji & siklus", "Mata uang & anggaran", "Pemetaan notifikasi",
  "Nama pengirim", "Sesi uji",
];

/** Spec "Sectioned Settings Page" and "Sign Out in Pengaturan" (apply-markas-pengaturan D2, D3, D5, D20). */
describe("Pengaturan rail", () => {
  beforeEach(() => {
    navigation.pathname = "/settings";
    vi.clearAllMocks();
  });

  it("lists the nine sections in three labelled groups and marks HP & perangkat current at /settings", async () => {
    const { container } = renderAt("/settings");
    const rail = screen.getByRole("navigation", { name: "Bagian pengaturan" });
    expect(within(rail).getAllByRole("link").map((link) => link.textContent)).toEqual(TITLES);
    expect(within(rail).getByRole("link", { name: "HP & perangkat" })).toHaveAttribute("aria-current", "page");
    expect(within(within(rail).getByRole("list", { name: "Perangkat" })).getAllByRole("link")).toHaveLength(2);
    expect(within(within(rail).getByRole("list", { name: "Tampilan" })).getAllByRole("link")).toHaveLength(1);
    expect(within(within(rail).getByRole("list", { name: "Akun" })).getAllByRole("link")).toHaveLength(6);
    expect(screen.queryByRole("link", { name: "Semua pengaturan" })).not.toBeInTheDocument();
    expect(screen.getAllByRole("navigation")).toHaveLength(1);
    expect(container.querySelector(".set")).not.toBeNull();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("keeps every section address and marks only the open one current", () => {
    for (const slug of ["perangkat", "kebiasaan", "tampilan", "profil", "gaji", "anggaran", "pemetaan", "pengirim", "sesi-uji"]) {
      const view = renderAt(`/settings/${slug}`);
      const rail = screen.getByRole("navigation", { name: "Bagian pengaturan" });
      expect(within(rail).getByRole("link", { current: "page" })).toHaveAttribute("href", `/settings/${slug}`);
      expect(within(rail).getAllByRole("link").filter((link) => link.hasAttribute("aria-current"))).toHaveLength(1);
      view.unmount();
    }
  });

  it("keeps focus on page load and moves it to the section title after a rail choice", () => {
    const view = renderAt("/settings");
    expect(document.body).toHaveFocus();
    screen.getByRole("link", { name: "Pemetaan notifikasi" }).focus();
    navigation.pathname = "/settings/pemetaan";
    view.rerender(<SettingsLayout>{probe}</SettingsLayout>);
    expect(screen.getByRole("heading", { name: "Isi bagian" })).toHaveFocus();
  });

  it("ends with Keluar, which signs out exactly as the account menu does", async () => {
    renderAt("/settings/tampilan");
    const rail = screen.getByRole("navigation", { name: "Bagian pengaturan" });
    const signOut = within(rail).getByRole("button", { name: "Keluar" });
    expect(rail.lastElementChild).toBe(signOut);
    await userEvent.setup().click(signOut);
    expect(workspace.logout).toHaveBeenCalledOnce();
  });

  it("reads the rail in English for an English account", () => {
    navigation.pathname = "/settings/anggaran";
    renderWithIntl(<SettingsLayout>{probe}</SettingsLayout>, { language: "en" });
    const rail = screen.getByRole("navigation", { name: "Settings sections" });
    expect(within(rail).getAllByRole("link").map((link) => link.textContent)).toEqual([
      "Phone & devices", "Habits", "Appearance", "Profile", "Salary & cycle", "Currency & budget", "Notification mapping",
      "Sender names", "Test session",
    ]);
    expect(within(rail).getByRole("list", { name: "Account" })).toBeInTheDocument();
    expect(within(rail).getByRole("button", { name: "Sign out" })).toBeInTheDocument();
  });
});
