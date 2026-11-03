import { render, screen, within } from "@testing-library/react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import SettingsLayout from "./layout";

const navigation = vi.hoisted(() => ({ pathname: "/settings" }));
vi.mock("next/navigation", () => ({ usePathname: () => navigation.pathname }));

function renderAt(pathname: string) {
  navigation.pathname = pathname;
  return render(
    <SettingsLayout>
      <section aria-labelledby="probe-title"><h2 id="probe-title" data-settings-heading tabIndex={-1}>Isi bagian</h2></section>
    </SettingsLayout>,
  );
}

const TITLES = ["HP & perangkat", "Kebiasaan", "Tampilan", "Profil", "Gaji & siklus", "Mata uang & anggaran", "Pemetaan notifikasi", "Nama pengirim"];

describe("settings section navigation", () => {
  beforeEach(() => {
    navigation.pathname = "/settings";
  });

  it("lists the eight sections with the Akun group in both navigations and marks HP & perangkat current at /settings", async () => {
    const { container } = renderAt("/settings");
    const wide = screen.getByRole("navigation", { name: "Bagian pengaturan" });
    // The phone index and the back link are hidden by the wide layout's styles; their markup is still checked.
    const narrow = screen.getByRole("navigation", { name: "Daftar bagian pengaturan", hidden: true });
    expect(within(wide).getAllByRole("link").map((link) => link.textContent)).toEqual(TITLES);
    expect(within(wide).getByRole("link", { name: "HP & perangkat" })).toHaveAttribute("aria-current", "page");
    expect(within(wide).getByRole("heading", { name: "Akun" })).toBeInTheDocument();
    expect(within(wide).getByRole("list", { name: "Akun" }).querySelectorAll("a")).toHaveLength(5);
    expect(within(narrow).getAllByRole("link", { hidden: true })).toHaveLength(8);
    expect(within(narrow).getByRole("link", { name: /Nama pengirim/, hidden: true })).toHaveAttribute("href", "/settings/pengirim");
    expect(within(narrow).queryByRole("link", { current: "page", hidden: true })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Semua pengaturan", hidden: true })).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("keeps old addresses and the two new sections working", () => {
    for (const slug of ["profil", "gaji", "anggaran", "perangkat", "pemetaan", "pengirim", "kebiasaan", "tampilan"]) {
      const view = renderAt(`/settings/${slug}`);
      expect(within(screen.getByRole("navigation", { name: "Bagian pengaturan" })).getByRole("link", { current: "page" })).toHaveAttribute("href", `/settings/${slug}`);
      view.unmount();
    }
  });

  it("marks the open section current and offers the way back on narrow screens", () => {
    renderAt("/settings/perangkat");
    const navs = screen.getAllByRole("navigation");
    expect(navs).toHaveLength(1);
    expect(within(navs[0]).getByRole("link", { name: "HP & perangkat" })).toHaveAttribute("aria-current", "page");
    expect(within(navs[0]).getByRole("link", { name: "Profil" })).not.toHaveAttribute("aria-current");
    // Hidden on wide screens by the layout's styles, so its accessible name is not computed here.
    expect(screen.getByText("Semua pengaturan").closest("a")).toHaveAttribute("href", "/settings");
  });

  it("keeps focus on page load and moves it to the section heading when the activated link disappears", () => {
    const view = renderAt("/settings");
    expect(document.body).toHaveFocus();
    const link = screen.getByRole("link", { name: /Pemetaan notifikasi.*dipetakan ke rekening/, hidden: true });
    link.focus();
    navigation.pathname = "/settings/pemetaan";
    view.rerender(
      <SettingsLayout>
        <section aria-labelledby="probe-title"><h2 id="probe-title" data-settings-heading tabIndex={-1}>Isi bagian</h2></section>
      </SettingsLayout>,
    );
    // jsdom has no layout, so the link reads as hidden once its screen opens; the section heading takes focus.
    expect(screen.getByRole("heading", { name: "Isi bagian" })).toHaveFocus();
  });
});
