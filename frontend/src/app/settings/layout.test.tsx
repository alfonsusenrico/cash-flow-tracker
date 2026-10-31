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

const TITLES = ["Profil", "Gaji & siklus", "Mata uang & anggaran", "Perangkat & akses", "Pemetaan notifikasi", "Nama pengirim"];

describe("settings section navigation", () => {
  beforeEach(() => {
    navigation.pathname = "/settings";
  });

  it("lists the six sections in both navigations and marks Profil current at /settings", async () => {
    const { container } = renderAt("/settings");
    const wide = screen.getByRole("navigation", { name: "Bagian pengaturan" });
    const narrow = screen.getByRole("navigation", { name: "Daftar bagian pengaturan" });
    expect(within(wide).getAllByRole("link").map((link) => link.textContent)).toEqual(TITLES);
    expect(within(wide).getByRole("link", { name: "Profil" })).toHaveAttribute("aria-current", "page");
    expect(within(narrow).getAllByRole("link")).toHaveLength(6);
    expect(within(narrow).getByRole("link", { name: /Nama pengirim/ })).toHaveAttribute("href", "/settings/pengirim");
    expect(within(narrow).queryByRole("link", { current: "page" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Semua pengaturan" })).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("marks the open section current and offers the way back on narrow screens", () => {
    renderAt("/settings/perangkat");
    const navs = screen.getAllByRole("navigation");
    expect(navs).toHaveLength(1);
    expect(within(navs[0]).getByRole("link", { name: "Perangkat & akses" })).toHaveAttribute("aria-current", "page");
    expect(within(navs[0]).getByRole("link", { name: "Profil" })).not.toHaveAttribute("aria-current");
    expect(screen.getByRole("link", { name: "Semua pengaturan" })).toHaveAttribute("href", "/settings");
  });

  it("keeps focus on page load and moves it to the heading when the activated link disappears", () => {
    const view = renderAt("/settings");
    expect(document.body).toHaveFocus();
    const link = screen.getByRole("link", { name: /Pemetaan notifikasi.*rekening Anda/ });
    link.focus();
    navigation.pathname = "/settings/pemetaan";
    view.rerender(
      <SettingsLayout>
        <section aria-labelledby="probe-title"><h2 id="probe-title" data-settings-heading tabIndex={-1}>Isi bagian</h2></section>
      </SettingsLayout>,
    );
    // jsdom has no layout, so every element reads as hidden and the page title is the fallback target.
    expect(screen.getByRole("heading", { name: "Pengaturan" })).toHaveFocus();
  });
});
