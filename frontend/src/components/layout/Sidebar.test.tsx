import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MobileDrawer, Sidebar } from "./Sidebar";

const navigation = vi.hoisted(() => ({ pathname: "/" }));
vi.mock("next/navigation", () => ({ usePathname: () => navigation.pathname }));
vi.mock("@/lib/api", () => ({ api: { post: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ user: { username: "owner", currency: "IDR" }, reviewAttention: null }),
}));

describe("Settings entry", () => {
  beforeEach(() => {
    navigation.pathname = "/";
  });

  it("links the sidebar user card to the Settings page", () => {
    render(<Sidebar collapsed={false} onToggleCollapse={vi.fn()} />);
    const entry = screen.getByRole("link", { name: "Pengaturan" });
    expect(entry).toHaveAttribute("href", "/settings");
    expect(entry).not.toHaveAttribute("aria-current");
  });

  it("marks the entry current on any settings section", () => {
    navigation.pathname = "/settings/pengirim";
    render(<Sidebar collapsed={false} onToggleCollapse={vi.fn()} />);
    expect(screen.getByRole("link", { name: "Pengaturan" })).toHaveAttribute("aria-current", "page");
  });

  it("offers Settings from the collapsed sidebar menu", async () => {
    const user = userEvent.setup();
    render(<Sidebar collapsed onToggleCollapse={vi.fn()} />);
    await user.click(screen.getByTitle(/Menu Akun/));
    expect(screen.getByRole("link", { name: "Pengaturan" })).toHaveAttribute("href", "/settings");
  });

  it("closes the phone drawer when Settings is chosen", async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();
    render(<MobileDrawer open onClose={onClose} />);
    const entry = screen.getByRole("link", { name: "Pengaturan" });
    expect(entry).toHaveAttribute("href", "/settings");
    entry.addEventListener("click", (event) => event.preventDefault());
    await user.click(entry);
    expect(onClose).toHaveBeenCalled();
  });
});
