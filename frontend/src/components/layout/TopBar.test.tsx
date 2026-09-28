import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TopBar } from "./TopBar";

const setCycleOffset = vi.fn();

vi.mock("next/navigation", () => ({ usePathname: () => "/goals" }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    hideBalances: false,
    setHideBalances: vi.fn(),
    theme: "dark",
    setTheme: vi.fn(),
    cycleOffset: 0,
    setCycleOffset,
  }),
}));

describe("mobile header actions", () => {
  it("updates the sticky content offset when the header changes height", () => {
    let measuredHeight = 105;
    let onResize: () => void = () => {};
    const disconnect = vi.fn();
    const observe = vi.fn();
    const measurement = vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(
      () => ({ height: measuredHeight } as DOMRect),
    );
    vi.stubGlobal("ResizeObserver", class {
      constructor(callback: () => void) { onResize = callback; }
      observe = observe;
      disconnect = disconnect;
    });
    try {
      const { container, unmount } = render(<TopBar />);
      expect(observe).toHaveBeenCalledWith(screen.getByRole("banner"));
      expect(container.style.getPropertyValue("--app-topbar-height")).toBe("105px");
      measuredHeight = 135;
      onResize();
      expect(container.style.getPropertyValue("--app-topbar-height")).toBe("135px");
      unmount();
      expect(disconnect).toHaveBeenCalledOnce();
      expect(container.style.getPropertyValue("--app-topbar-height")).toBe("");
    } finally {
      measurement.mockRestore();
      vi.unstubAllGlobals();
    }
  });

  it("names icon actions and keeps month navigation keyboard operable", async () => {
    const openMenu = vi.fn();
    render(<TopBar onToggleMobileMenu={openMenu} />);

    expect(screen.getByRole("button", { name: "Sembunyikan saldo" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Aktifkan mode terang" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Buka menu" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "Pengaturan" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Bulan berikutnya" })).toBeDisabled();

    await userEvent.setup().click(screen.getByRole("button", { name: "Bulan sebelumnya" }));
    expect(setCycleOffset).toHaveBeenCalledWith(-1);
    await userEvent.setup().click(screen.getByRole("button", { name: "Buka menu" }));
    expect(openMenu).toHaveBeenCalledOnce();
  });
});
