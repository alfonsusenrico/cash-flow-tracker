import { act, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PHONE_QUERY, RAIL_QUERY } from "./layers";
import { Drawer, DrawerSlotContext, TOAST_MS, ToastProvider, useToast } from "./overlays";
import { ShortcutProvider } from "./shortcuts";

type Width = "desk" | "rail" | "phone";

function stubWidth(width: Width) {
  vi.stubGlobal(
    "matchMedia",
    vi.fn((query: string) => ({
      matches: (query === PHONE_QUERY && width === "phone") || (query === RAIL_QUERY && width === "rail"),
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })),
  );
}

function DrawerHarness({ slot = null, setDrawerOpen = () => {} }: { slot?: HTMLElement | null; setDrawerOpen?: (open: boolean) => void }) {
  const [open, setOpen] = useState(false);
  return (
    <ShortcutProvider>
      <DrawerSlotContext.Provider value={{ slot, setDrawerOpen }}>
        <button type="button" onClick={() => setOpen(true)}>
          Lihat detail
        </button>
        <Drawer open={open} onClose={() => setOpen(false)} title="Warung Bu Sri" subtitle="Keluar · 2 Nov">
          <button type="button">Ubah kategori</button>
          <button type="button">Hapus</button>
        </Drawer>
      </DrawerSlotContext.Provider>
    </ShortcutProvider>
  );
}

async function openDrawer(user: ReturnType<typeof userEvent.setup>) {
  const opener = screen.getByRole("button", { name: "Lihat detail" });
  await user.click(opener);
  return { opener, drawer: await screen.findByRole("dialog", { name: /Warung Bu Sri/ }) };
}

describe("Drawer", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    document.body.style.overflow = "";
  });

  it("opens as a modal sheet at phone width: focus moves in, Tab is trapped, the page is locked", async () => {
    stubWidth("phone");
    const user = userEvent.setup();
    render(<DrawerHarness />);
    const { opener, drawer } = await openDrawer(user);

    expect(drawer).toHaveAttribute("data-mode", "sheet");
    expect(drawer).toHaveAttribute("aria-modal", "true");
    expect(screen.getByRole("heading", { name: /Warung Bu Sri/ })).toHaveFocus();
    expect(document.body.style.overflow).toBe("hidden");
    expect((await axe(document.body)).violations).toHaveLength(0);

    await user.tab();
    expect(screen.getByRole("button", { name: "Tutup detail" })).toHaveFocus();
    await user.tab();
    await user.tab();
    expect(screen.getByRole("button", { name: "Hapus" })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole("button", { name: "Tutup detail" })).toHaveFocus();
    await user.tab({ shift: true });
    expect(screen.getByRole("button", { name: "Hapus" })).toHaveFocus();
    expect(opener).not.toHaveFocus();

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
    expect(document.body.style.overflow).toBe("");
  });

  it("restores the earlier body overflow after the sheet closes", async () => {
    stubWidth("phone");
    document.body.style.overflow = "clip";
    const user = userEvent.setup();
    render(<DrawerHarness />);
    await openDrawer(user);
    expect(document.body.style.overflow).toBe("hidden");
    await user.click(screen.getByRole("button", { name: "Tutup detail" }));
    expect(document.body.style.overflow).toBe("clip");
  });

  it("is a modal overlay in the rail range and traps focus without locking the page", async () => {
    stubWidth("rail");
    const user = userEvent.setup();
    render(<DrawerHarness slot={document.createElement("div")} />);
    const { opener, drawer } = await openDrawer(user);

    expect(drawer).toHaveAttribute("data-mode", "overlay");
    expect(drawer).toHaveAttribute("aria-modal", "true");
    expect(document.body.style.overflow).toBe("");
    screen.getByRole("button", { name: "Hapus" }).focus();
    await user.tab();
    expect(screen.getByRole("button", { name: "Tutup detail" })).toHaveFocus();

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
  });

  it("closes from the scrim", async () => {
    stubWidth("rail");
    const user = userEvent.setup();
    const { baseElement } = render(<DrawerHarness />);
    const { opener } = await openDrawer(user);
    await user.click(baseElement.querySelector(".ws-sheet-scrim")!);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
  });

  it("sits non-modally in the window's drawer slot above 1180 px", async () => {
    stubWidth("desk");
    const slot = document.createElement("div");
    document.body.appendChild(slot);
    const setDrawerOpen = vi.fn();
    const user = userEvent.setup();
    render(<DrawerHarness slot={slot} setDrawerOpen={setDrawerOpen} />);
    const { opener, drawer } = await openDrawer(user);

    expect(slot).toContainElement(drawer);
    expect(drawer).toHaveAttribute("data-mode", "side");
    expect(drawer).toHaveAttribute("aria-modal", "false");
    expect(setDrawerOpen).toHaveBeenLastCalledWith(true);
    expect(screen.getByRole("heading", { name: /Warung Bu Sri/ })).toHaveFocus();

    await user.keyboard("{Escape}");
    expect(setDrawerOpen).toHaveBeenLastCalledWith(false);
    expect(opener).toHaveFocus();
    slot.remove();
  });

  it("keeps focus inside when it mounts already open at phone width while a slot exists", async () => {
    stubWidth("phone");
    const slot = document.createElement("div");
    document.body.appendChild(slot);
    const opener = document.createElement("button");
    document.body.appendChild(opener);
    opener.focus();
    const onClose = vi.fn();
    render(
      <ShortcutProvider>
        <DrawerSlotContext.Provider value={{ slot, setDrawerOpen: () => {} }}>
          <Drawer open onClose={onClose} title="Warung Bu Sri">
            <button type="button">Hapus</button>
          </Drawer>
        </DrawerSlotContext.Provider>
      </ShortcutProvider>,
    );
    const drawer = await screen.findByRole("dialog", { name: "Warung Bu Sri" });
    expect(drawer).toHaveAttribute("data-mode", "sheet");
    expect(drawer).toContainElement(document.activeElement as HTMLElement);
    slot.remove();
    opener.remove();
  });

  it("falls back to a modal overlay above 1180 px when no slot exists", async () => {
    stubWidth("desk");
    const user = userEvent.setup();
    render(<DrawerHarness />);
    const { drawer } = await openDrawer(user);
    expect(drawer).toHaveAttribute("data-mode", "overlay");
    expect(drawer).toHaveAttribute("aria-modal", "true");
  });
});

function ToastTrigger({ onUndo }: { onUndo?: () => void }) {
  const { showToast } = useToast();
  return (
    <button type="button" onClick={() => showToast("Transaksi dihapus", onUndo ? { onUndo } : undefined)}>
      Hapus transaksi
    </button>
  );
}

function showToastNow(onUndo?: () => void) {
  render(
    <ToastProvider>
      <ToastTrigger onUndo={onUndo} />
    </ToastProvider>,
  );
  fireEvent.click(screen.getByRole("button", { name: "Hapus transaksi" }));
  return screen.getByText("Transaksi dihapus").closest(".ws-toast") as HTMLElement;
}

describe("Toast", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("speaks through a polite status region and leaves after 6.5 s", () => {
    showToastNow();
    expect(TOAST_MS).toBe(6500);
    const region = screen.getByRole("status");
    expect(region).toHaveAttribute("aria-live", "polite");
    expect(region).toHaveTextContent("Transaksi dihapus");

    act(() => vi.advanceTimersByTime(6499));
    expect(screen.getByText("Transaksi dihapus")).toBeInTheDocument();
    act(() => vi.advanceTimersByTime(1));
    expect(screen.queryByText("Transaksi dihapus")).not.toBeInTheDocument();
    expect(screen.getByRole("status")).toBeEmptyDOMElement();
  });

  it("pauses while hovered and resumes with the time that was left", () => {
    const toast = showToastNow();
    act(() => vi.advanceTimersByTime(4000));
    fireEvent.mouseEnter(toast);
    act(() => vi.advanceTimersByTime(20_000));
    expect(screen.getByText("Transaksi dihapus")).toBeInTheDocument();
    fireEvent.mouseLeave(toast);
    act(() => vi.advanceTimersByTime(2499));
    expect(screen.getByText("Transaksi dihapus")).toBeInTheDocument();
    act(() => vi.advanceTimersByTime(1));
    expect(screen.queryByText("Transaksi dihapus")).not.toBeInTheDocument();
  });

  it("pauses while focus is inside so Urungkan stays reachable", () => {
    showToastNow(() => {});
    const undo = screen.getByRole("button", { name: "Urungkan" });
    act(() => undo.focus());
    act(() => vi.advanceTimersByTime(20_000));
    expect(undo).toBeInTheDocument();
    act(() => undo.blur());
    act(() => vi.advanceTimersByTime(TOAST_MS));
    expect(screen.queryByRole("button", { name: "Urungkan" })).not.toBeInTheDocument();
  });

  it("calls the undo callback once and closes", () => {
    const onUndo = vi.fn();
    showToastNow(onUndo);
    fireEvent.click(screen.getByRole("button", { name: "Urungkan" }));
    expect(onUndo).toHaveBeenCalledOnce();
    expect(screen.queryByText("Transaksi dihapus")).not.toBeInTheDocument();
  });

  it("shows no Urungkan without a callback", () => {
    showToastNow();
    expect(screen.queryByRole("button", { name: "Urungkan" })).not.toBeInTheDocument();
  });

  it("replaces the previous message and restarts the timer", () => {
    showToastNow();
    act(() => vi.advanceTimersByTime(5000));
    fireEvent.click(screen.getByRole("button", { name: "Hapus transaksi" }));
    act(() => vi.advanceTimersByTime(5000));
    expect(screen.getAllByText("Transaksi dihapus")).toHaveLength(1);
    act(() => vi.advanceTimersByTime(1500));
    expect(screen.queryByText("Transaksi dihapus")).not.toBeInTheDocument();
  });
});
