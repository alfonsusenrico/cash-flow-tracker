import { act, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Modal } from "@/components/ui/Modal";
import { PHONE_QUERY } from "./layers";
import { Drawer, TOAST_MS, ToastProvider, useToast } from "./overlays";
import { ShortcutProvider } from "./shortcuts";

type Width = "desk" | "tablet" | "phone";

function stubWidth(width: Width) {
  vi.stubGlobal(
    "matchMedia",
    vi.fn((query: string) => ({
      matches: query === PHONE_QUERY && width === "phone",
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })),
  );
}

function DrawerHarness({ stacked = false }: { stacked?: boolean }) {
  const [open, setOpen] = useState(false);
  const [inner, setInner] = useState(false);
  return (
    <ShortcutProvider>
      <button type="button" onClick={() => setOpen(true)}>
        Lihat detail
      </button>
      <Drawer open={open} onClose={() => setOpen(false)} title="Warung Bu Sri" subtitle="Pengeluaran · 2 Nov">
        <button type="button" onClick={() => setInner(true)}>
          Ubah kategori
        </button>
        <button type="button">Hapus</button>
      </Drawer>
      {stacked && (
        <Modal open={inner} onClose={() => setInner(false)} title="Pilih kategori">
          <button type="button">Makanan</button>
          <button type="button">Belanja</button>
        </Modal>
      )}
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

  it.each(["desk", "tablet"] as const)("opens as a centered modal dialog at %s width, never in a side column", async (width) => {
    stubWidth(width);
    const user = userEvent.setup();
    const { container } = render(<DrawerHarness />);
    const { opener, drawer } = await openDrawer(user);

    expect(drawer).toHaveAttribute("data-mode", "dialog");
    expect(drawer).toHaveAttribute("aria-modal", "true");
    // Portalled to the body, outside the page's own tree, over a scrim.
    expect(container).not.toContainElement(drawer);
    expect(document.body.querySelector(".ws-sheet-scrim")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Warung Bu Sri/ })).toHaveFocus();
    expect(document.body.style.overflow).toBe("hidden");
    expect((await axe(document.body)).violations).toHaveLength(0);

    screen.getByRole("button", { name: "Hapus" }).focus();
    await user.tab();
    expect(screen.getByRole("button", { name: "Tutup detail" })).toHaveFocus();
    await user.tab({ shift: true });
    expect(screen.getByRole("button", { name: "Hapus" })).toHaveFocus();

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
    expect(document.body.style.overflow).toBe("");
  });

  it("closes from the scrim and from the close button, returning focus each time", async () => {
    stubWidth("desk");
    const user = userEvent.setup();
    const { baseElement } = render(<DrawerHarness />);
    const { opener } = await openDrawer(user);
    await user.click(baseElement.querySelector(".ws-sheet-scrim")!);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();

    await openDrawer(user);
    await user.click(screen.getByRole("button", { name: "Tutup detail" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
  });

  it("lets a dialog opened from inside it own Tab and Esc, then takes the keyboard back", async () => {
    stubWidth("desk");
    const user = userEvent.setup();
    render(<DrawerHarness stacked />);
    await openDrawer(user);
    await user.click(screen.getByRole("button", { name: "Ubah kategori" }));
    const inner = await screen.findByRole("dialog", { name: "Pilih kategori" });

    screen.getByRole("button", { name: "Belanja" }).focus();
    await user.tab();
    expect(inner).toContainElement(document.activeElement as HTMLElement);

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog", { name: "Pilih kategori" })).not.toBeInTheDocument();
    expect(screen.getByRole("dialog", { name: /Warung Bu Sri/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Ubah kategori" })).toHaveFocus();

    screen.getByRole("button", { name: "Hapus" }).focus();
    await user.tab();
    expect(screen.getByRole("button", { name: "Tutup detail" })).toHaveFocus();
  });

  it("stays a bottom sheet at phone width: focus moves in, Tab is trapped, the page is locked", async () => {
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

  it("restores the earlier body overflow after it closes", async () => {
    stubWidth("phone");
    document.body.style.overflow = "clip";
    const user = userEvent.setup();
    render(<DrawerHarness />);
    await openDrawer(user);
    expect(document.body.style.overflow).toBe("hidden");
    await user.click(screen.getByRole("button", { name: "Tutup detail" }));
    expect(document.body.style.overflow).toBe("clip");
  });

  it("keeps focus inside when it mounts already open", async () => {
    stubWidth("phone");
    const opener = document.createElement("button");
    document.body.appendChild(opener);
    opener.focus();
    render(
      <ShortcutProvider>
        <Drawer open onClose={vi.fn()} title="Warung Bu Sri">
          <button type="button">Hapus</button>
        </Drawer>
      </ShortcutProvider>,
    );
    const drawer = await screen.findByRole("dialog", { name: "Warung Bu Sri" });
    expect(drawer).toHaveAttribute("data-mode", "sheet");
    expect(drawer).toContainElement(document.activeElement as HTMLElement);
    opener.remove();
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
