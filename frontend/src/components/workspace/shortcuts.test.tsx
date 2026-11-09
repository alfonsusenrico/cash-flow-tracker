import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useRef } from "react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { SHORTCUTS_STORAGE_KEY, ShortcutProvider, isTypingTarget, usePageShortcutGroup, useShortcut, useShortcuts, type PageShortcutGroup } from "./shortcuts";

const handlers = { n: vi.fn(), modK: vi.fn() };

/** Node's own experimental localStorage global shadows jsdom's, so each test gets an in-memory one. */
function installStorage(overrides: Partial<Storage> = {}) {
  const data = new Map<string, string>();
  const storage = {
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => void data.set(key, String(value)),
    removeItem: (key: string) => void data.delete(key),
    clear: () => data.clear(),
    key: (index: number) => [...data.keys()][index] ?? null,
    get length() {
      return data.size;
    },
    ...overrides,
  };
  Object.defineProperty(window, "localStorage", { configurable: true, value: storage });
  return storage;
}

function GlobalKeys() {
  useShortcut("n", () => handlers.n());
  useShortcut("mod+k", () => handlers.modK());
  const { openHelp, singleKeysEnabled, setSingleKeysEnabled } = useShortcuts();
  return (
    <>
      <button type="button">Di luar kolom</button>
      <button type="button" onClick={openHelp}>
        Bantuan
      </button>
      {/* Stands in for the switch in Pengaturan › Tampilan, which uses the same setting. */}
      <button type="button" role="switch" aria-checked={singleKeysEnabled} onClick={() => setSingleKeysEnabled(!singleKeysEnabled)}>
        Pintasan satu tombol
      </button>
      <input aria-label="Cari transaksi" />
      <textarea aria-label="Catatan" />
      <select aria-label="Kategori">
        <option>Makan</option>
      </select>
      <div aria-label="Teks bebas" role="textbox" contentEditable suppressContentEditableWarning tabIndex={0} />
    </>
  );
}

function show() {
  const user = userEvent.setup();
  const view = render(
    <ShortcutProvider>
      <GlobalKeys />
    </ShortcutProvider>,
  );
  return { ...view, user };
}

async function turnSingleKeysOff(user: ReturnType<typeof userEvent.setup>) {
  const toggle = screen.getByRole("switch", { name: /Pintasan satu tombol/ });
  expect(toggle).toHaveAttribute("aria-checked", "true");
  await user.click(toggle);
  expect(toggle).toHaveAttribute("aria-checked", "false");
}

describe("shortcut registry", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    installStorage();
  });

  it("runs N outside fields and skips it inside input, textarea, select and contenteditable", async () => {
    const { user } = show();
    screen.getByRole("button", { name: "Di luar kolom" }).focus();
    await user.keyboard("n");
    expect(handlers.n).toHaveBeenCalledTimes(1);

    await user.type(screen.getByRole("textbox", { name: "Cari transaksi" }), "n");
    await user.type(screen.getByRole("textbox", { name: "Catatan" }), "n");
    screen.getByRole("combobox", { name: "Kategori" }).focus();
    await user.keyboard("n");
    const editable = screen.getByRole("textbox", { name: "Teks bebas" });
    // jsdom does not compute isContentEditable; the browser does.
    Object.defineProperty(editable, "isContentEditable", { configurable: true, value: true });
    editable.focus();
    await user.keyboard("n");
    expect(handlers.n).toHaveBeenCalledTimes(1);
  });

  it("runs ⌘K and Ctrl+K even while typing in a field", async () => {
    const { user } = show();
    screen.getByRole("textbox", { name: "Cari transaksi" }).focus();
    await user.keyboard("{Meta>}k{/Meta}");
    await user.keyboard("{Control>}k{/Control}");
    expect(handlers.modK).toHaveBeenCalledTimes(2);
    expect(handlers.n).not.toHaveBeenCalled();
  });

  it("Opening Bantuan: ? opens a 560 px dialog with the facts, the key table and the Tampilan line; Esc returns focus", async () => {
    const { user } = show();
    const outside = screen.getByRole("button", { name: "Di luar kolom" });
    outside.focus();
    await user.keyboard("?");
    const help = await screen.findByRole("dialog", { name: "Bantuan & pintasan" });
    expect(help).toHaveAttribute("open");
    expect(help.style.getPropertyValue("--dw")).toBe("560px");
    expect(help).toContainElement(document.activeElement as HTMLElement);
    expect(within(help).getAllByRole("listitem")).toHaveLength(3);
    const table = within(help).getByRole("table", { name: "Pintasan keyboard" });
    expect(within(table).getAllByRole("columnheader").map((cell) => cell.textContent)).toEqual(["Tombol", "Fungsi"]);
    const rows = (within(table).getAllByRole("row").slice(1) as HTMLTableRowElement[]).map((row) => [row.querySelector("kbd")?.textContent, row.cells[1].textContent]);
    expect(rows).toEqual([
      ["⌘K / Ctrl K", "Cari transaksi"],
      ["N", "Catat transaksi"],
      ["?", "Bantuan ini"],
      ["Esc", "Tutup lapisan teratas"],
    ]);
    expect(help).toHaveTextContent("Pintasan satu tombol bisa dimatikan di Pengaturan › Tampilan.");
    expect(within(help).queryByRole("switch")).not.toBeInTheDocument();
    expect(help).not.toHaveTextContent("titik-titik");
    expect((await axe(help)).violations).toHaveLength(0);

    await user.keyboard("{Escape}");
    expect(help).not.toHaveAttribute("open");
    expect(outside).toHaveFocus();
  });

  it("keeps Tab inside the open help dialog", async () => {
    const { user } = show();
    await user.keyboard("?");
    const help = await screen.findByRole("dialog", { name: "Bantuan & pintasan" });
    const close = screen.getByRole("button", { name: "Tutup bantuan" });
    close.focus();
    await user.tab();
    expect(close).toHaveFocus();
    await user.tab({ shift: true });
    expect(help).toContainElement(document.activeElement as HTMLElement);
  });

  it("with single-key shortcuts off, N and ? do nothing while ⌘K and Esc still work", async () => {
    const { user } = show();
    await turnSingleKeysOff(user);
    expect(window.localStorage.getItem(SHORTCUTS_STORAGE_KEY)).toBe("off");

    const outside = screen.getByRole("button", { name: "Di luar kolom" });
    outside.focus();
    await user.keyboard("n?");
    expect(handlers.n).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog", { hidden: true })).not.toHaveAttribute("open");

    await user.keyboard("{Control>}k{/Control}");
    expect(handlers.modK).toHaveBeenCalledTimes(1);

    await user.click(screen.getByRole("button", { name: "Bantuan" }));
    expect(screen.getByRole("dialog", { name: "Bantuan & pintasan" })).toHaveAttribute("open");
    await user.keyboard("{Escape}");
    expect(screen.getByRole("dialog", { hidden: true })).not.toHaveAttribute("open");
  });

  it("remembers the switch under the shortcuts key across a remount", async () => {
    const first = show();
    await turnSingleKeysOff(first.user);
    first.unmount();

    const { user } = show();
    screen.getByRole("button", { name: "Di luar kolom" }).focus();
    await user.keyboard("n");
    expect(handlers.n).not.toHaveBeenCalled();
    expect(screen.getByRole("switch", { name: /Pintasan satu tombol/ })).toHaveAttribute("aria-checked", "false");

    await user.click(screen.getByRole("switch", { name: /Pintasan satu tombol/ }));
    expect(window.localStorage.getItem(SHORTCUTS_STORAGE_KEY)).toBe("on");
  });

  it("keeps working when storage throws", async () => {
    const blocked = () => {
      throw new Error("blocked");
    };
    installStorage({ getItem: blocked, setItem: blocked });
    const { user } = show();
    screen.getByRole("button", { name: "Di luar kolom" }).focus();
    await user.keyboard("n");
    expect(handlers.n).toHaveBeenCalledTimes(1);
    await turnSingleKeysOff(user);
    screen.getByRole("button", { name: "Di luar kolom" }).focus();
    await user.keyboard("n");
    expect(handlers.n).toHaveBeenCalledTimes(1);
  });
});

const scoped = { region: vi.fn(), global: vi.fn() };

function ScopedRegion() {
  const region = useRef<HTMLDivElement>(null);
  useShortcut("j", () => scoped.region(), region);
  return (
    <div ref={region} aria-label="Antrean" role="region">
      <button type="button">Baris pertama</button>
    </div>
  );
}

function GlobalJ() {
  useShortcut("j", () => scoped.global());
  return null;
}

describe("scoped shortcuts", () => {
  beforeEach(() => vi.clearAllMocks());

  it("fires only while focus is inside its region", async () => {
    const user = userEvent.setup();
    render(
      <ShortcutProvider>
        <button type="button">Di luar</button>
        <ScopedRegion />
      </ShortcutProvider>,
    );
    screen.getByRole("button", { name: "Di luar" }).focus();
    await user.keyboard("j");
    expect(scoped.region).not.toHaveBeenCalled();

    screen.getByRole("button", { name: "Baris pertama" }).focus();
    await user.keyboard("j");
    expect(scoped.region).toHaveBeenCalledTimes(1);
  });

  it("prefers the scoped handler inside its region and the global one elsewhere", async () => {
    const user = userEvent.setup();
    render(
      <ShortcutProvider>
        <button type="button">Di luar</button>
        <ScopedRegion />
        <GlobalJ />
      </ShortcutProvider>,
    );
    screen.getByRole("button", { name: "Baris pertama" }).focus();
    await user.keyboard("j");
    screen.getByRole("button", { name: "Di luar" }).focus();
    await user.keyboard("j");
    expect(scoped.region).toHaveBeenCalledTimes(1);
    expect(scoped.global).toHaveBeenCalledTimes(1);
  });

  it("stops firing once its component unmounts", async () => {
    const user = userEvent.setup();
    const { rerender } = render(
      <ShortcutProvider>
        <GlobalJ />
      </ShortcutProvider>,
    );
    rerender(<ShortcutProvider>{null}</ShortcutProvider>);
    await act(async () => {
      await user.keyboard("j");
    });
    expect(scoped.global).not.toHaveBeenCalled();
  });
});

describe("isTypingTarget", () => {
  it("recognises form fields and editable content only", () => {
    expect(isTypingTarget(document.createElement("input"))).toBe(true);
    expect(isTypingTarget(document.createElement("textarea"))).toBe(true);
    expect(isTypingTarget(document.createElement("select"))).toBe(true);
    // jsdom leaves isContentEditable undefined, so non-fields come back falsy rather than false here.
    expect(isTypingTarget(document.createElement("button"))).toBeFalsy();
    expect(isTypingTarget(null)).toBe(false);
  });

  it("lets single keys through on a checkbox or radio, which take no text (apply-markas-transaksi D18)", () => {
    for (const type of ["checkbox", "radio"]) {
      const input = document.createElement("input");
      input.type = type;
      expect(isTypingTarget(input)).toBe(false);
    }
    const search = document.createElement("input");
    search.type = "search";
    expect(isTypingTarget(search)).toBe(true);
  });

  it("Page keys in the help", async () => {
    installStorage();
    const group: PageShortcutGroup = { name: "Di Tinjau", entries: [{ keys: ["J", "K"], text: "Pindah item" }] };
    function PageKeys() {
      usePageShortcutGroup(group);
      return null;
    }
    const user = userEvent.setup();
    const view = render(<ShortcutProvider><GlobalKeys /><PageKeys /></ShortcutProvider>);
    await user.click(screen.getByRole("button", { name: "Bantuan" }));
    let help = await screen.findByRole("dialog", { name: "Bantuan & pintasan" });
    const pageRow = within(help).getByRole("row", { name: /Pindah item di Tinjau/ });
    expect(pageRow.querySelector("kbd")).toHaveTextContent("J / K");
    expect(help).toHaveTextContent("Pintasan satu tombol bisa dimatikan di Pengaturan › Tampilan.");
    expect((await axe(help)).violations).toHaveLength(0);
    view.unmount();
    // On a page without its own keys (Beranda) the group and the caption are gone.
    render(<ShortcutProvider><GlobalKeys /></ShortcutProvider>);
    await user.click(screen.getByRole("button", { name: "Bantuan" }));
    help = await screen.findByRole("dialog", { name: "Bantuan & pintasan" });
    expect(help).not.toHaveTextContent("di Tinjau");
    expect(help).toHaveTextContent("Pintasan satu tombol bisa dimatikan di Pengaturan › Tampilan.");
  });
});
