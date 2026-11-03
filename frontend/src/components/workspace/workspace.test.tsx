import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import AppLayout from "@/components/layout/AppLayout";
import { api, getDevices } from "@/lib/api";
import { WorkspaceContext, useWorkspace, type WorkspaceValue } from "./context";
import { PhoneTabBar } from "./PhoneNav";
import { SideNav } from "./SideNav";
import { ShortcutProvider, useShortcut } from "./shortcuts";
import { TopBar } from "./TopBar";

const navigation = vi.hoisted(() => ({ pathname: "/", push: vi.fn() }));

vi.mock("next/navigation", () => ({
  usePathname: () => navigation.pathname,
  useRouter: () => ({ push: navigation.push }),
}));
vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn() },
  ApiError: class extends Error {
    status = 0;
  },
  getDevices: vi.fn(),
}));
vi.mock("@/components/capture/CatatDialog", () => ({
  default: ({ open }: { open: boolean }) => (open ? <div role="dialog" aria-label="Catat transaksi" /> : null),
}));
vi.mock("@/components/ui/QuickCaptureModal", () => ({
  QuickCaptureModal: ({ open }: { open: boolean }) => (open ? <div role="dialog" aria-label="Isi manual" /> : null),
}));

/** Node's own experimental localStorage global shadows jsdom's, so each test gets an in-memory one. */
function installStorage() {
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
  };
  Object.defineProperty(window, "localStorage", { configurable: true, value: storage });
  return storage;
}

function stubMatchMedia() {
  vi.stubGlobal(
    "matchMedia",
    vi.fn((query: string) => ({ matches: false, media: query, addEventListener: vi.fn(), removeEventListener: vi.fn() })),
  );
}

function workspaceValue(overrides: Partial<WorkspaceValue> = {}): WorkspaceValue {
  return {
    username: "enrico",
    reviewAttention: 3,
    reportBadge: true,
    notifications: [
      { id: "report-2026-10-31", title: "Laporan Oktober siap dibaca", detail: "Periodenya selesai 31 Okt", href: "/laporan?basis=month" },
      { id: "plan-1", title: "Internet Rp 350.000 besok", detail: "Rencana · jatuh tempo 3 Nov", href: "/goals" },
    ],
    themeChoice: "system",
    resolvedTheme: "light",
    setThemeChoice: vi.fn(),
    hideBalances: false,
    toggleHideBalances: vi.fn(),
    collapsed: false,
    toggleCollapsed: vi.fn(),
    openCapture: vi.fn(),
    openHelp: vi.fn(),
    logout: vi.fn(),
    header: null,
    setHeader: vi.fn(),
    controlsSlot: null,
    setControlsSlot: vi.fn(),
    ...overrides,
  };
}

/** Mirrors AppLayout: the global N opens capture through the registry. */
function CaptureShortcut() {
  const { openCapture } = useWorkspace();
  useShortcut("n", openCapture);
  return null;
}

function renderShell(children: ReactNode, overrides: Partial<WorkspaceValue> = {}) {
  const value = workspaceValue(overrides);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrap = (current: WorkspaceValue, content: ReactNode) => (
    <QueryClientProvider client={client}>
      <ShortcutProvider>
        <WorkspaceContext.Provider value={current}>
          <CaptureShortcut />
          {content}
        </WorkspaceContext.Provider>
      </ShortcutProvider>
    </QueryClientProvider>
  );
  const view = render(wrap(value, children));
  return {
    ...view,
    value,
    user: userEvent.setup(),
    rerenderWith: (next: Partial<WorkspaceValue>) => view.rerender(wrap({ ...value, ...next }, children)),
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  navigation.pathname = "/";
  installStorage();
  stubMatchMedia();
  vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [] });
});
afterEach(() => vi.unstubAllGlobals());

describe("SideNav", () => {
  it("lists Harian, AI, Uang and Laporan in order with their links", async () => {
    const { container } = renderShell(<SideNav />);
    const nav = screen.getByRole("navigation", { name: "Halaman" });
    const groups = within(nav).getAllByRole("group");
    expect(groups.map((group) => group.getAttribute("aria-labelledby") && document.getElementById(group.getAttribute("aria-labelledby")!)?.textContent)).toEqual([
      "Harian",
      "AI",
      "Uang",
      "Laporan",
    ]);
    const hrefs = groups.map((group) => within(group).getAllByRole("link").map((link) => link.getAttribute("href")));
    expect(hrefs).toEqual([["/", "/review", "/ledger?period=cycle"], ["/tanya"], ["/accounts", "/goals"], ["/insights", "/laporan"]]);
    expect(within(groups[1]).getByRole("link", { name: "Tanya" })).toBeInTheDocument();
    expect(within(groups[2]).getByRole("link", { name: "Rekening & Kantong" })).toBeInTheDocument();
    expect(within(groups[2]).getByRole("link", { name: "Target & Tagihan" })).toBeInTheDocument();
    expect(within(groups[3]).getByRole("link", { name: "Analisis" })).toBeInTheDocument();
    await screen.findByRole("link", { name: /Belum ada HP terhubung/ });
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("marks Tanya active on its page", () => {
    navigation.pathname = "/tanya";
    renderShell(<SideNav />);
    expect(screen.getByRole("link", { name: "Tanya" })).toHaveAttribute("aria-current", "page");
  });

  it("marks only the active link with aria-current", () => {
    navigation.pathname = "/ledger";
    renderShell(<SideNav />);
    expect(screen.getByRole("link", { name: "Transaksi" })).toHaveAttribute("aria-current", "page");
    const current = screen.getAllByRole("link").filter((link) => link.getAttribute("aria-current") === "page");
    expect(current).toHaveLength(1);
  });

  it("marks Pengaturan active on a settings sub-page", () => {
    navigation.pathname = "/settings/perangkat";
    renderShell(<SideNav />);
    expect(screen.getByRole("link", { name: "Pengaturan" })).toHaveAttribute("aria-current", "page");
  });

  it("announces the Tinjau and Laporan badges in words", () => {
    const { rerenderWith } = renderShell(<SideNav />);
    expect(screen.getByRole("link", { name: "Tinjau, 3 perlu ditinjau" })).toHaveAttribute("href", "/review");
    expect(screen.getByRole("link", { name: "Laporan, laporan baru siap" })).toHaveAttribute("href", "/laporan");

    rerenderWith({ reviewAttention: 0, reportBadge: false });
    expect(screen.getByRole("link", { name: "Tinjau" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Laporan" })).toBeInTheDocument();
    expect(screen.queryByText("3")).not.toBeInTheDocument();
  });

  it("caps a large badge at 99+ while the name keeps the real count", () => {
    renderShell(<SideNav />, { reviewAttention: 140 });
    const link = screen.getByRole("link", { name: "Tinjau, 140 perlu ditinjau" });
    expect(within(link).getAllByText("99+")).toHaveLength(2);
  });

  it("toggles the collapse button and reports its state", async () => {
    const { user, value, rerenderWith } = renderShell(<SideNav />);
    const toggle = screen.getByRole("button", { name: "Ciutkan menu samping" });
    expect(toggle).toHaveAttribute("aria-pressed", "false");
    await user.click(toggle);
    expect(value.toggleCollapsed).toHaveBeenCalledOnce();

    rerenderWith({ collapsed: true });
    expect(screen.getByRole("button", { name: "Lebarkan menu samping" })).toHaveAttribute("aria-pressed", "true");
  });

  it("gives every rail item an accessible name and a tooltip with its label", async () => {
    const { container } = renderShell(<SideNav />, { collapsed: true });
    await screen.findByRole("link", { name: /Belum ada HP terhubung/ });
    const items = Array.from(container.querySelectorAll<HTMLElement>(".ws-nav-item, .ws-companion"));
    expect(items).toHaveLength(11);
    for (const item of items) {
      const name = item.getAttribute("aria-label");
      const tip = item.querySelector(".ws-tip");
      expect(name).toBeTruthy();
      expect(tip).toHaveAttribute("aria-hidden", "true");
      expect(name!.startsWith(tip!.textContent!)).toBe(true);
    }
    expect(screen.getByRole("button", { name: "Bantuan dan pintasan keyboard" }).querySelector(".ws-tip")).toHaveTextContent("Bantuan");
  });

  it("opens help from Bantuan and has no capture, avatar or theme control", async () => {
    const { user, value } = renderShell(<SideNav />);
    await user.click(screen.getByRole("button", { name: "Bantuan dan pintasan keyboard" }));
    expect(value.openHelp).toHaveBeenCalledOnce();
    const aside = screen.getByRole("complementary", { name: "Navigasi samping" });
    expect(within(aside).queryByRole("button", { name: /Catat|Tema|Akun/ })).not.toBeInTheDocument();
  });

  it("hides the companion status when the device request fails", async () => {
    vi.mocked(getDevices).mockRejectedValue(new Error("offline"));
    const { container } = renderShell(<SideNav />);
    await waitFor(() => expect(getDevices).toHaveBeenCalled());
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(container.querySelector(".ws-companion")).toBeNull();
    expect(screen.getByRole("link", { name: "Pengaturan" })).toBeInTheDocument();
  });
});

describe("TopBar", () => {
  it("takes its title from the route table unless the page sets one", () => {
    navigation.pathname = "/ledger";
    const { rerenderWith } = renderShell(<TopBar />);
    expect(screen.getByRole("heading", { level: 1, name: "Transaksi" })).toBeInTheDocument();
    expect(screen.getByText("Semua uang masuk, keluar, dan pindah saldo")).toBeInTheDocument();
    rerenderWith({ header: { title: "Senin, 2 November", subtitle: "Siklus 25 Okt – 24 Nov" } });
    expect(screen.getByRole("heading", { level: 1, name: "Senin, 2 November" })).toBeInTheDocument();
  });

  it("opens Transaksi with the search on Enter", async () => {
    const { user } = renderShell(<TopBar />);
    await user.type(screen.getByRole("searchbox", { name: "Cari transaksi" }), "warung{Enter}");
    expect(navigation.push).toHaveBeenCalledWith("/ledger?q=warung");
  });

  it("opens Transaksi without a query when the search is blank", async () => {
    const { user } = renderShell(<TopBar />);
    await user.type(screen.getByRole("searchbox", { name: "Cari transaksi" }), "   {Enter}");
    expect(navigation.push).toHaveBeenCalledWith("/ledger");
  });

  it("focuses the search with ⌘K and Ctrl+K", async () => {
    // jsdom has no layout, so offsetParent is always null; the visible search has one in a browser.
    const offsetParent = vi.spyOn(HTMLElement.prototype, "offsetParent", "get").mockReturnValue(document.body);
    try {
      const { user } = renderShell(<TopBar />);
      const search = screen.getByRole("searchbox", { name: "Cari transaksi" });
      await user.keyboard("{Meta>}k{/Meta}");
      expect(search).toHaveFocus();
      search.blur();
      await user.keyboard("{Control>}k{/Control}");
      expect(search).toHaveFocus();
      expect(navigation.push).not.toHaveBeenCalled();
    } finally {
      offsetParent.mockRestore();
    }
  });

  it("goes to Transaksi on ⌘K when the search is hidden at phone width", async () => {
    const { user } = renderShell(<TopBar />);
    await user.keyboard("{Control>}k{/Control}");
    expect(navigation.push).toHaveBeenCalledWith("/ledger");
  });

  it("does not open capture when n is typed in the search, but does outside it", async () => {
    const { user, value } = renderShell(<TopBar />);
    await user.type(screen.getByRole("searchbox", { name: "Cari transaksi" }), "nasi");
    expect(value.openCapture).not.toHaveBeenCalled();
    screen.getByRole("searchbox").blur();
    await user.keyboard("n");
    expect(value.openCapture).toHaveBeenCalledOnce();
  });

  it("offers Terang, Gelap and Ikuti sistem and applies the choice", async () => {
    const { user, value } = renderShell(<TopBar />);
    const button = screen.getByRole("button", { name: "Tema: Ikuti sistem" });
    await user.click(button);
    expect(button).toHaveAttribute("aria-expanded", "true");
    const menu = screen.getByRole("region", { name: "Pilih tema" });
    expect(within(menu).getAllByRole("button").map((item) => item.textContent)).toEqual(["Terang", "Gelap", "Ikuti sistem"]);
    expect(within(menu).getByRole("button", { name: "Ikuti sistem" })).toHaveAttribute("aria-pressed", "true");
    await user.click(within(menu).getByRole("button", { name: "Gelap" }));
    expect(value.setThemeChoice).toHaveBeenCalledWith("dark");
    expect(screen.queryByRole("region", { name: "Pilih tema" })).not.toBeInTheDocument();
  });

  it("closes a menu on Esc and returns focus to its button", async () => {
    const { user } = renderShell(<TopBar />);
    for (const name of ["Tema: Ikuti sistem", "Notifikasi, 2 baru", "Akun enrico"]) {
      const button = screen.getByRole("button", { name });
      await user.click(button);
      expect(button).toHaveAttribute("aria-expanded", "true");
      await user.tab();
      await user.keyboard("{Escape}");
      expect(button).toHaveAttribute("aria-expanded", "false");
      expect(button).toHaveFocus();
    }
  });

  it("closes a menu on a press outside", async () => {
    const { user } = renderShell(<TopBar />);
    await user.click(screen.getByRole("button", { name: "Akun enrico" }));
    await user.click(screen.getByRole("heading", { level: 1 }));
    expect(screen.queryByRole("region", { name: "Akun" })).not.toBeInTheDocument();
  });

  it("names the notifications button with its count and lists each item with a link", async () => {
    const { user, rerenderWith } = renderShell(<TopBar />);
    await user.click(screen.getByRole("button", { name: "Notifikasi, 2 baru" }));
    const panel = screen.getByRole("region", { name: "Notifikasi" });
    expect(within(panel).getByRole("link", { name: /Laporan Oktober siap dibaca/ })).toHaveAttribute("href", "/laporan?basis=month");
    expect(within(panel).getByRole("link", { name: /Internet Rp 350.000 besok/ })).toHaveTextContent("jatuh tempo 3 Nov");

    rerenderWith({ notifications: [] });
    expect(screen.getByRole("button", { name: "Notifikasi, tidak ada yang baru" })).toBeInTheDocument();
  });

  it("opens capture from + Catat", async () => {
    const { user, value } = renderShell(<TopBar />);
    await user.click(screen.getByRole("button", { name: "Catat" }));
    expect(value.openCapture).toHaveBeenCalledOnce();
  });

  it("shows the account menu with the username, balance privacy, Pengaturan and Keluar", async () => {
    const { user, value, rerenderWith } = renderShell(<TopBar />);
    await user.click(screen.getByRole("button", { name: "Akun enrico" }));
    const menu = screen.getByRole("region", { name: "Akun" });
    expect(menu).toHaveTextContent("Masuk sebagai enrico");
    expect(within(menu).getByRole("link", { name: "Pengaturan" })).toHaveAttribute("href", "/settings");
    await user.click(within(menu).getByRole("button", { name: "Sembunyikan saldo" }));
    expect(value.toggleHideBalances).toHaveBeenCalledOnce();

    rerenderWith({ hideBalances: true });
    await user.click(screen.getByRole("button", { name: "Akun enrico" }));
    expect(screen.getByRole("button", { name: "Tampilkan saldo" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Keluar" }));
    expect(value.logout).toHaveBeenCalledOnce();
  });

  it("has no axe violations with a menu open", async () => {
    const { user, container } = renderShell(<TopBar />);
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Akun enrico" }));
    expect((await axe(container)).violations).toHaveLength(0);
  });
});

describe("PhoneTabBar and MoreSheet", () => {
  it("shows Beranda, Tinjau with its badge in words, Catat, Transaksi and Lainnya", async () => {
    const { container, value, user } = renderShell(<PhoneTabBar />);
    const bar = screen.getByRole("navigation", { name: "Navigasi utama" });
    const tabs = Array.from(bar.children).map((tab) => tab.getAttribute("aria-label") ?? tab.textContent);
    expect(tabs).toEqual(["Beranda", "Tinjau, 3 perlu ditinjau", "Catat transaksi", "Transaksi", "Lainnya"]);
    expect(within(bar).getByRole("link", { name: "Beranda" })).toHaveAttribute("aria-current", "page");
    await user.click(within(bar).getByRole("button", { name: "Catat transaksi" }));
    expect(value.openCapture).toHaveBeenCalledOnce();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("marks Transaksi current on /ledger and Lainnya current on a page inside it", () => {
    navigation.pathname = "/ledger";
    const { unmount } = renderShell(<PhoneTabBar />);
    expect(screen.getByRole("link", { name: "Transaksi" })).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("button", { name: "Lainnya" })).not.toHaveAttribute("aria-current");
    unmount();

    navigation.pathname = "/accounts";
    renderShell(<PhoneTabBar />);
    expect(screen.getByRole("button", { name: "Lainnya" })).toHaveAttribute("aria-current", "page");
  });

  it("opens Lainnya as a dialog listing every other page, Bantuan and Keluar", async () => {
    const { user, value } = renderShell(<PhoneTabBar />);
    const more = screen.getByRole("button", { name: "Lainnya" });
    await user.click(more);
    expect(more).toHaveAttribute("aria-expanded", "true");
    const sheet = screen.getByRole("dialog", { name: "Lainnya" });
    const pages = within(sheet).getByRole("navigation", { name: "Halaman lain" });
    expect(within(pages).getAllByRole("link").map((link) => link.textContent)).toEqual([
      "Tanya",
      "Rekening & Kantong",
      "Target & Tagihan",
      "Analisis",
      "Laporan1",
      "Pengaturan",
    ]);
    expect(within(pages).getByRole("link", { name: "Laporan, laporan baru siap" })).toHaveAttribute("href", "/laporan");
    expect(within(pages).getByRole("link", { name: "Tanya" })).toHaveAttribute("href", "/tanya");
    expect(within(sheet).getByRole("region", { name: "Notifikasi" })).toBeInTheDocument();
    expect(within(sheet).getByRole("button", { name: "Bantuan" })).toBeInTheDocument();
    await user.click(within(sheet).getByRole("button", { name: "Keluar" }));
    expect(value.logout).toHaveBeenCalledOnce();
  });

  it("traps Tab and Shift+Tab inside the sheet and returns focus to Lainnya on Esc", async () => {
    const { user, baseElement } = renderShell(<PhoneTabBar />);
    const more = screen.getByRole("button", { name: "Lainnya" });
    await user.click(more);
    const sheet = screen.getByRole("dialog", { name: "Lainnya" });
    const close = within(sheet).getByRole("button", { name: "Tutup Lainnya" });
    const logout = within(sheet).getByRole("button", { name: "Keluar" });
    expect(close).toHaveFocus();
    expect((await axe(baseElement)).violations).toHaveLength(0);

    await user.tab({ shift: true });
    expect(logout).toHaveFocus();
    await user.tab();
    expect(close).toHaveFocus();
    for (let step = 0; step < 15; step += 1) {
      await user.tab();
      expect(sheet).toContainElement(document.activeElement as HTMLElement);
    }

    await user.keyboard("{Escape}");
    expect(sheet).not.toHaveAttribute("open");
    expect(more).toHaveFocus();
    expect(more).toHaveAttribute("aria-expanded", "false");
  });

  it("returns focus to Lainnya after the close button", async () => {
    const { user } = renderShell(<PhoneTabBar />);
    const more = screen.getByRole("button", { name: "Lainnya" });
    await user.click(more);
    const sheet = screen.getByRole("dialog", { name: "Lainnya" });
    await user.click(screen.getByRole("button", { name: "Tutup Lainnya" }));
    expect(sheet).not.toHaveAttribute("open");
    expect(more).toHaveFocus();
  });

  it("closes the sheet before opening help from Bantuan", async () => {
    const { user, value } = renderShell(<PhoneTabBar />);
    await user.click(screen.getByRole("button", { name: "Lainnya" }));
    const sheet = screen.getByRole("dialog", { name: "Lainnya" });
    await user.click(within(sheet).getByRole("button", { name: "Bantuan" }));
    expect(value.openHelp).toHaveBeenCalledOnce();
    expect(sheet).not.toHaveAttribute("open");
  });
});

describe("AppLayout wiring", () => {
  function today() {
    return new Date().toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });
  }

  function mockApi() {
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path === "/auth/me") return { ok: true, user: { id: "qa-user", username: "enrico", currency: "IDR" } };
      if (path === "/auth/currency/rates") return { ok: true, usdidr: 16000 };
      if (path === "/review/summary") return { ok: true, attention: 4 };
      if (path === "/reports/monthly/latest") {
        return { ready: true, period: { label: "Oktober 2026", end_date: today() }, url: "/laporan?basis=month&start=2026-10-01" };
      }
      if (path === "/recurring") return { rules: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
  }

  function showLayout() {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    const view = render(
      <QueryClientProvider client={client}>
        <AppLayout>
          <p>Isi halaman</p>
        </AppLayout>
      </QueryClientProvider>,
    );
    return { ...view, user };
  }

  beforeEach(mockApi);
  afterEach(() => document.documentElement.classList.remove("dark"));

  it("persists the collapsed sidebar in sidebarCollapsed across a remount", async () => {
    const first = showLayout();
    await first.user.click(await screen.findByRole("button", { name: "Ciutkan menu samping" }));
    expect(window.localStorage.getItem("sidebarCollapsed")).toBe("1");
    expect(first.container.querySelector(".ws-window")).toHaveAttribute("data-collapsed", "true");
    first.unmount();

    const second = showLayout();
    expect(await screen.findByRole("button", { name: "Lebarkan menu samping" })).toHaveAttribute("aria-pressed", "true");
    expect(second.container.querySelector(".ws-window")).toHaveAttribute("data-collapsed", "true");
    await second.user.click(screen.getByRole("button", { name: "Lebarkan menu samping" }));
    expect(window.localStorage.getItem("sidebarCollapsed")).toBe("0");
  });

  it("feeds the badges and notifications from the review summary and the fresh report", async () => {
    showLayout();
    const side = await screen.findByRole("complementary", { name: "Navigasi samping" });
    expect(await within(side).findByRole("link", { name: "Tinjau, 4 perlu ditinjau" })).toBeInTheDocument();
    expect(await within(side).findByRole("link", { name: "Laporan, laporan baru siap" })).toBeInTheDocument();
    const tabs = screen.getByRole("navigation", { name: "Navigasi utama" });
    expect(within(tabs).getByRole("link", { name: "Tinjau, 4 perlu ditinjau" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Notifikasi, 1 baru" })).toBeInTheDocument();
  });

  it("opens capture with N but not while typing n in the search", async () => {
    const { user } = showLayout();
    const search = await screen.findByRole("searchbox", { name: "Cari transaksi" });
    await user.type(search, "n");
    expect(screen.queryByRole("dialog", { name: "Catat transaksi" })).not.toBeInTheDocument();
    search.blur();
    await user.keyboard("n");
    expect(await screen.findByRole("dialog", { name: "Catat transaksi" })).toBeInTheDocument();
  });

  it("stores the theme choice and applies the dark class", async () => {
    const { user } = showLayout();
    await user.click(await screen.findByRole("button", { name: "Tema: Ikuti sistem" }));
    await user.click(screen.getByRole("button", { name: "Gelap" }));
    expect(window.localStorage.getItem("theme")).toBe("dark");
    expect(document.documentElement).toHaveClass("dark");
    expect(screen.getByRole("button", { name: "Tema: Gelap" })).toBeInTheDocument();
  });

  it("signs out through the API from Keluar", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const { user } = showLayout();
      await user.click(await screen.findByRole("button", { name: "Akun enrico" }));
      await user.click(screen.getByRole("button", { name: "Keluar" }));
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/auth/logout"));
    } finally {
      error.mockRestore();
    }
  });
});
