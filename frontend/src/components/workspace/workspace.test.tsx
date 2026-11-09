import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import AppLayout, { planNotifications } from "@/components/layout/AppLayout";
import { api, getDevices } from "@/lib/api";
import type { DueExpectation, RecurringRule } from "@/types/recurring";
import { PageActions, WorkspaceContext, useWorkspace, type WorkspaceValue } from "./context";
import { PHONE_QUERY } from "./layers";
import { renderWithIntl, testWords } from "@/lib/i18n/testing";
import { HelpDialog } from "./shortcuts";
import { MORE_LINKS, NAV_GROUPS, NAV_LINKS, frameWidth, isActivePath } from "./navigation";
import { untranslatedPageTitle } from "./untranslatedPages";

const { t } = testWords();
import { PhoneHeader, PhoneTabBar } from "./PhoneNav";
import { SideNav } from "./SideNav";
import { ShortcutProvider, useShortcut } from "./shortcuts";
import { TitleBlock } from "./TitleBlock";
import { TopBar } from "./TopBar";

const navigation = vi.hoisted(() => ({ pathname: "/", push: vi.fn(), refresh: vi.fn() }));

vi.mock("next/navigation", () => ({
  usePathname: () => navigation.pathname,
  useRouter: () => ({ push: navigation.push, refresh: navigation.refresh }),
}));
vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn() },
  ApiError: class extends Error {
    status = 0;
  },
  getDevices: vi.fn(),
}));
vi.mock("@/components/capture/CaptureDialog", () => ({
  default: ({ request }: { request: { view: string } }) => (
    <div role="dialog" aria-label={request.view === "sentence" ? "Tulis transaksi" : "Catat transaksi"} />
  ),
}));

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

function stubMatchMedia(phone = false) {
  vi.stubGlobal(
    "matchMedia",
    vi.fn((query: string) => ({ matches: phone && query === PHONE_QUERY, media: query, addEventListener: vi.fn(), removeEventListener: vi.fn() })),
  );
}

const NOTIFICATIONS: WorkspaceValue["notifications"] = [
  { id: "report-2026-10-31", title: "Laporan Oktober siap dibaca", detail: "Periodenya selesai 31 Okt", href: "/laporan?basis=month" },
  { id: "plan-1-2026-11-05", title: "Tagihan SPayLater Rp 434.000 hari ini", detail: "Rencana · jatuh tempo 5 Nov", opensDuePlans: true },
];

function workspaceValue(overrides: Partial<WorkspaceValue> = {}): WorkspaceValue {
  return {
    username: "enrico",
    displayName: null,
    reviewAttention: 3,
    reportBadge: true,
    notifications: NOTIFICATIONS,
    unreadIds: new Set(NOTIFICATIONS.map((item) => item.id)),
    markNotificationsSeen: vi.fn(),
    openDuePlans: vi.fn(),
    themeChoice: "system",
    resolvedTheme: "light",
    setThemeChoice: vi.fn(),
    hideBalances: false,
    toggleHideBalances: vi.fn(),
    collapsed: false,
    toggleCollapsed: vi.fn(),
    openCapture: vi.fn(),
    openMovementCapture: vi.fn(),
    openHelp: vi.fn(),
    logout: vi.fn(),
    header: null,
    setHeader: vi.fn(),
    actionsSlot: null,
    setActionsSlot: vi.fn(),
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

describe("navigation model", () => {
  it("groups the seven pages under HARIAN, RINGKASAN and SIMPANAN with every href unchanged", () => {
    expect(NAV_GROUPS.map((group) => [t(`nav.groups.${group.key}`), group.links.map((link) => t(`nav.${link.key}`))])).toEqual([
      ["Harian", ["Beranda", "Tinjau", "Transaksi"]],
      ["Ringkasan", ["Analisis", "Laporan"]],
      ["Simpanan", ["Rekening & Kantong", "Target & Tagihan"]],
    ]);
    expect(NAV_LINKS.map((link) => link.href)).toEqual(["/", "/review", "/ledger?period=cycle", "/insights", "/laporan", "/accounts", "/goals"]);
  });

  it("orders Lainnya like the sidebar, then Pengaturan", () => {
    expect(MORE_LINKS.map((link) => t(`nav.${link.key}`))).toEqual(["Analisis", "Laporan", "Rekening & Kantong", "Target & Tagihan", "Pengaturan"]);
  });

  it("keeps isActivePath", () => {
    expect(isActivePath("/", "/")).toBe(true);
    expect(isActivePath("/ledger", "/ledger?period=cycle")).toBe(true);
    expect(isActivePath("/settings/tampilan", "/settings/perangkat")).toBe(true);
    expect(isActivePath("/review", "/")).toBe(false);
  });

  it("gives Transaksi and Pengaturan the 1040 frame and the prototype's subtitles", () => {
    // Transaksi writes its title from the catalogue since apply-markas-transaksi, so it has no untranslated entry.
    expect({ title: untranslatedPageTitle("/ledger"), width: frameWidth("/ledger") }).toEqual({ title: null, width: 1040 });
    // Every product page writes its title from the catalogue since its phase; Pengaturan was the last
    // (apply-markas-pengaturan D19), so the local component sheet is the untranslated example.
    expect({ title: untranslatedPageTitle("/settings/profil"), width: frameWidth("/settings/profil") }).toEqual({ title: null, width: 1040 });
    expect(untranslatedPageTitle("/komponen")).toEqual({ title: "Komponen", subtitle: "Lembar komponen Markas+, hanya di build lokal" });
    expect(untranslatedPageTitle("/accounts")).toBeNull();
    expect(untranslatedPageTitle("/goals")).toBeNull();
    expect(frameWidth("/")).toBe(1200);
  });

  it("switches the phone layouts below 768 px", () => {
    expect(PHONE_QUERY).toBe("(max-width: 767px)");
  });
});

describe("SideNav", () => {
  it("Navigating via streamlined sidebar: three labelled groups, the links in order and the foot", async () => {
    navigation.pathname = "/ledger";
    const { container } = renderShell(<SideNav />, { displayName: "Raka Contoh" });
    const nav = screen.getByRole("navigation", { name: "Menu utama" });
    const groups = within(nav).getAllByRole("group");
    expect(groups.map((group) => group.getAttribute("aria-labelledby") && document.getElementById(group.getAttribute("aria-labelledby")!)?.textContent)).toEqual([
      "Harian",
      "Ringkasan",
      "Simpanan",
    ]);
    expect(within(groups[0]).getAllByRole("link").map((link) => link.getAttribute("href"))).toEqual(["/", "/review", "/ledger?period=cycle"]);
    expect(within(nav).getAllByRole("link").map((link) => link.getAttribute("href"))).toEqual([
      "/", "/review", "/ledger?period=cycle", "/insights", "/laporan", "/accounts", "/goals",
    ]);
    const current = within(nav).getAllByRole("link").filter((link) => link.getAttribute("aria-current") === "page");
    expect(current.map((link) => link.getAttribute("aria-label"))).toEqual(["Transaksi"]);
    // The straight 2 × 20 px bar is the shared `.ws-select-bar` (selectionMarks.test.ts guards its geometry).
    expect(current[0]).toHaveClass("nav-item", "ws-select-bar");
    const aside = screen.getByRole("complementary", { name: "Navigasi samping" });
    expect(within(aside).getByRole("link", { name: "Pengaturan" })).toHaveAttribute("href", "/settings");
    expect(within(aside).getByRole("button", { name: "Akun Raka Contoh" })).toHaveTextContent("RCRaka Contohenrico");
    expect(within(aside).queryByText(/HP terhubung|Belum ada HP/)).not.toBeInTheDocument();
    expect(within(aside).queryByRole("button", { name: /Bantuan|Catat|Tema/ })).not.toBeInTheDocument();
    expect(within(aside).queryByRole("link", { name: /Tanya/ })).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("marks Pengaturan current on a settings sub-page", () => {
    navigation.pathname = "/settings/perangkat";
    renderShell(<SideNav />);
    expect(screen.getByRole("link", { name: "Pengaturan" })).toHaveAttribute("aria-current", "page");
  });

  it("shows the Tinjau and Laporan counts as muted text and announces them in words", () => {
    const { rerenderWith, container } = renderShell(<SideNav />);
    const review = screen.getByRole("link", { name: "Tinjau, 3 perlu ditinjau" });
    expect(review).toHaveAttribute("href", "/review");
    expect(within(review).getByText("3")).toHaveClass("count");
    expect(within(review).getByText("3")).toHaveAttribute("aria-hidden", "true");
    expect(screen.getByRole("link", { name: "Laporan, laporan baru siap" })).toHaveTextContent("1");
    expect(container.querySelector(".ws-count, .ws-rail-count")).toBeNull();

    rerenderWith({ reviewAttention: 0, reportBadge: false });
    expect(screen.getByRole("link", { name: "Tinjau" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Laporan" })).toBeInTheDocument();
    expect(container.querySelector(".nav-item .count")).toBeNull();
  });

  it("caps a large count at 99+ while the name keeps the real count", () => {
    renderShell(<SideNav />, { reviewAttention: 140 });
    expect(within(screen.getByRole("link", { name: "Tinjau, 140 perlu ditinjau" })).getByText("99+")).toBeInTheDocument();
  });

  it("Toggling sidebar collapse on desktop: the toggle reports its state", async () => {
    const { user, value, rerenderWith } = renderShell(<SideNav />);
    const toggle = screen.getByRole("button", { name: "Ciutkan menu samping" });
    expect(toggle).toHaveAttribute("aria-pressed", "false");
    await user.click(toggle);
    expect(value.toggleCollapsed).toHaveBeenCalledOnce();
    rerenderWith({ collapsed: true });
    expect(screen.getByRole("button", { name: "Bentangkan menu samping" })).toHaveAttribute("aria-pressed", "true");
  });

  it("gives every rail item an accessible name with its count and a hidden tooltip with its label", () => {
    const { container } = renderShell(<SideNav />, { collapsed: true });
    const items = Array.from(container.querySelectorAll<HTMLElement>(".nav-item"));
    expect(items).toHaveLength(8);
    for (const item of items) {
      const name = item.getAttribute("aria-label")!;
      const tip = item.querySelector(".nav-tip")!;
      expect(tip).toHaveAttribute("aria-hidden", "true");
      expect(name.startsWith(tip.textContent!)).toBe(true);
    }
    expect(screen.getByRole("link", { name: "Tinjau, 3 perlu ditinjau" })).toBeInTheDocument();
  });

  it("Account menu: a menu with the name and username, Profil and Keluar, operable with ↑/↓ and Esc", async () => {
    const { user, value } = renderShell(<SideNav />, { displayName: "Raka Contoh" });
    const button = screen.getByRole("button", { name: "Akun Raka Contoh" });
    expect(button).toHaveAttribute("aria-haspopup", "menu");
    await user.click(button);
    const menu = screen.getByRole("menu", { name: "Akun" });
    expect(menu).toHaveTextContent("Raka Contohenrico");
    const items = within(menu).getAllByRole("menuitem");
    expect(items.map((item) => item.textContent)).toEqual(["Profil", "Keluar"]);
    expect(items[0]).toHaveAttribute("href", "/settings/profil");
    expect(items[0]).toHaveFocus();
    await user.keyboard("{ArrowDown}");
    expect(items[1]).toHaveFocus();
    await user.keyboard("{ArrowDown}");
    expect(items[0]).toHaveFocus();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
    expect(button).toHaveFocus();
    await user.click(button);
    await user.click(within(screen.getByRole("menu")).getByRole("menuitem", { name: "Keluar" }));
    expect(value.logout).toHaveBeenCalledOnce();
    expect(screen.queryByText("Sembunyikan saldo")).not.toBeInTheDocument();
  });
});

describe("TopBar", () => {
  it("holds only the global actions, in the prototype's order, and no title", async () => {
    navigation.pathname = "/ledger";
    const { container } = renderShell(<TopBar />);
    const bar = container.querySelector("header.topbar")!;
    const controls = Array.from(bar.querySelectorAll<HTMLElement>("input, button")).map(
      (element) => element.getAttribute("aria-label") ?? element.textContent,
    );
    expect(controls).toEqual(["Cari transaksi", "Sembunyikan saldo", "Tema", "Notifikasi, 2 baru", "Catat"]);
    expect(within(bar as HTMLElement).queryByRole("heading")).not.toBeInTheDocument();
    expect(screen.getByRole("searchbox", { name: "Cari transaksi" })).toHaveAttribute("placeholder", "Cari transaksi…");
    expect(bar).toHaveTextContent("⌘K");
    expect(screen.getByRole("button", { name: "Catat" })).toHaveAttribute("aria-keyshortcuts", "N");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Search: Enter opens Transaksi with the query, a blank search opens it plain", async () => {
    const { user } = renderShell(<TopBar />);
    await user.type(screen.getByRole("searchbox", { name: "Cari transaksi" }), "warung{Enter}");
    expect(navigation.push).toHaveBeenCalledWith("/ledger?q=warung");
    await user.clear(screen.getByRole("searchbox"));
    await user.type(screen.getByRole("searchbox"), "   {Enter}");
    expect(navigation.push).toHaveBeenLastCalledWith("/ledger");
  });

  it("Search on Transaksi: ⌘K focuses the page's own search there and the top-bar search elsewhere", async () => {
    const offsetParent = vi.spyOn(HTMLElement.prototype, "offsetParent", "get").mockReturnValue(document.body);
    try {
      navigation.pathname = "/ledger";
      const { user, unmount } = renderShell(
        <>
          <TopBar />
          <input id="ledger-search" aria-label="Cari di Transaksi" />
        </>,
      );
      await user.keyboard("{Meta>}k{/Meta}");
      expect(screen.getByRole("textbox", { name: "Cari di Transaksi" })).toHaveFocus();
      expect(screen.getByRole("searchbox", { name: "Cari transaksi" })).toBeVisible();
      unmount();

      navigation.pathname = "/";
      const second = renderShell(<TopBar />);
      await second.user.keyboard("{Control>}k{/Control}");
      expect(screen.getByRole("searchbox", { name: "Cari transaksi" })).toHaveFocus();
      expect(navigation.push).not.toHaveBeenCalled();
    } finally {
      offsetParent.mockRestore();
    }
  });

  it("opens Transaksi with its search focused on ⌘K when the top bar is hidden on a phone", async () => {
    const { user } = renderShell(<TopBar />);
    await user.keyboard("{Control>}k{/Control}");
    expect(navigation.push).toHaveBeenCalledWith("/ledger?fokus=cari");
  });

  it("does not open capture when n is typed in the search, but does outside it", async () => {
    const { user, value } = renderShell(<TopBar />);
    await user.type(screen.getByRole("searchbox", { name: "Cari transaksi" }), "nasi");
    expect(value.openCapture).not.toHaveBeenCalled();
    screen.getByRole("searchbox").blur();
    await user.keyboard("n");
    expect(value.openCapture).toHaveBeenCalledOnce();
  });

  it("Hiding balances from the top bar: a pressed-state button with a constant name", async () => {
    const { user, value, rerenderWith } = renderShell(<TopBar />);
    const button = screen.getByRole("button", { name: "Sembunyikan saldo" });
    expect(button).toHaveAttribute("aria-pressed", "false");
    await user.click(button);
    expect(value.toggleHideBalances).toHaveBeenCalledOnce();
    rerenderWith({ hideBalances: true });
    expect(screen.getByRole("button", { name: "Sembunyikan saldo" })).toHaveAttribute("aria-pressed", "true");
  });

  it("Theme menu: radio items with the current one checked, ↓ and Enter choose, focus returns to Tema", async () => {
    const { user, value } = renderShell(<TopBar />, { themeChoice: "light" });
    const button = screen.getByRole("button", { name: "Tema" });
    await user.click(button);
    expect(button).toHaveAttribute("aria-expanded", "true");
    const menu = screen.getByRole("menu", { name: "Tema" });
    const items = within(menu).getAllByRole("menuitemradio");
    expect(items.map((item) => item.textContent)).toEqual(["Terang", "Gelap", "Ikuti sistem"]);
    expect(items[0]).toHaveAttribute("aria-checked", "true");
    expect(items[0]).toHaveFocus();
    expect((await axe(menu)).violations).toHaveLength(0);
    await user.keyboard("{ArrowDown}");
    expect(items[1]).toHaveFocus();
    await user.keyboard("{Enter}");
    expect(value.setThemeChoice).toHaveBeenCalledWith("dark");
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
    expect(button).toHaveFocus();
  });

  it("moves through a menu with Home and End, and closes it on a press outside", async () => {
    const { user } = renderShell(<TopBar />);
    await user.click(screen.getByRole("button", { name: "Tema" }));
    const items = within(screen.getByRole("menu")).getAllByRole("menuitemradio");
    await user.keyboard("{End}");
    expect(items[2]).toHaveFocus();
    await user.keyboard("{Home}");
    expect(items[0]).toHaveFocus();
    await user.keyboard("{ArrowUp}");
    expect(items[2]).toHaveFocus();
    await user.click(document.body);
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
  });

  it("Notifications: lists each item with its date and an unread dot, marks them seen on opening", async () => {
    const { user, value, rerenderWith } = renderShell(<TopBar />);
    const bell = screen.getByRole("button", { name: "Notifikasi, 2 baru" });
    expect(bell).toHaveTextContent("2");
    await user.click(bell);
    expect(value.markNotificationsSeen).toHaveBeenCalledOnce();
    const panel = screen.getByRole("dialog", { name: "Notifikasi" });
    const report = within(panel).getByRole("link", { name: /Laporan Oktober siap dibaca/ });
    expect(report).toHaveAttribute("href", "/laporan?basis=month");
    expect(report).toHaveTextContent("Periodenya selesai 31 Okt");
    expect(report).toHaveTextContent("(baru)");
    expect(report.querySelector(".unread")).not.toBeNull();
    expect((await axe(panel)).violations).toHaveLength(0);

    rerenderWith({ unreadIds: new Set() });
    expect(screen.getByRole("button", { name: "Notifikasi, tidak ada yang baru" })).not.toHaveTextContent(/\d/);
  });

  it("Due plan from the bell: a due item opens Rencana jatuh tempo and focus goes back to the bell first", async () => {
    const { user, value } = renderShell(<TopBar />);
    const bell = screen.getByRole("button", { name: "Notifikasi, 2 baru" });
    await user.click(bell);
    await user.click(screen.getByRole("button", { name: /Tagihan SPayLater Rp 434.000 hari ini/ }));
    expect(value.openDuePlans).toHaveBeenCalledOnce();
    expect(screen.queryByRole("dialog", { name: "Notifikasi" })).not.toBeInTheDocument();
    expect(bell).toHaveFocus();
  });

  it("closes the notifications on Esc and returns focus to the bell", async () => {
    const { user } = renderShell(<TopBar />);
    const bell = screen.getByRole("button", { name: "Notifikasi, 2 baru" });
    await user.click(bell);
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog", { name: "Notifikasi" })).not.toBeInTheDocument();
    expect(bell).toHaveFocus();
  });

  it("says there is nothing new without items", async () => {
    const { user } = renderShell(<TopBar />, { notifications: [], unreadIds: new Set() });
    await user.click(screen.getByRole("button", { name: "Notifikasi, tidak ada yang baru" }));
    expect(screen.getByRole("dialog", { name: "Notifikasi" })).toHaveTextContent("Belum ada yang baru.");
  });

  it("opens capture from + Catat", async () => {
    const { user, value } = renderShell(<TopBar />);
    await user.click(screen.getByRole("button", { name: "Catat" }));
    expect(value.openCapture).toHaveBeenCalledOnce();
  });
});

describe("planNotifications", () => {
  const now = new Date("2026-11-05T07:00:00+07:00");
  const rule = (id: string, name: string, due: string, extra: Partial<RecurringRule> = {}): RecurringRule => ({
    id,
    name,
    type: "expense",
    amount: 434_000,
    source_account_id: "jago",
    schedule_type: "monthly_day",
    is_payroll_allocation: false,
    auto_post: false,
    next_due_date: due,
    is_active: true,
    ...extra,
  });
  const pending = (base: RecurringRule): DueExpectation => ({ ...base, match_state: "none", window_days: 7, candidates: [] });

  it("No duplicate plan item: a plan due today in both sources is listed once and opens the due-plan view", () => {
    const spaylater = rule("r1", "Tagihan SPayLater", "2026-11-05");
    const items = planNotifications([spaylater], [pending(spaylater)], [], testWords(), now);
    expect(items).toEqual([
      { id: "plan-r1-2026-11-05", title: "Tagihan SPayLater Rp 434.000 hari ini", detail: "Rencana · jatuh tempo 5 Nov", opensDuePlans: true },
    ]);
  });

  it("orders by date, words overdue, tomorrow and the day after, and links later plans to Target & Tagihan", () => {
    const items = planNotifications(
      [rule("r2", "Internet", "2026-11-06"), rule("r3", "Listrik", "2026-11-07"), rule("r4", "Gaji", "2026-11-05", { type: "income" }), rule("r5", "Sewa", "2026-11-20")],
      [pending(rule("r6", "Kelas Yoga", "2026-11-02", { amount: 150_000 }))],
      [],
      testWords(),
      now,
    );
    expect(items.map((item) => [item.title, item.opensDuePlans ?? false, item.href ?? null])).toEqual([
      ["Kelas Yoga Rp 150.000 lewat jatuh tempo", true, null],
      ["Internet Rp 434.000 besok", false, "/goals"],
      ["Listrik Rp 434.000 lusa", false, "/goals"],
    ]);
  });

  it("adds one item for recent automatic links", () => {
    const items = planNotifications([], [], [
      { rule_id: "r1", scheduled_for: "2026-11-03" },
      { rule_id: "r2", scheduled_for: "2026-11-04" },
    ] as never, testWords(), now);
    expect(items).toEqual([
      { id: "autolink-r1-2026-11-03,r2-2026-11-04", title: "2 rencana ditautkan otomatis", detail: "Rencana · jatuh tempo 4 Nov", opensDuePlans: true },
    ]);
  });
});

describe("planNotifications with credit bills (apply-markas-target D16)", () => {
  const now = new Date("2026-11-05T07:00:00+07:00");
  const bill = (account: string, name: string, due: string, extra: Record<string, unknown> = {}) => ({
    account_id: account, account_name: name, due_date: due, statement_date: "2026-10-25", amount: 434_000, paid: 0, left_to_pay: 434_000,
    status: due < "2026-11-05" ? "overdue" : due === "2026-11-05" ? "due_today" : "upcoming", settled_on: null, ...extra,
  }) as never;
  const transferPlan: RecurringRule = {
    id: "r-spay", name: "Bayar SPayLater", type: "transfer", amount: 434_000, source_account_id: "jago", target_account_id: "spay",
    schedule_type: "monthly_day", is_payroll_allocation: false, auto_post: false, next_due_date: "2026-11-06", is_active: true,
  };

  it("Due credit bill: lists unpaid bills once per account and due date, opening the payment form", () => {
    const pay = vi.fn();
    const items = planNotifications([], [], [], testWords(), now, [
      bill("bca-cc", "Kartu BCA", "2026-11-01", { left_to_pay: 1_250_000 }),
      bill("bca-cc", "Kartu BCA", "2026-11-01", { left_to_pay: 1_250_000 }),
      bill("akulaku", "Akulaku", "2026-11-05"),
      bill("lunas", "Lunas", "2026-11-05", { status: "paid", left_to_pay: 0 }),
      bill("later", "Nanti", "2026-11-20"),
    ], pay);
    expect(items.map((item) => [item.id, item.title, item.detail])).toEqual([
      ["credit-bca-cc-2026-11-01", "Tagihan Kartu BCA Rp 1.250.000 lewat jatuh tempo", "Kredit · jatuh tempo 1 Nov"],
      ["credit-akulaku-2026-11-05", "Tagihan Akulaku Rp 434.000 hari ini", "Kredit · jatuh tempo 5 Nov"],
    ]);
    items[0].onSelect?.();
    expect(pay).toHaveBeenCalledWith(expect.objectContaining({ account_id: "bca-cc", left_to_pay: 1_250_000 }));
    expect(items[0].href).toBeUndefined();
  });

  it("lists a bill that a listed plan pays once, as the plan, and words English titles", () => {
    const items = planNotifications([transferPlan], [], [], testWords("en"), now, [bill("spay", "SPayLater", "2026-11-06")], vi.fn());
    expect(items.map((item) => item.title)).toEqual(["Bayar SPayLater Rp 434.000 due tomorrow"]);
    const english = planNotifications([], [], [], testWords("en"), now, [bill("spay", "SPayLater", "2026-11-05")], vi.fn());
    expect(english.map((item) => [item.title, item.detail])).toEqual([["SPayLater bill Rp 434.000 due today", "Credit · due 5 Nov"]]);
  });
});

function TitleWithActions({ more, children }: { more?: Parameters<typeof PageActions>[0]["more"]; children?: ReactNode }) {
  return (
    <>
      <TitleBlock />
      <PageActions more={more}>{children}</PageActions>
    </>
  );
}

describe("TitleBlock and PageActions", () => {
  function renderTitle(path: string, content: ReactNode, overrides: Partial<WorkspaceValue> = {}) {
    navigation.pathname = path;
    let slot: HTMLElement | null = null;
    const view = renderShell(content, { ...overrides, setActionsSlot: (element) => void (slot = element) });
    view.rerenderWith({ actionsSlot: slot, setActionsSlot: () => {} });
    return view;
  }

  it("takes the title and subtitle from the route and puts the page's actions on the title's edge", () => {
    renderTitle("/ledger", <TitleWithActions><button type="button">Periode</button></TitleWithActions>);
    const heading = screen.getByRole("heading", { level: 1, name: "Transaksi" });
    expect(heading).toHaveClass("t-title");
    expect(screen.getByText("Semua uang masuk, pengeluaran, dan pindah saldo")).toHaveClass("sub");
    const actions = document.querySelector(".tb-actions")!;
    expect(within(actions as HTMLElement).getByRole("button", { name: "Periode" })).toBeInTheDocument();
  });

  it("Page-written title: Beranda's date and cycle line, as the page's only h1", () => {
    renderTitle("/", <TitleBlock />, { header: { title: "Kamis, 5 November", subtitle: "Siklus gaji 23 Okt – 24 Nov · hari ke-14" } });
    expect(screen.getAllByRole("heading", { level: 1 }).map((heading) => heading.textContent)).toEqual(["Kamis, 5 November"]);
    expect(screen.getByText("Siklus gaji 23 Okt – 24 Nov · hari ke-14")).toBeInTheDocument();
  });

  it("shows no subtitle on Pengaturan", () => {
    renderTitle("/settings/profil", <TitleBlock />);
    expect(screen.getByRole("heading", { level: 1, name: "Pengaturan" })).toBeInTheDocument();
    expect(document.querySelector(".title-block .sub")).toBeNull();
  });

  it("More actions: at most two visible actions and the rest under Tindakan lain", async () => {
    const addDebt = vi.fn();
    const { user } = renderTitle(
      "/goals",
      <TitleWithActions more={[{ key: "utang", label: "Tambah utang", icon: "plus", onSelect: addDebt }]}>
        <button type="button" className="ws-btn">Rencana</button>
        <button type="button" className="ws-btn">Tambah target</button>
      </TitleWithActions>,
    );
    const actions = document.querySelector<HTMLElement>(".tb-actions")!;
    const visible = Array.from(actions.children).map((child) => child.getAttribute("aria-label") ?? child.textContent);
    expect(visible).toEqual(["Rencana", "Tambah target", "Tindakan lain"]);
    expect(actions.querySelector(".ws-btn-accent")).toBeNull();
    const more = within(actions).getByRole("button", { name: "Tindakan lain" });
    expect(more).toHaveAttribute("aria-haspopup", "menu");
    await user.click(more);
    const menu = screen.getByRole("menu", { name: "Tindakan lain" });
    await user.click(within(menu).getByRole("menuitem", { name: "Tambah utang" }));
    expect(addDebt).toHaveBeenCalledOnce();
    expect(more).toHaveFocus();
  });

  it("lists a phone-only action in … only below 768 px", async () => {
    const more = [
      { key: "harga", label: "Perbarui harga", icon: "refresh-cw" as const, onSelect: vi.fn(), phoneOnly: true },
      { key: "rencana", label: "Rencana", icon: "calendar" as const, onSelect: vi.fn() },
    ];
    const desk = renderTitle("/accounts", <TitleWithActions more={more} />);
    await desk.user.click(screen.getByRole("button", { name: "Tindakan lain" }));
    expect(within(screen.getByRole("menu")).getAllByRole("menuitem").map((item) => item.textContent)).toEqual(["Rencana"]);
    desk.unmount();

    stubMatchMedia(true);
    const phone = renderTitle("/accounts", <TitleWithActions more={more} />);
    await phone.user.click(screen.getByRole("button", { name: "Tindakan lain" }));
    expect(within(screen.getByRole("menu")).getAllByRole("menuitem").map((item) => item.textContent)).toEqual(["Perbarui harga", "Rencana"]);
    expect(screen.getByRole("menu")).toHaveClass("is-sheet");
  });

  it("renders the actions in place outside the shell, so page tests keep their buttons", () => {
    render(
      <PageActions>
        <button type="button">Tambah rekening</button>
      </PageActions>,
    );
    expect(screen.getByRole("button", { name: "Tambah rekening" })).toBeInTheDocument();
  });
});

describe("Phone shell", () => {
  it("Searching from the phone header: brand, search link, Sembunyikan saldo and the bell", async () => {
    const { container } = renderShell(<PhoneHeader />);
    const header = container.querySelector<HTMLElement>("header.ph-header")!;
    expect(within(header).getByRole("link", { name: "CashFlow" })).toHaveAttribute("href", "/");
    expect(within(header).getByRole("link", { name: "Cari transaksi" })).toHaveAttribute("href", "/ledger?fokus=cari");
    expect(within(header).getByRole("button", { name: "Sembunyikan saldo" })).toHaveAttribute("aria-pressed", "false");
    expect(within(header).getByRole("button", { name: "Notifikasi, 2 baru" })).toBeInTheDocument();
    expect(within(header).queryByRole("button", { name: /Akun/ })).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Notifications on a phone: a bottom sheet with focus inside, closed back to the bell", async () => {
    stubMatchMedia(true);
    const { user } = renderShell(<PhoneHeader />);
    const bell = screen.getByRole("button", { name: "Notifikasi, 2 baru" });
    await user.click(bell);
    const sheet = screen.getByRole("dialog", { name: "Notifikasi" });
    expect(sheet).toHaveClass("is-sheet");
    expect(sheet).toContainElement(document.activeElement as HTMLElement);
    await user.click(within(sheet).getByRole("button", { name: "Tutup notifikasi" }));
    expect(bell).toHaveFocus();
  });

  it("Catat on the tab bar and the tabs: Beranda, Tinjau with its count in words, Catat, Transaksi, Lainnya", async () => {
    const { container, value, user } = renderShell(<PhoneTabBar />);
    const bar = screen.getByRole("navigation", { name: "Navigasi ponsel" });
    const tabs = Array.from(bar.children).map((tab) => tab.getAttribute("aria-label") ?? tab.textContent);
    expect(tabs).toEqual(["Beranda", "Tinjau, 3 perlu ditinjau", "Catat transaksi", "Transaksi", "Lainnya"]);
    expect(within(bar).getByRole("link", { name: "Beranda" })).toHaveAttribute("aria-current", "page");
    const catat = within(bar).getByRole("button", { name: "Catat transaksi" });
    expect(catat).toHaveTextContent("Catat");
    expect(catat.querySelector(".plus")).not.toBeNull();
    await user.click(catat);
    expect(value.openCapture).toHaveBeenCalledOnce();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Active tab: Transaksi on /ledger, Lainnya on a page inside it", () => {
    navigation.pathname = "/ledger";
    const { unmount } = renderShell(<PhoneTabBar />);
    expect(screen.getByRole("link", { name: "Transaksi" })).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("button", { name: "Lainnya" })).not.toHaveAttribute("aria-current");
    unmount();
    navigation.pathname = "/accounts";
    renderShell(<PhoneTabBar />);
    expect(screen.getByRole("button", { name: "Lainnya" })).toHaveAttribute("aria-current", "page");
  });

  it("Reaching every page on a phone: Lainnya lists the pages in the sidebar order, Notifikasi and Tema", async () => {
    const { user, baseElement } = renderShell(<PhoneTabBar />, { themeChoice: "dark" });
    const more = screen.getByRole("button", { name: "Lainnya" });
    await user.click(more);
    expect(more).toHaveAttribute("aria-expanded", "true");
    const sheet = screen.getByRole("dialog", { name: "Lainnya" });
    const rows = within(sheet).getAllByRole("listitem").map((row) => row.textContent);
    expect(rows).toEqual(["Analisis", "Laporan1 baru", "Rekening & Kantong", "Target & Tagihan", "Pengaturan", "Notifikasi2 baru", "TemaGelap"]);
    expect(within(sheet).getByRole("link", { name: "Laporan, laporan baru siap" })).toHaveAttribute("href", "/laporan");
    // Keluar lives at the end of Pengaturan's rail since apply-markas-pengaturan D5.
    expect(within(sheet).queryByText(/Tanya|Bantuan|Keluar/)).not.toBeInTheDocument();
    expect(sheet).toContainElement(document.activeElement as HTMLElement);
    expect((await axe(baseElement)).violations).toHaveLength(0);
    await user.keyboard("{Escape}");
    expect(more).toHaveFocus();
  });

  it("traps Tab inside Lainnya and returns focus to Lainnya on Esc", async () => {
    const { user } = renderShell(<PhoneTabBar />);
    const more = screen.getByRole("button", { name: "Lainnya" });
    await user.click(more);
    const sheet = screen.getByRole("dialog", { name: "Lainnya" });
    for (let step = 0; step < 15; step += 1) {
      await user.tab();
      expect(sheet).toContainElement(document.activeElement as HTMLElement);
    }
    await user.keyboard("{Escape}");
    expect(sheet).not.toHaveAttribute("open");
    expect(more).toHaveFocus();
    expect(more).toHaveAttribute("aria-expanded", "false");
  });

  it("opens Notifikasi and Tema from Lainnya as sheets and returns focus to Lainnya", async () => {
    stubMatchMedia(true);
    const { user, value } = renderShell(<PhoneTabBar />);
    const more = screen.getByRole("button", { name: "Lainnya" });
    await user.click(more);
    await user.click(within(screen.getByRole("dialog", { name: "Lainnya" })).getByRole("button", { name: /Notifikasi/ }));
    expect(screen.getByRole("dialog", { name: "Notifikasi" })).toHaveClass("is-sheet");
    await user.keyboard("{Escape}");
    expect(more).toHaveFocus();

    await user.click(more);
    await user.click(within(screen.getByRole("dialog", { name: "Lainnya" })).getByRole("button", { name: /Tema/ }));
    const menu = screen.getByRole("menu", { name: "Tema" });
    expect(menu).toHaveClass("is-sheet");
    await user.click(within(menu).getByRole("menuitemradio", { name: "Gelap" }));
    expect(value.setThemeChoice).toHaveBeenCalledWith("dark");
    expect(more).toHaveFocus();
  });
});

describe("AppLayout wiring", () => {
  function today() {
    return new Date().toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });
  }

  function mockApi(pending: unknown[] = []) {
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path === "/auth/me") return { ok: true, user: { id: "qa-user", username: "enrico", currency: "IDR" } };
      if (path === "/auth/currency/rates") return { ok: true, usdidr: 16000 };
      if (path === "/review/summary") return { ok: true, attention: 4 };
      if (path === "/reports/monthly/latest") {
        return { ready: true, period: { label: "Oktober 2026", end_date: today() }, url: "/laporan?basis=month&start=2026-10-01" };
      }
      if (path === "/recurring") return { rules: [] };
      if (path === "/recurring/pending") return { ok: true, pending_count: pending.length, rules: pending, recent_auto_links: [] };
      if (path === "/ingest/test-sessions/current") return { ok: true, session: null };
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

  beforeEach(() => mockApi());
  afterEach(() => document.documentElement.classList.remove("dark"));

  it("renders the frame: skip link, sidebar, top bar, phone header, main with the title block and the tab bar", async () => {
    const { container } = showLayout();
    expect(await screen.findByRole("link", { name: "Lewati ke isi" })).toHaveAttribute("href", "#main");
    const main = screen.getByRole("main");
    expect(main).toHaveAttribute("id", "main");
    expect(main).toHaveClass("page");
    expect(main.style.getPropertyValue("--page-w")).toBe("1200px");
    expect(main.querySelector(".page-inner > .title-block h1")).toHaveTextContent("Beranda");
    expect(within(main).getByText("Isi halaman")).toBeInTheDocument();
    expect(container.querySelector(".ws-stage, .ws-window")).toBeNull();
    expect(screen.getByRole("navigation", { name: "Navigasi ponsel" })).toBeInTheDocument();
    expect(screen.getAllByRole("heading", { level: 1 })).toHaveLength(1);
  });

  it("persists the collapsed sidebar in sidebarCollapsed across a remount", async () => {
    const first = showLayout();
    await first.user.click(await screen.findByRole("button", { name: "Ciutkan menu samping" }));
    expect(window.localStorage.getItem("sidebarCollapsed")).toBe("1");
    expect(first.container.querySelector(".app")).toHaveAttribute("data-collapsed", "true");
    first.unmount();

    const second = showLayout();
    expect(await screen.findByRole("button", { name: "Bentangkan menu samping" })).toHaveAttribute("aria-pressed", "true");
    expect(second.container.querySelector(".app")).toHaveAttribute("data-collapsed", "true");
    await second.user.click(screen.getByRole("button", { name: "Bentangkan menu samping" }));
    expect(window.localStorage.getItem("sidebarCollapsed")).toBe("0");
  });

  it("feeds the counts and the bell from the review summary, the fresh report and the due plans", async () => {
    mockApi([{ id: "r1", name: "Tagihan SPayLater", type: "expense", amount: 434_000, next_due_date: today(), match_state: "none", window_days: 7, candidates: [] }]);
    showLayout();
    const side = await screen.findByRole("complementary", { name: "Navigasi samping" });
    expect(await within(side).findByRole("link", { name: "Tinjau, 4 perlu ditinjau" })).toBeInTheDocument();
    expect(await within(side).findByRole("link", { name: "Laporan, laporan baru siap" })).toBeInTheDocument();
    const tabs = screen.getByRole("navigation", { name: "Navigasi ponsel" });
    expect(within(tabs).getByRole("link", { name: "Tinjau, 4 perlu ditinjau" })).toBeInTheDocument();
    // The top bar and the phone header both carry the bell; CSS shows one of them per width.
    expect(await screen.findAllByRole("button", { name: "Notifikasi, 2 baru" })).toHaveLength(2);
  });

  it("Notifications: opening the bell clears the count and remembers the seen ids", async () => {
    const { user } = showLayout();
    const [bell] = await screen.findAllByRole("button", { name: "Notifikasi, 1 baru" });
    await user.click(bell);
    expect(within(screen.getByRole("dialog", { name: "Notifikasi" })).getByRole("link", { name: /Laporan Oktober 2026 siap dibaca \(baru\)/ })).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(screen.getAllByRole("button", { name: "Notifikasi, tidak ada yang baru" })).toHaveLength(2);
    expect(JSON.parse(window.localStorage.getItem("notificationsSeen")!)).toEqual([`report-${today()}`]);
  });

  it("Seen state unavailable: every item stays unread and nothing fails", async () => {
    const blocked = () => {
      throw new Error("blocked");
    };
    installStorage({ getItem: blocked, setItem: blocked });
    const { user } = showLayout();
    const [bell] = await screen.findAllByRole("button", { name: "Notifikasi, 1 baru" });
    await user.click(bell);
    await user.keyboard("{Escape}");
    expect(screen.getAllByRole("button", { name: "Notifikasi, 1 baru" })).toHaveLength(2);
  });

  it("opens Rencana jatuh tempo from a due item in the bell", async () => {
    mockApi([{ id: "r1", name: "Tagihan SPayLater", type: "expense", amount: 434_000, next_due_date: today(), match_state: "none", window_days: 7, candidates: [] }]);
    const { user } = showLayout();
    const [bell] = await screen.findAllByRole("button", { name: "Notifikasi, 2 baru" });
    await user.click(bell);
    await user.click(screen.getByRole("button", { name: /Tagihan SPayLater Rp 434.000 hari ini/ }));
    const drawer = await screen.findByRole("dialog", { name: /^Rencana jatuh tempo/ });
    expect(await within(drawer).findByText(/Tagihan SPayLater/)).toBeInTheDocument();
  });

  it("opens capture with N but not while typing n in the search", async () => {
    const { user } = showLayout();
    const search = await screen.findByRole("searchbox", { name: "Cari transaksi" });
    await user.type(search, "n");
    expect(screen.queryByRole("dialog", { name: "Tulis transaksi" })).not.toBeInTheDocument();
    search.blur();
    await user.keyboard("n");
    expect(await screen.findByRole("dialog", { name: "Tulis transaksi" })).toBeInTheDocument();
  });

  it("stores the theme choice from the Tema menu and applies the dark class", async () => {
    const { user } = showLayout();
    await user.click(await screen.findByRole("button", { name: "Tema" }));
    await user.click(screen.getByRole("menuitemradio", { name: "Gelap" }));
    expect(window.localStorage.getItem("theme")).toBe("dark");
    expect(document.documentElement).toHaveClass("dark");
    await user.click(screen.getByRole("button", { name: "Tema" }));
    expect(screen.getByRole("menuitemradio", { name: "Gelap" })).toHaveAttribute("aria-checked", "true");
  });

  it("hides balances from the top bar and remembers it under hideBalances", async () => {
    const { user } = showLayout();
    const [button] = await screen.findAllByRole("button", { name: "Sembunyikan saldo" });
    await user.click(button);
    await waitFor(() => expect(window.localStorage.getItem("hideBalances")).toBe("1"));
    expect(screen.getAllByRole("button", { name: "Sembunyikan saldo" }).every((item) => item.getAttribute("aria-pressed") === "true")).toBe(true);
  });

  it("signs out through the API from Keluar in the account menu", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const { user } = showLayout();
      await user.click(await screen.findByRole("button", { name: "Akun enrico" }));
      await user.click(screen.getByRole("menuitem", { name: "Keluar" }));
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/auth/logout"));
    } finally {
      error.mockRestore();
    }
  });
});

describe("the shell in English (add-language-system)", () => {
  function renderEnglish(children: ReactNode, overrides: Partial<WorkspaceValue> = {}) {
    const value = workspaceValue(overrides);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const view = renderWithIntl(
      <QueryClientProvider client={client}>
        <ShortcutProvider>
          <WorkspaceContext.Provider value={value}>{children}</WorkspaceContext.Provider>
        </ShortcutProvider>
      </QueryClientProvider>,
      { language: "en" },
    );
    return { ...view, value, user: userEvent.setup() };
  }

  beforeEach(() => {
    navigation.pathname = "/";
  });

  it("names the sidebar groups, pages, badges and the profile button in English", async () => {
    const { container } = renderEnglish(<SideNav />);
    const nav = screen.getByRole("navigation", { name: "Main menu" });
    const groupNames = within(nav).getAllByRole("group").map((group) => document.getElementById(group.getAttribute("aria-labelledby") ?? "")?.textContent);
    expect(groupNames).toEqual(["Daily", "Overview", "Money"]);
    expect(within(nav).getByRole("link", { name: "Home" })).toHaveAttribute("aria-current", "page");
    expect(within(nav).getByRole("link", { name: "Review, 3 need review" })).toBeInTheDocument();
    expect(within(nav).getByRole("link", { name: "Transactions" })).toBeInTheDocument();
    expect(within(nav).getByRole("link", { name: "Reports, new report ready" })).toBeInTheDocument();
    expect(within(nav).getByRole("link", { name: "Accounts & Pockets" })).toBeInTheDocument();
    expect(within(nav).getByRole("link", { name: "Goals & Bills" })).toBeInTheDocument();
    expect(screen.getByRole("complementary", { name: "Sidebar navigation" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Settings" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Collapse sidebar" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Profile, enrico" })).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("uses the singular badge for one item to review", () => {
    renderEnglish(<SideNav />, { reviewAttention: 1 });
    expect(screen.getByRole("link", { name: "Review, 1 needs review" })).toBeInTheDocument();
  });

  it("offers Profile and Sign out in the account menu", async () => {
    const { user, value } = renderEnglish(<SideNav />);
    await user.click(screen.getByRole("button", { name: "Profile, enrico" }));
    const menu = screen.getByRole("menu", { name: "Profile menu" });
    expect(within(menu).getByRole("menuitem", { name: "Profile" })).toBeInTheDocument();
    await user.click(within(menu).getByRole("menuitem", { name: "Sign out" }));
    expect(value.logout).toHaveBeenCalled();
  });

  it("words the top bar, the theme menu and the bell in English", async () => {
    const { user } = renderEnglish(<TopBar />);
    expect(screen.getByRole("searchbox", { name: "Search transactions" })).toHaveAttribute("placeholder", "Search transactions…");
    expect(screen.getByRole("button", { name: "Hide balances" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Add" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Theme" }));
    const menu = screen.getByRole("menu", { name: "Theme" });
    expect(within(menu).getAllByRole("menuitemradio").map((item) => item.textContent)).toEqual(["Light", "Dark", "Match system"]);
    await user.keyboard("{Escape}");
    await user.click(screen.getByRole("button", { name: "Notifications, 2 new" }));
    const panel = screen.getByRole("dialog", { name: "Notifications" });
    expect(within(panel).getByRole("button", { name: "Close notifications" })).toBeInTheDocument();
    expect(panel).toHaveTextContent("(new)");
  });

  it("says nothing new on an empty bell", async () => {
    const { user } = renderEnglish(<TopBar />, { notifications: [], unreadIds: new Set() });
    await user.click(screen.getByRole("button", { name: "Notifications, nothing new" }));
    expect(screen.getByRole("dialog", { name: "Notifications" })).toHaveTextContent("Nothing new yet.");
  });

  it("words the phone header, tab bar and More in English", async () => {
    stubMatchMedia(true);
    const { user } = renderEnglish(
      <>
        <PhoneHeader />
        <PhoneTabBar />
      </>,
    );
    expect(screen.getByRole("link", { name: "Search transactions" })).toBeInTheDocument();
    const tabs = screen.getByRole("navigation", { name: "Phone navigation" });
    expect(within(tabs).getByRole("link", { name: "Home" })).toBeInTheDocument();
    expect(within(tabs).getByRole("link", { name: "Review, 3 need review" })).toBeInTheDocument();
    expect(within(tabs).getByRole("button", { name: "Add transaction" })).toBeInTheDocument();
    expect(within(tabs).getByRole("link", { name: "Transactions" })).toBeInTheDocument();
    await user.click(within(tabs).getByRole("button", { name: "More" }));
    const more = screen.getByRole("dialog", { name: "More" });
    expect(within(more).getByRole("link", { name: "Reports, new report ready" })).toHaveTextContent("1 new");
    expect(within(more).getByRole("button", { name: /Notifications/ })).toHaveTextContent("2 new");
    expect(within(more).getByRole("button", { name: /Theme/ })).toHaveTextContent("Match system");
    expect(within(more).queryByRole("button", { name: "Sign out" })).not.toBeInTheDocument();
    expect(within(more).getByRole("button", { name: "Close More" })).toBeInTheDocument();
  });

  it("shows Help & shortcuts in English, with page keys still Indonesian and marked as such", () => {
    renderEnglish(<HelpDialog open onClose={() => {}} />);
    const dialog = screen.getByRole("dialog", { name: "Help & shortcuts" });
    expect(dialog).toHaveTextContent("The phone forwards bank notifications, and transactions are recorded automatically.");
    expect(dialog).toHaveTextContent("Home shows how much is safe to spend until payday.");
    expect(within(dialog).getByRole("table", { name: "Keyboard shortcuts" })).toBeInTheDocument();
    expect(within(dialog).getAllByRole("columnheader").map((cell) => cell.textContent)).toEqual(["Key", "Action"]);
    expect(dialog).toHaveTextContent("Close the top layer");
    expect(dialog).toHaveTextContent("Single-key shortcuts can be turned off in Settings › Appearance.");
    expect(within(dialog).getByRole("button", { name: "Close help" })).toBeInTheDocument();
  });

  it("names the plan notifications in English from the same dates", () => {
    const words = testWords("en");
    const plan = {
      id: "r1", name: "Tagihan SPayLater", amount: 434_000, type: "expense", next_due_date: "2026-11-05", is_active: true,
    } as unknown as RecurringRule;
    const items = planNotifications([plan], [], [], words, new Date("2026-11-05T03:00:00Z"));
    expect(items[0]).toMatchObject({ title: "Tagihan SPayLater Rp 434.000 due today", detail: "Plan · due 5 Nov" });
  });
});

describe("pages not translated yet (add-language-system D10)", () => {
  function mockProfile() {
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path === "/auth/me") return { ok: true, user: { id: "qa-user", username: "enrico", currency: "IDR", language: "en", region: "ID" } };
      if (path === "/auth/currency/rates") return { ok: true, usdidr: 16000 };
      if (path === "/review/summary") return { ok: true, attention: 0 };
      if (path === "/reports/monthly/latest") return { ready: false, period: null, url: null };
      if (path === "/recurring") return { rules: [] };
      if (path === "/recurring/pending") return { ok: true, pending_count: 0, rules: [], recent_auto_links: [] };
      if (path === "/ingest/test-sessions/current") return { ok: true, session: null };
      throw new Error(`Unexpected GET ${path}`);
    });
  }

  function showPage(pathname: string, language: "en" | "id") {
    navigation.pathname = pathname;
    if (language === "id") {
      vi.mocked(api.get).mockImplementation(async (path: string) => {
        if (path === "/auth/me") return { ok: true, user: { id: "qa-user", username: "enrico", currency: "IDR", language: "id", region: "ID" } };
        if (path === "/review/summary") return { ok: true, attention: 0 };
        if (path === "/reports/monthly/latest") return { ready: false, period: null, url: null };
        if (path === "/recurring") return { rules: [] };
        if (path === "/recurring/pending") return { ok: true, pending_count: 0, rules: [], recent_auto_links: [] };
        if (path === "/ingest/test-sessions/current") return { ok: true, session: null };
        if (path === "/auth/currency/rates") return { ok: true, usdidr: 16000 };
        throw new Error(`Unexpected GET ${path}`);
      });
    } else {
      mockProfile();
    }
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    return renderWithIntl(
      <QueryClientProvider client={client}>
        <AppLayout>
          <p>Isi halaman</p>
        </AppLayout>
      </QueryClientProvider>,
      { language },
    );
  }

  // The example is the local component sheet, the last route no phase translates (apply-markas-pengaturan D19).
  it("shows an English untranslated page's Indonesian title block and content marked lang=id, with the notice in English", async () => {
    showPage("/komponen", "en");
    const main = await screen.findByRole("main");
    expect(main).toHaveAttribute("lang", "id");
    expect(within(main).getByRole("heading", { level: 1, name: "Komponen" })).toHaveAttribute("lang", "id");
    const notice = within(main).getByText("This page is in Indonesian for now.");
    expect(notice).toHaveAttribute("lang", "en");
    expect(screen.getByRole("navigation", { name: "Main menu" })).toHaveTextContent("Review");
    expect(screen.getByRole("link", { name: "Skip to content" })).toBeInTheDocument();
  });

  it("shows no notice and no lang=id on an English Beranda", async () => {
    showPage("/", "en");
    const main = await screen.findByRole("main");
    expect(main).not.toHaveAttribute("lang");
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
    expect(document.querySelector("[lang='id']")).toBeNull();
  });

  it("shows no notice and no lang=id on an English Pengaturan, which apply-markas-pengaturan translated", async () => {
    showPage("/settings/anggaran", "en");
    const main = await screen.findByRole("main");
    expect(main).not.toHaveAttribute("lang");
    expect(within(main).getByRole("heading", { level: 1, name: "Settings" })).not.toHaveAttribute("lang");
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
  });

  it("shows no notice on an Indonesian Pengaturan", async () => {
    showPage("/settings", "id");
    const main = await screen.findByRole("main");
    expect(main).not.toHaveAttribute("lang");
    expect(within(main).getByRole("heading", { level: 1, name: "Pengaturan" })).not.toHaveAttribute("lang");
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
  });

  it("shows no notice and no lang=id on an English Analisis, which apply-markas-analisis translated", async () => {
    showPage("/insights", "en");
    const main = await screen.findByRole("main");
    expect(main).not.toHaveAttribute("lang");
    expect(within(main).getByRole("heading", { level: 1, name: "Analysis" })).not.toHaveAttribute("lang");
    expect(within(main).getByText("Where the money went, compared with the last period")).toBeInTheDocument();
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
    expect(document.querySelector("main [lang='id']")).toBeNull();
  });

  it("shows no notice and no lang=id on an English Transaksi, which apply-markas-transaksi translated", async () => {
    showPage("/ledger", "en");
    const main = await screen.findByRole("main");
    expect(main).not.toHaveAttribute("lang");
    expect(within(main).getByRole("heading", { level: 1, name: "Transactions" })).not.toHaveAttribute("lang");
    expect(within(main).getByText("All income, spending and transfers")).toBeInTheDocument();
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
    expect(document.querySelector("main [lang='id']")).toBeNull();
  });

  it("shows no notice and no lang=id on an English Tinjau, which apply-markas-tinjau translated", async () => {
    showPage("/review", "en");
    const main = await screen.findByRole("main");
    expect(main).not.toHaveAttribute("lang");
    expect(within(main).getByRole("heading", { level: 1, name: "Review" })).not.toHaveAttribute("lang");
    expect(within(main).getByText("What needs an answer and what was recorded automatically")).toBeInTheDocument();
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
    expect(document.querySelector("main [lang='id']")).toBeNull();
  });
});
