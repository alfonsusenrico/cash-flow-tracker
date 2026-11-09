import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, createMovement } from "@/lib/api";
import { useWorkspace } from "@/components/workspace/context";
import AppLayout, { useAppCtx } from "./AppLayout";

vi.mock("next/navigation", () => ({ usePathname: () => "/", useRouter: () => ({ push: vi.fn() }) }));
vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn() },
  ApiError: class extends Error {},
  createMovement: vi.fn(),
  uploadTransactionReceipt: vi.fn(),
  getDevices: vi.fn().mockResolvedValue({ ok: true, devices: [] }),
}));

function MovementTrigger() {
  const { openMovementCapture } = useWorkspace();
  return <button type="button" onClick={() => openMovementCapture({})}>Global movement</button>;
}

/** Laporan's data-quality panel opens the manual form in a mode. */
function QuickAddTrigger() {
  const { openQuickAdd } = useAppCtx();
  return <button type="button" onClick={() => openQuickAdd("income")}>Add income</button>;
}

/** "Bayar tagihan" in the Rekening detail (apply-markas-rekening D8): the account and the bill's amount filled in. */
function PayBillTrigger() {
  const { openMovementCapture } = useWorkspace();
  return <button type="button" onClick={() => openMovementCapture({ targetAccountId: "cash-b", amount: 434_000 })}>Pay bill</button>;
}

function renderLayout(children: React.ReactNode = <MovementTrigger />) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <AppLayout>{children}</AppLayout>
    </QueryClientProvider>,
  );
}

let currentSession: { id: string; status: "open" | "ended"; planned_end_at: string } | null = null;

describe("global movement entry point", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    currentSession = null;
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: vi.fn().mockReturnValue({ matches: false }),
    });
    const stored = new Map<string, string>();
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: {
        getItem: vi.fn((key: string) => stored.get(key) ?? null),
        setItem: vi.fn((key: string, value: string) => void stored.set(key, value)),
        removeItem: vi.fn((key: string) => void stored.delete(key)),
      },
    });
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/auth/me") return { ok: true, user: { id: "qa-user", currency: "IDR" } };
      if (path === "/auth/currency/rates") return { ok: true, usdidr: 16000 };
      if (path === "/accounts") return { accounts: [
        { id: "cash-a", name: "Cash A", type: "cash", balance: 1000 },
        { id: "cash-b", name: "Cash B", type: "bank", balance: 0 },
        { id: "stock", name: "Stock", type: "investment", balance: 5000 },
      ] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path === "/review/summary") return { ok: true, attention: 4, unreviewed: 3 };
      if (path === "/reports/monthly/latest") return { ready: false, period: null, url: null };
      if (path === "/recurring") return { rules: [] };
      if (path === "/recurring/pending") return { ok: true, rules: [], recent_auto_links: [] };
      if (path === "/ingest/test-sessions/current") return { ok: true, session: currentSession };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(createMovement).mockResolvedValue({
      ok: true,
      movement_id: "movement-1",
      expense_transaction_id: "movement-out",
      income_transaction_id: "movement-in",
    });
  });

  it("opens the capture dialog on Perpindahan from the shared workspace context", async () => {
    const user = userEvent.setup();
    renderLayout();

    await user.click(await screen.findByRole("button", { name: "Global movement" }));
    expect(await screen.findByRole("dialog", { name: "Catat transaksi" })).toBeVisible();
    expect(screen.getByRole("radio", { name: "Perpindahan" })).toHaveAttribute("aria-checked", "true");
    expect(screen.queryByRole("option", { name: /Stock/ })).not.toBeInTheDocument();
    expect(await screen.findByRole("combobox", { name: "Dari rekening" })).toHaveValue("cash-a");
    expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("cash-b");
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "300");
    await user.click(screen.getByRole("button", { name: "Pindahkan saldo" }));

    await waitFor(() => expect(createMovement).toHaveBeenCalledWith(expect.objectContaining({
      source_account_id: "cash-a",
      target_account_id: "cash-b",
      amount: 300,
    })));
    expect(api.post).not.toHaveBeenCalled();
  });

  it("opens Perpindahan with the target and amount filled in and posts nothing until it is saved", async () => {
    const user = userEvent.setup();
    renderLayout(<PayBillTrigger />);
    await user.click(await screen.findByRole("button", { name: "Pay bill" }));
    expect(await screen.findByRole("dialog", { name: "Catat transaksi" })).toBeVisible();
    expect(screen.getByRole("radio", { name: "Perpindahan" })).toHaveAttribute("aria-checked", "true");
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("cash-b"));
    expect(screen.getByRole("textbox", { name: "Nominal (IDR)" })).toHaveValue("434.000");
    expect(createMovement).not.toHaveBeenCalled();
    expect(api.post).not.toHaveBeenCalled();
  });

  it("puts every page inside the workspace: skip link, sidebar, main and the phone tab bar", async () => {
    renderLayout();
    expect(await screen.findByRole("link", { name: "Lewati ke isi" })).toHaveAttribute("href", "#main");
    expect(screen.getByRole("complementary", { name: "Navigasi samping" })).toBeInTheDocument();
    expect(screen.getByRole("main")).toHaveAttribute("id", "main");
    expect(screen.getByRole("navigation", { name: "Menu utama" })).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Navigasi ponsel" })).toBeInTheDocument();
    // Sidebar and tab bar both name the badge in words (CSS shows one of them per width).
    expect(await screen.findAllByRole("link", { name: "Tinjau, 4 perlu ditinjau" })).toHaveLength(2);
    expect(screen.queryByRole("button", { name: /Buka menu/ })).not.toBeInTheDocument();
  });

  it("Session running: one neutral line above the title block with the end time, Lihat sesi and Akhiri sesi", async () => {
    currentSession = { id: "sesi-1", status: "open", planned_end_at: "2026-11-04T11:30:00Z" };
    renderLayout();
    const line = await screen.findByRole("region", { name: "Sesi uji" });
    expect(line).toHaveTextContent("Sesi uji berjalan sampai 18.30 · notifikasi disimpan, tidak dicatat");
    expect(line).toHaveClass("session-line");
    expect(line.className).not.toMatch(/warn|ws-banner/);
    // Between the top bar and the page, so the title block follows it and it scrolls with the page.
    expect(line.nextElementSibling).toBe(screen.getByRole("main"));
    expect(within(line).getByRole("link", { name: "Lihat sesi" })).toHaveAttribute("href", "/settings/sesi-uji");
    expect(within(line).getByRole("button", { name: "Akhiri sesi" })).toHaveClass("ws-btn-quiet", "ws-btn-sm");
  });

  it("Ending from the notice: Akhiri sesi ends the session and the line disappears", async () => {
    const user = userEvent.setup();
    currentSession = { id: "sesi-1", status: "open", planned_end_at: "2026-11-04T11:30:00Z" };
    vi.mocked(api.post).mockImplementation(async () => {
      currentSession = { ...currentSession!, status: "ended" };
      return { ok: true };
    });
    renderLayout();
    const line = await screen.findByRole("region", { name: "Sesi uji" });
    await user.click(within(line).getByRole("button", { name: "Akhiri sesi" }));
    expect(api.post).toHaveBeenCalledWith("/ingest/test-sessions/sesi-1/end");
    await waitFor(() => expect(screen.queryByRole("region", { name: "Sesi uji" })).not.toBeInTheDocument());
  });

  it("shows no Sesi uji banner without a running session", async () => {
    renderLayout();
    await screen.findByRole("link", { name: "Lewati ke isi" });
    await waitFor(() => expect(api.get).toHaveBeenCalledWith("/ingest/test-sessions/current"));
    expect(screen.queryByRole("region", { name: "Sesi uji" })).not.toBeInTheDocument();
  });

  it("remembers the collapsed sidebar", async () => {
    const user = userEvent.setup();
    renderLayout();
    await user.click(await screen.findByRole("button", { name: "Ciutkan menu samping" }));
    expect(localStorage.setItem).toHaveBeenCalledWith("sidebarCollapsed", "1");
    expect(document.querySelector(".app")).toHaveAttribute("data-collapsed", "true");
  });

  it("opens capture with N and not while typing", async () => {
    const user = userEvent.setup();
    renderLayout(<input aria-label="Ketik" />);
    await screen.findByRole("main");
    await user.type(screen.getByRole("textbox", { name: "Ketik" }), "n");
    expect(screen.queryByRole("dialog", { name: "Tulis transaksi" })).not.toBeInTheDocument();
    (document.activeElement as HTMLElement).blur();
    await user.keyboard("n");
    const dialog = await screen.findByRole("dialog", { name: "Tulis transaksi" });
    expect(dialog).toBeVisible();
    await waitFor(() => expect(screen.getByRole("textbox", { name: "Kalimat transaksi" })).toHaveFocus());
    // N types into the field; it never opens a second dialog (D16).
    await user.keyboard("n");
    expect(screen.getAllByRole("dialog")).toHaveLength(1);
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Tulis transaksi" })).not.toBeInTheDocument());
  });

  it("opens the manual form in the mode Laporan asks for", async () => {
    const user = userEvent.setup();
    renderLayout(<QuickAddTrigger />);
    const trigger = await screen.findByRole("button", { name: "Add income" });
    await user.click(trigger);
    expect(await screen.findByRole("dialog", { name: "Catat transaksi" })).toBeVisible();
    expect(screen.getByRole("radio", { name: "Pemasukan" })).toHaveAttribute("aria-checked", "true");
    await user.click(screen.getByRole("button", { name: "Batal" }));
    await waitFor(() => expect(trigger).toHaveFocus());
  });
});
