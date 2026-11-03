import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ExplainProvider, ExplainableFigure, type ExplanationData } from "./explain";
import { PHONE_QUERY } from "./layers";
import { ShortcutProvider } from "./shortcuts";

const SAFE_TO_SPEND: ExplanationData = {
  title: "Aman dipakai hari ini",
  rule: "Sisa uang siklus ini dibagi hari sampai gaji berikutnya.",
  lines: [
    { label: "Masuk", amount: "+Rp 8.000.000" },
    { label: "Keluar", amount: "−Rp 3.100.000", href: "/ledger?type=expense" },
    { label: "Tagihan", note: "2 rencana belum dibayar", amount: "−Rp 1.200.000" },
    { label: "Setoran target", amount: "−Rp 1.000.000" },
    { label: "Sisa", amount: "Rp 2.700.000" },
    { label: "Dibagi 12 hari lagi", amount: "Rp 225.000" },
    { label: "Dibulatkan", amount: "−Rp 11.000" },
  ],
  total: { label: "Aman dipakai hari ini", amount: "Rp 214.000" },
  notes: ["3 transaksi masih menunggu ditinjau dan belum dihitung."],
  link: { href: "/ledger?period=cycle", label: "Lihat transaksinya" },
};

const UNAVAILABLE: ExplanationData = {
  title: "Keluar siklus ini",
  rule: "Semua uang keluar sejak gajian terakhir.",
  lines: [],
  total: { label: "Keluar siklus ini", amount: "Rp 999.999" },
  unavailable: { reason: "Tanggal gajian belum diatur.", fix: "Atur tanggal gajian di Pengaturan supaya siklusnya terhitung." },
};

function stubPhone(phone: boolean) {
  vi.stubGlobal(
    "matchMedia",
    vi.fn((query: string) => ({ matches: phone && query === PHONE_QUERY, media: query, addEventListener: vi.fn(), removeEventListener: vi.fn() })),
  );
}

function show() {
  const user = userEvent.setup();
  const view = render(
    <ShortcutProvider>
      <ExplainProvider>
        <main>
          <p>Ringkasan</p>
          <ExplainableFigure label="Aman dipakai hari ini" amountText="Rp 214.000" explanation={SAFE_TO_SPEND} />
          <ExplainableFigure label="Keluar siklus ini" amountText="belum tersedia" explanation={UNAVAILABLE} />
        </main>
      </ExplainProvider>
    </ShortcutProvider>,
  );
  const safe = screen.getByRole("button", { name: "Aman dipakai hari ini Rp 214.000: lihat rincian" });
  const spent = screen.getByRole("button", { name: "Keluar siklus ini belum tersedia: lihat rincian" });
  return { ...view, user, safe, spent };
}

describe("ExplainableFigure", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    document.body.style.overflow = "";
  });

  it("is a button that keeps the figure as text and names it with lihat rincian", () => {
    const { safe } = show();
    expect(safe).toHaveTextContent("Rp 214.000");
    expect(safe).toHaveAttribute("aria-haspopup", "dialog");
    expect(safe).toHaveAttribute("aria-expanded", "false");
    expect(safe).toHaveClass("ws-xn");
  });

  it("opens with Enter and moves focus to the explanation heading", async () => {
    const { user, safe } = show();
    safe.focus();
    await user.keyboard("{Enter}");
    const dialog = screen.getByRole("dialog", { name: "Aman dipakai hari ini" });
    expect(dialog).toHaveAttribute("data-mode", "popover");
    expect(dialog).toHaveAttribute("aria-modal", "false");
    expect(within(dialog).getByRole("heading", { name: "Aman dipakai hari ini" })).toHaveFocus();
    expect(safe).toHaveAttribute("aria-expanded", "true");
    expect(document.body.style.overflow).toBe("");
  });

  it("opens with Space too", async () => {
    const { user, safe } = show();
    safe.focus();
    await user.keyboard(" ");
    expect(screen.getByRole("dialog", { name: "Aman dipakai hari ini" })).toBeInTheDocument();
  });

  it("renders the eyebrow, rule, lines, total, notes and link from the data only", async () => {
    const { user, safe, baseElement } = show();
    await user.click(safe);
    const dialog = screen.getByRole("dialog", { name: "Aman dipakai hari ini" });
    expect(dialog).toHaveTextContent("Rincian hitungan");
    expect(dialog).toHaveTextContent(SAFE_TO_SPEND.rule);

    const rows = within(dialog).getAllByRole("row");
    expect(rows).toHaveLength(SAFE_TO_SPEND.lines.length + 1);
    SAFE_TO_SPEND.lines.forEach((line, index) => {
      expect(rows[index]).toHaveTextContent(line.label);
      expect(rows[index]).toHaveTextContent(line.amount);
    });
    expect(rows[2]).toHaveTextContent("2 rencana belum dibayar");
    expect(rows[rows.length - 1]).toHaveClass("is-total");
    expect(rows[rows.length - 1]).toHaveTextContent("Aman dipakai hari iniRp 214.000");

    expect(within(dialog).getByRole("link", { name: "Keluar" })).toHaveAttribute("href", "/ledger?type=expense");
    expect(dialog).toHaveTextContent("3 transaksi masih menunggu ditinjau dan belum dihitung.");
    expect(within(dialog).getByRole("link", { name: "Lihat transaksinya" })).toHaveAttribute("href", "/ledger?period=cycle");
    expect((await axe(baseElement)).violations).toHaveLength(0);
  });

  it("shows the reason and fix of an unavailable figure and no total", async () => {
    const { user, spent } = show();
    await user.click(spent);
    const dialog = screen.getByRole("dialog", { name: "Keluar siklus ini" });
    expect(dialog).toHaveTextContent("Kenapa belum ada: Tanggal gajian belum diatur.");
    expect(dialog).toHaveTextContent("Atur tanggal gajian di Pengaturan supaya siklusnya terhitung.");
    expect(within(dialog).queryByRole("table")).not.toBeInTheDocument();
    expect(dialog).not.toHaveTextContent("Rp 999.999");
  });

  it("closes on Esc and returns focus to the trigger", async () => {
    const { user, safe } = show();
    await user.click(safe);
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(safe).toHaveFocus();
    expect(safe).toHaveAttribute("aria-expanded", "false");
  });

  it("closes with its close button and returns focus to the trigger", async () => {
    const { user, safe } = show();
    await user.click(safe);
    await user.click(screen.getByRole("button", { name: "Tutup penjelasan" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(safe).toHaveFocus();
  });

  it("closes on a press outside and returns focus to the trigger", async () => {
    const { user, safe } = show();
    await user.click(safe);
    fireEvent.pointerDown(screen.getByText("Ringkasan"));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(safe).toHaveFocus();
  });

  it("stays open on a press inside and toggles closed from its trigger", async () => {
    const { user, safe } = show();
    await user.click(safe);
    const dialog = screen.getByRole("dialog");
    fireEvent.pointerDown(within(dialog).getByText(SAFE_TO_SPEND.rule));
    expect(screen.getByRole("dialog")).toBeInTheDocument();
    await user.click(safe);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("keeps only one explanation open at a time", async () => {
    const { user, safe, spent } = show();
    await user.click(safe);
    await user.click(spent);
    expect(screen.getAllByRole("dialog")).toHaveLength(1);
    expect(screen.getByRole("dialog", { name: "Keluar siklus ini" })).toBeInTheDocument();
    expect(safe).toHaveAttribute("aria-expanded", "false");
    expect(spent).toHaveAttribute("aria-expanded", "true");
  });

  it("opens as a modal bottom sheet at phone width with the page locked", async () => {
    stubPhone(true);
    const { user, safe } = show();
    await user.click(safe);
    const dialog = screen.getByRole("dialog", { name: "Aman dipakai hari ini" });
    expect(dialog).toHaveAttribute("data-mode", "sheet");
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(document.body.style.overflow).toBe("hidden");
    expect(within(dialog).getByRole("heading", { name: "Aman dipakai hari ini" })).toHaveFocus();

    await user.tab();
    expect(screen.getByRole("button", { name: "Tutup penjelasan" })).toHaveFocus();
    screen.getByRole("link", { name: "Lihat transaksinya" }).focus();
    await user.tab();
    expect(screen.getByRole("button", { name: "Tutup penjelasan" })).toHaveFocus();

    await user.click(screen.getByRole("button", { name: "Tutup penjelasan" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(document.body.style.overflow).toBe("");
    expect(safe).toHaveFocus();
  });

  it("needs an ExplainProvider", () => {
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    expect(() =>
      render(<ExplainableFigure label="Aman dipakai hari ini" amountText="Rp 214.000" explanation={SAFE_TO_SPEND} />),
    ).toThrow("ExplainableFigure needs an ExplainProvider");
    error.mockRestore();
  });
});
