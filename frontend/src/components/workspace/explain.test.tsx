import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { readFileSync } from "node:fs";
import path from "node:path";
import { useRef, useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ExplainProvider, ExplainableFigure, type ExplanationData } from "./explain";
import { ExplanationDialog, type RincianData } from "./ExplanationDialog";
import { PHONE_QUERY } from "./layers";
import { ShortcutProvider } from "./shortcuts";

const app = vi.hoisted(() => ({ hideBalances: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ hideBalances: app.hideBalances, bal: (amount: number) => (app.hideBalances ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`) }),
}));
vi.mock("next/navigation", () => ({ usePathname: () => "/", useRouter: () => ({ push: vi.fn() }) }));

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
    document.documentElement.style.overflow = "";
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
    expect(document.documentElement.style.overflow).toBe("");
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
    expect(document.documentElement.style.overflow).toBe("hidden");
    expect(within(dialog).getByRole("heading", { name: "Aman dipakai hari ini" })).toHaveFocus();

    await user.tab();
    expect(screen.getByRole("button", { name: "Tutup penjelasan" })).toHaveFocus();
    screen.getByRole("link", { name: "Lihat transaksinya" }).focus();
    await user.tab();
    expect(screen.getByRole("button", { name: "Tutup penjelasan" })).toHaveFocus();

    await user.click(screen.getByRole("button", { name: "Tutup penjelasan" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(document.documentElement.style.overflow).toBe("");
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

const RINCIAN: RincianData = {
  title: "Aman dipakai sampai gajian",
  rule: "Masuk dikurangi Pengeluaran, Ditabung & investasi dan tagihan terjadwal sampai akhir siklus.",
  lines: [
    { key: "masuk", label: "Masuk", amount: "Rp 9.518.400", parts: [{ label: "Gaji", amount: "Rp 9.500.000" }, { label: "Refund", amount: "Rp 18.400" }] },
    {
      key: "spending",
      label: "Pengeluaran",
      amount: "−Rp 3.502.990",
      parts: [
        { label: "Kebutuhan", amount: "Rp 2.341.000", href: "/ledger?period=cycle&kakeibo=need" },
        { label: "Belum diklasifikasi", amount: "Rp 1.161.990", href: "/ledger?period=cycle&kakeibo=unclassified" },
      ],
    },
    { key: "saving", label: "Ditabung & investasi", amount: "−Rp 2.750.000" },
    { key: "scheduled", label: "Terjadwal sampai 24 Nov", amount: "−Rp 989.000", parts: [{ label: "Tagihan: SPayLater", note: "jatuh tempo 5 Nov", amount: "−Rp 434.000" }], partNotes: ["Sampai sehari sebelum perkiraan gaji berikutnya (24 Nov)."] },
  ],
  total: { label: "Aman dipakai", amount: "Rp 2.276.410" },
  notes: ["4 hal (Rp 190.500) masih menunggu jawaban; Rp 45.000 di antaranya sudah termasuk."],
  waiting: {
    label: "Menunggu jawaban",
    amount: "Rp 190.500",
    lines: [{ label: "TRF BIFAST", note: "Belum dikenali · sudah dihitung di Pengeluaran sebagai Belum diklasifikasi", amount: "Rp 120.000" }],
    link: { href: "/review", label: "Jawab di Tinjau" },
  },
  link: { href: "/ledger?period=cycle", label: "Lihat transaksi siklus ini" },
};

function RincianHarness() {
  const [open, setOpen] = useState(false);
  const opener = useRef<HTMLButtonElement>(null);
  return (
    <ShortcutProvider>
      <main>
        <button ref={opener} type="button" aria-haspopup="dialog" aria-expanded={open} aria-label="Rincian aman dipakai sampai gajian" onClick={() => setOpen(true)}>
          Rincian
        </button>
        <a href="/ledger">Semua transaksi</a>
      </main>
      <ExplanationDialog
        open={open}
        onClose={() => {
          setOpen(false);
          opener.current?.focus();
        }}
        data={RINCIAN}
      />
    </ShortcutProvider>
  );
}

describe("ExplanationDialog (Rincian)", () => {
  beforeEach(() => {
    app.hideBalances = false;
  });

  async function openRincian() {
    const user = userEvent.setup();
    render(<RincianHarness />);
    const opener = screen.getByRole("button", { name: "Rincian aman dipakai sampai gajian" });
    await user.click(opener);
    const dialog = await screen.findByRole("dialog", { name: "Aman dipakai sampai gajian" });
    return { user, opener, dialog };
  }
  const lines = (dialog: HTMLElement) => [...dialog.querySelectorAll(".list > li > .row")].map((row) => row.textContent);

  it("opens a 480 px dialog with the meta, the rule, four lines, the total and the footer link, with focus inside", async () => {
    const { opener, dialog } = await openRincian();
    expect(opener).toHaveAttribute("aria-expanded", "true");
    expect(dialog).toHaveClass("shell-dialog", "rincian");
    expect(dialog.style.getPropertyValue("--dw")).toBe("480px");
    expect(dialog).toHaveTextContent("Rincian hitungan");
    expect(within(dialog).getByText(RINCIAN.rule)).toHaveClass("t-body");
    expect(lines(dialog)).toEqual([
      "MasukRp 9.518.400", "Pengeluaran−Rp 3.502.990", "Ditabung & investasi−Rp 2.750.000", "Terjadwal sampai 24 Nov−Rp 989.000",
      "Aman dipakaiRp 2.276.410", "Menunggu jawaban · Rp 190.500",
    ]);
    expect(dialog.querySelector(".is-total .t-fig")).toHaveTextContent("Rp 2.276.410");
    expect(within(dialog).getByRole("link", { name: "Lihat transaksi siklus ini" })).toHaveAttribute("href", "/ledger?period=cycle");
    expect(dialog.querySelector(".dlg-f")).toContainElement(within(dialog).getByRole("link", { name: "Lihat transaksi siklus ini" }));
    expect(dialog).toContainElement(document.activeElement as HTMLElement);
  });

  it("ends an over period in what it is over by, then the result Rp 0, never a negative Aman dipakai", () => {
    const over: RincianData = { ...RINCIAN, over: { label: "Lebih dari uang masuk", amount: "Rp 1.726.424" }, total: { label: "Aman dipakai", amount: "Rp 0" } };
    render(<ExplanationDialog open onClose={() => {}} data={over} />);
    const dialog = screen.getByRole("dialog", { name: "Aman dipakai sampai gajian" });
    expect(lines(dialog).slice(4, 6)).toEqual(["Lebih dari uang masukRp 1.726.424", "Aman dipakaiRp 0"]);
    expect(dialog.querySelector(".is-over .t-amt")).toHaveTextContent("Rp 1.726.424");
    expect(dialog.querySelector(".is-total")).toHaveClass("after-over");
    expect(dialog.querySelector(".is-total .t-fig")).toHaveTextContent("Rp 0");
  });

  it("is centered from 768 px and a bottom sheet below, by the shared dialog rules", () => {
    const css = readFileSync(path.join(__dirname, "../../app/globals.css"), "utf8");
    expect(css).toMatch(/dialog\.shell-dialog \{[^}]*width: min\(var\(--dw, 560px\), calc\(100vw - 48px\)\);[^}]*margin: auto;/);
    const phone = css.slice(css.indexOf("@media (max-width: 767px)"));
    expect(phone).toMatch(/dialog\.shell-dialog \{ width: 100%; max-height: 92vh; margin: auto 0 0; border-bottom: 0; border-radius: var\(--r-layer\) var\(--r-layer\) 0 0; \}/);
  });

  it("traps Tab inside and returns focus to Rincian on Esc and on the close button", async () => {
    const { user, opener, dialog } = await openRincian();
    for (let step = 0; step < 12; step += 1) {
      await user.tab();
      expect(dialog).toContainElement(document.activeElement as HTMLElement);
    }
    await user.keyboard("{Escape}");
    await waitForClosed();
    expect(opener).toHaveFocus();
    expect(opener).toHaveAttribute("aria-expanded", "false");

    await user.click(opener);
    const again = await screen.findByRole("dialog", { name: "Aman dipakai sampai gajian" });
    await user.click(within(again).getByRole("button", { name: "Tutup rincian" }));
    await waitForClosed();
    expect(opener).toHaveFocus();
  });

  async function waitForClosed() {
    await vi.waitFor(() => expect(screen.queryByRole("dialog", { name: "Aman dipakai sampai gajian" })).not.toBeInTheDocument());
  }

  it("opens a line in place with Enter and Space and shows exactly the lines it is given", async () => {
    const { user, dialog } = await openRincian();
    const spending = within(dialog).getByRole("button", { name: /^Pengeluaran/ });
    expect(spending).toHaveAttribute("aria-expanded", "false");
    spending.focus();
    await user.keyboard("{Enter}");
    expect(spending).toHaveAttribute("aria-expanded", "true");
    const parts = document.getElementById(spending.getAttribute("aria-controls")!)!;
    expect([...parts.querySelectorAll(".part")].map((part) => part.textContent)).toEqual(["KebutuhanRp 2.341.000", "Belum diklasifikasiRp 1.161.990"]);
    expect(within(parts).getByRole("link", { name: "Kebutuhan" })).toHaveAttribute("href", "/ledger?period=cycle&kakeibo=need");
    await user.keyboard(" ");
    expect(spending).toHaveAttribute("aria-expanded", "false");
    expect(within(dialog).queryByRole("link", { name: "Kebutuhan" })).not.toBeInTheDocument();
    // A line without parts is plain text, not a disclosure.
    expect(within(dialog).queryByRole("button", { name: /^Ditabung/ })).not.toBeInTheDocument();

    const scheduled = within(dialog).getByRole("button", { name: /^Terjadwal sampai 24 Nov/ });
    await user.click(scheduled);
    expect(dialog).toHaveTextContent("Tagihan: SPayLaterjatuh tempo 5 Nov−Rp 434.000");
    expect(dialog).toHaveTextContent("Sampai sehari sebelum perkiraan gaji berikutnya (24 Nov).");

    const waiting = within(dialog).getByRole("button", { name: /^Menunggu jawaban/ });
    await user.click(waiting);
    expect(dialog).toHaveTextContent("TRF BIFASTBelum dikenali · sudah dihitung di Pengeluaran sebagai Belum diklasifikasiRp 120.000");
    expect(within(dialog).getByRole("link", { name: "Jawab di Tinjau" })).toHaveAttribute("href", "/review");
  });

  it("masks every amount, parts included, with hidden balances", async () => {
    app.hideBalances = true;
    const { user, dialog } = await openRincian();
    await user.click(within(dialog).getByRole("button", { name: /^Pengeluaran/ }));
    const masks = within(dialog).getAllByRole("img", { name: "Disembunyikan" });
    expect(masks.length).toBe(4 + 1 + 1 + 2);
    // Only the window's date keeps its digits.
    const amounts = [...dialog.querySelectorAll(".list")].map((list) => list.textContent).join(" ").replace("sampai 24 Nov", "");
    expect(amounts).not.toMatch(/\d/);
  });

  it.each([
    ["light", false],
    ["dark", true],
  ])("has no axe violations when open, with a line open, in the %s theme", async (_theme, dark) => {
    if (dark) document.documentElement.classList.add("dark");
    try {
      const { user, dialog } = await openRincian();
      await user.click(within(dialog).getByRole("button", { name: /^Masuk/ }));
      expect((await axe(document.body)).violations).toHaveLength(0);
    } finally {
      document.documentElement.classList.remove("dark");
    }
  });
});
