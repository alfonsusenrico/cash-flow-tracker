"use client";

import { usePathname } from "next/navigation";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";

interface TopBarProps {
  onToggleMobileMenu: () => void;
  onQuickAdd?: () => void;
  onOpenSettings?: () => void;
}

const TITLES: Record<string, { title: string; subtitle: string }> = {
  "/": { title: "Beranda", subtitle: "Pantauan Keuangan & Arus Kas Harian" },
  "/ledger": { title: "Transaksi", subtitle: "Riwayat & Catatan Keuangan Lengkap" },
  "/insights": { title: "Analisis", subtitle: "Laporan Pengeluaran & Anggaran" },
  "/accounts": { title: "Rekening & Saldo", subtitle: "Daftar Rekening, Dompet Digital & Tunai" },
  "/goals": { title: "Target & Tagihan", subtitle: "Target Tabungan & Rencana Pembayaran" },
};

export function TopBar({ onToggleMobileMenu, onQuickAdd }: TopBarProps) {
  const pathname = usePathname();
  const {
    hideBalances,
    setHideBalances,
    theme,
    setTheme,
    cycleOffset,
    setCycleOffset,
  } = useAppCtx();

  const currentMeta = TITLES[pathname] || {
    title: "Beranda",
    subtitle: "Catatan Keuangan Pribadi",
  };

  function toggleTheme() {
    const next = theme === "dark" ? "light" : "dark";
    setTheme(next);
    document.documentElement.classList.toggle("dark", next === "dark");
    localStorage.setItem("theme", next);
  }

  function toggleHideBalances() {
    const next = !hideBalances;
    setHideBalances(next);
    localStorage.setItem("hideBalances", next ? "1" : "0");
  }

  return (
    <header className="sticky top-0 z-30 flex h-14 w-full items-center justify-between border-b border-[var(--border-structural)] bg-[var(--canvas-app)]/95 px-4 sm:px-6 backdrop-blur-md select-none">
      {/* Left: Mobile Hamburger + Breadcrumb/Title */}
      <div className="flex items-center gap-3">
        <button
          type="button"
          onClick={onToggleMobileMenu}
          className="flex lg:hidden p-1.5 rounded-xl text-[var(--text-muted)] hover:bg-[var(--canvas-subtle)] hover:text-[var(--text-primary)] transition-colors cursor-pointer"
          title="Buka Menu"
        >
          <Icon name="menu" className="h-5 w-5" />
        </button>

        <div className="flex flex-col">
          <div className="flex items-center gap-2">
            <span className="text-sm sm:text-base font-bold tracking-tight text-[var(--text-primary)] leading-none">
              {currentMeta.title}
            </span>
          </div>
          <span className="hidden sm:block text-[11px] text-[var(--text-muted)] font-medium mt-0.5">
            {currentMeta.subtitle}
          </span>
        </div>
      </div>

      {/* Center: Month & Year Statement Stepper */}
      <div className="flex items-center gap-1 rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-card)] px-1.5 py-1 text-xs shadow-xs">
        <button
          type="button"
          onClick={() => setCycleOffset(cycleOffset - 1)}
          title="Bulan Sebelumnya"
          className="p-1 rounded-lg text-[var(--text-muted)] hover:bg-[var(--canvas-subtle)] hover:text-[var(--text-primary)] transition-colors cursor-pointer"
        >
          <Icon name="chevron-left" className="h-3.5 w-3.5" />
        </button>

        <span className="text-xs font-bold text-[var(--text-primary)] px-2 select-none min-w-[76px] text-center tabular">
          {(() => {
            const d = new Date();
            d.setDate(1);
            d.setMonth(d.getMonth() + cycleOffset);
            return d.toLocaleDateString("id-ID", { month: "short", year: "numeric" });
          })()}
        </span>

        <button
          type="button"
          disabled={cycleOffset >= 0}
          onClick={() => setCycleOffset(cycleOffset + 1)}
          title="Bulan Berikutnya"
          className="p-1 rounded-lg text-[var(--text-muted)] hover:bg-[var(--canvas-subtle)] hover:text-[var(--text-primary)] transition-colors disabled:opacity-30 disabled:hover:bg-transparent cursor-pointer"
        >
          <Icon name="chevron-right" className="h-3.5 w-3.5" />
        </button>

        {cycleOffset !== 0 && (
          <button
            type="button"
            onClick={() => setCycleOffset(0)}
            title="Kembali ke bulan ini"
            className="ml-1 text-[10px] text-[#1E7E34] dark:text-[#66CC55] hover:underline font-bold px-1.5 py-0.5 rounded bg-[#66CC55]/10 cursor-pointer"
          >
            Bulan Ini
          </button>
        )}
      </div>

      {/* Right: Actions */}
      <div className="flex items-center gap-2">
        {/* Balance Privacy Eye */}
        <button
          type="button"
          onClick={toggleHideBalances}
          title={hideBalances ? "Tampilkan saldo" : "Sembunyikan saldo"}
          className="p-2 rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-card)] text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--canvas-subtle)] transition-colors shadow-xs cursor-pointer"
        >
          <Icon name={hideBalances ? "eye-off" : "eye"} className="h-4 w-4" />
        </button>

        {/* Theme Toggle */}
        <button
          type="button"
          onClick={toggleTheme}
          title={theme === "dark" ? "Ganti ke mode terang" : "Ganti ke mode gelap"}
          className="p-2 rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-card)] text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--canvas-subtle)] transition-colors shadow-xs cursor-pointer"
        >
          <Icon name={theme === "dark" ? "sun" : "moon"} className="h-4 w-4" />
        </button>

        {/* Quick Add CTA Button (Matte Charcoal from Ref 1/2) */}
        {onQuickAdd && (
          <button
            type="button"
            onClick={onQuickAdd}
            title="Catat Transaksi (N)"
            className="btn-charcoal px-3 py-1.5 text-xs font-semibold rounded-xl"
          >
            <Icon name="plus" className="h-3.5 w-3.5 stroke-[2.5]" />
            <span className="hidden sm:inline">Catat</span>
            <kbd className="hidden md:inline rounded bg-white/20 px-1 py-0.2 text-[9px] font-mono text-white/90">
              N
            </kbd>
          </button>
        )}
      </div>
    </header>
  );
}
