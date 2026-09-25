"use client";

import { usePathname } from "next/navigation";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";

interface TopBarProps {
  onToggleMobileMenu?: () => void;
  onQuickAdd?: () => void;
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
    <header className="sticky top-0 z-30 flex min-h-14 w-full flex-wrap items-center justify-between gap-y-1.5 border-b border-[var(--border)] bg-[var(--surface)]/90 px-3 py-1.5 backdrop-blur-md sm:h-16 sm:flex-nowrap sm:gap-y-0 sm:px-6 sm:py-0 lg:px-8">
      {/* Left: Brand / Title */}
      <div className="order-1 flex min-w-[9rem] flex-1 items-center gap-2.5 sm:flex-none">
        {onToggleMobileMenu && <button type="button" onClick={onToggleMobileMenu} aria-label="Buka menu" title="Buka menu" className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-xl border border-[var(--border)] text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] lg:hidden"><Icon name="menu" className="h-4 w-4" /></button>}
        <div className="flex flex-col">
          <div className="flex items-center gap-2">
            <span className="text-sm sm:text-base font-bold tracking-tight text-[var(--text)] leading-tight">
              {currentMeta.title}
            </span>
          </div>
          <span className="hidden sm:block text-[11px] text-[var(--muted)] font-medium mt-0.5">
            {currentMeta.subtitle}
          </span>
        </div>
      </div>

      {/* Center: Month & Year Statement Stepper */}
      <div className="order-3 flex min-h-10 basis-full items-center justify-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/60 px-2 text-xs sm:order-2 sm:min-h-0 sm:basis-auto sm:py-1">
        <button
          type="button"
          onClick={() => setCycleOffset(cycleOffset - 1)}
          aria-label="Bulan sebelumnya"
          title="Bulan Sebelumnya"
          className="inline-flex min-h-10 min-w-10 items-center justify-center rounded-lg text-[var(--muted)] hover:bg-[var(--surface)] hover:text-[var(--text)] transition-colors sm:min-h-6 sm:min-w-6"
        >
          <Icon name="chevron-left" className="h-3.5 w-3.5" />
        </button>

        <span className="text-xs font-bold text-[var(--text)] px-2 select-none min-w-[76px] text-center">
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
          aria-label="Bulan berikutnya"
          title="Bulan Berikutnya"
          className="inline-flex min-h-10 min-w-10 items-center justify-center rounded-lg text-[var(--muted)] hover:bg-[var(--surface)] hover:text-[var(--text)] transition-colors disabled:opacity-30 disabled:hover:bg-transparent sm:min-h-6 sm:min-w-6"
        >
          <Icon name="chevron-right" className="h-3.5 w-3.5" />
        </button>

        {cycleOffset !== 0 && (
          <button
            type="button"
            onClick={() => setCycleOffset(0)}
            aria-label="Kembali ke bulan ini"
            title="Kembali ke bulan ini"
            className="ml-1 text-[11px] text-income hover:underline font-semibold"
          >
            Bulan Ini
          </button>
        )}
      </div>

      {/* Right: Actions */}
      <div className="order-2 ml-auto flex shrink-0 items-center gap-1 sm:order-3 sm:gap-3">
        {/* Balance Privacy Eye */}
        <button
          type="button"
          onClick={toggleHideBalances}
          aria-label={hideBalances ? "Tampilkan saldo" : "Sembunyikan saldo"}
          title={hideBalances ? "Tampilkan saldo" : "Sembunyikan saldo"}
          className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-xl border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)] hover:text-[var(--text)] transition-colors shadow-2xs"
        >
          <Icon name={hideBalances ? "eye-off" : "eye"} className="h-4 w-4" />
        </button>

        {/* Theme Toggle */}
        <button
          type="button"
          onClick={toggleTheme}
          aria-label={theme === "dark" ? "Aktifkan mode terang" : "Aktifkan mode gelap"}
          title={theme === "dark" ? "Mode Gelap (Klik untuk Mode Terang)" : "Mode Terang (Klik untuk Mode Gelap)"}
          className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-xl border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)] hover:text-[var(--text)] transition-colors shadow-2xs"
        >
          <Icon name={theme === "dark" ? "moon" : "sun"} className="h-4 w-4" />
        </button>

        {/* Quick Add CTA - Desktop only to avoid mobile duplication */}
        {onQuickAdd && (
          <button
            type="button"
            onClick={onQuickAdd}
            title="Catat Transaksi (N)"
            className="hidden md:flex items-center gap-1.5 px-3.5 py-1.5 rounded-btn bg-primary hover:bg-primary-hover text-primary-contrast text-xs font-semibold shadow-2xs transition-[background-color,transform] active:scale-95"
          >
            <Icon name="plus" className="h-3.5 w-3.5" />
            <span>Catat</span>
            <kbd className="rounded bg-black/20 px-1 py-0.2 text-[9px] font-mono text-white/90">
              N
            </kbd>
          </button>
        )}
      </div>
    </header>
  );
}
