"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";

interface BottomNavProps {
  onQuickAdd?: () => void;
}

export function BottomNav({ onQuickAdd }: BottomNavProps) {
  const pathname = usePathname();

  const isActive = (href: string) => {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  };

  return (
    <div
      className="lg:hidden fixed bottom-2.5 left-3 right-3 z-50 max-w-sm mx-auto pointer-events-none"
      style={{ paddingBottom: "env(safe-area-inset-bottom, 0px)" }}
    >
      <nav className="glass-dock rounded-full p-1.5 flex items-center justify-between pointer-events-auto border border-[var(--border)]/60 bg-[var(--surface)]/95 backdrop-blur-xl shadow-2xl">
        {/* 1. Beranda / Pulse */}
        <Link
          href="/"
          className={cn(
            "flex flex-col sm:flex-row items-center justify-center gap-1 px-3 py-1.5 rounded-full text-xs transition-all",
            isActive("/")
              ? "bg-[var(--accent-lime,#66CC55)] text-[#141814] font-bold shadow-xs"
              : "text-[var(--muted)] hover:text-[var(--text)]"
          )}
        >
          <Icon name="dashboard" className="h-4 w-4 shrink-0" />
          <span className="text-[10px] sm:text-xs tracking-tight">Beranda</span>
        </Link>

        {/* 2. Transaksi / Ledger */}
        <Link
          href="/ledger"
          className={cn(
            "flex flex-col sm:flex-row items-center justify-center gap-1 px-3 py-1.5 rounded-full text-xs transition-all",
            isActive("/ledger")
              ? "bg-[var(--accent-lime,#66CC55)] text-[#141814] font-bold shadow-xs"
              : "text-[var(--muted)] hover:text-[var(--text)]"
          )}
        >
          <Icon name="ledger" className="h-4 w-4 shrink-0" />
          <span className="text-[10px] sm:text-xs tracking-tight">Transaksi</span>
        </Link>

        {/* Center Quick Add Trigger */}
        <button
          type="button"
          onClick={onQuickAdd}
          aria-label="Catat Transaksi Cepat"
          title="Catat Transaksi Cepat"
          className="flex h-11 w-11 items-center justify-center rounded-full bg-[#1E201E] text-white border border-white/10 shadow-lg active:scale-95 transition-transform shrink-0 cursor-pointer -my-1"
        >
          <Icon name="plus" className="h-5 w-5 text-[var(--accent-lime,#66CC55)]" />
        </button>

        {/* 3. Analisis / Insights */}
        <Link
          href="/insights"
          className={cn(
            "flex flex-col sm:flex-row items-center justify-center gap-1 px-3 py-1.5 rounded-full text-xs transition-all",
            isActive("/insights")
              ? "bg-[var(--accent-lime,#66CC55)] text-[#141814] font-bold shadow-xs"
              : "text-[var(--muted)] hover:text-[var(--text)]"
          )}
        >
          <Icon name="analysis" className="h-4 w-4 shrink-0" />
          <span className="text-[10px] sm:text-xs tracking-tight">Analisis</span>
        </Link>

        {/* 4. Rekening / Accounts */}
        <Link
          href="/accounts"
          className={cn(
            "flex flex-col sm:flex-row items-center justify-center gap-1 px-3 py-1.5 rounded-full text-xs transition-all",
            isActive("/accounts")
              ? "bg-[var(--accent-lime,#66CC55)] text-[#141814] font-bold shadow-xs"
              : "text-[var(--muted)] hover:text-[var(--text)]"
          )}
        >
          <Icon name="credit-card" className="h-4 w-4 shrink-0" />
          <span className="text-[10px] sm:text-xs tracking-tight">Dompet</span>
        </Link>
      </nav>
    </div>
  );
}
