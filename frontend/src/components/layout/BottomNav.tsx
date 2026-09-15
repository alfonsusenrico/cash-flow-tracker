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
      className="lg:hidden fixed bottom-3 left-3 right-3 z-50 max-w-md mx-auto pointer-events-none"
      style={{ paddingBottom: "env(safe-area-inset-bottom, 0px)" }}
    >
      <nav className="glass-dock rounded-full p-1.5 flex items-center justify-around pointer-events-auto border border-white/[0.08] shadow-2xl">
        {/* 1. Beranda / Pulse */}
        <Link
          href="/"
          className={cn(
            "flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs transition-all",
            isActive("/")
              ? "bg-white/10 text-white font-semibold border border-white/10 shadow-xs"
              : "text-[var(--muted)] hover:text-[var(--text)]"
          )}
        >
          <Icon name="dashboard" className="h-4 w-4 shrink-0" />
          <span className={cn("text-[11px]", !isActive("/") && "hidden sm:inline")}>Beranda</span>
        </Link>

        {/* 2. Transaksi / Ledger */}
        <Link
          href="/ledger"
          className={cn(
            "flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs transition-all",
            isActive("/ledger")
              ? "bg-white/10 text-white font-semibold border border-white/10 shadow-xs"
              : "text-[var(--muted)] hover:text-[var(--text)]"
          )}
        >
          <Icon name="ledger" className="h-4 w-4 shrink-0" />
          <span className={cn("text-[11px]", !isActive("/ledger") && "hidden sm:inline")}>Transaksi</span>
        </Link>

        {/* Center Quick Add Trigger */}
        <button
          type="button"
          onClick={onQuickAdd}
          aria-label="Catat Transaksi Cepat"
          title="Catat Transaksi Cepat"
          className="flex h-10 w-10 items-center justify-center rounded-full bg-emerald-500 hover:bg-emerald-400 text-white shadow-lg active:scale-95 transition-transform shrink-0 cursor-pointer"
        >
          <Icon name="plus" className="h-5 w-5" />
        </button>

        {/* 3. Analisis / Insights */}
        <Link
          href="/insights"
          className={cn(
            "flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs transition-all",
            isActive("/insights")
              ? "bg-white/10 text-white font-semibold border border-white/10 shadow-xs"
              : "text-[var(--muted)] hover:text-[var(--text)]"
          )}
        >
          <Icon name="analysis" className="h-4 w-4 shrink-0" />
          <span className={cn("text-[11px]", !isActive("/insights") && "hidden sm:inline")}>Analisis</span>
        </Link>

        {/* 4. Rekening / Accounts */}
        <Link
          href="/accounts"
          className={cn(
            "flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs transition-all",
            isActive("/accounts")
              ? "bg-white/10 text-white font-semibold border border-white/10 shadow-xs"
              : "text-[var(--muted)] hover:text-[var(--text)]"
          )}
        >
          <Icon name="credit-card" className="h-4 w-4 shrink-0" />
          <span className={cn("text-[11px]", !isActive("/accounts") && "hidden sm:inline")}>Dompet</span>
        </Link>
      </nav>
    </div>
  );
}
