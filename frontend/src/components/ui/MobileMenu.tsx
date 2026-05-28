"use client";
import Link from "next/link";
import { cn } from "@/lib/utils";

interface MobileMenuProps {
  open: boolean;
  onClose: () => void;
}

const ALL_NAV = [
  { href: "/dashboard", label: "Summary", icon: "⊞" },
  { href: "/ledger", label: "Transactions", icon: "↕" },
  { href: "/accounts", label: "Accounts", icon: "⊡" },
  { href: "/buckets", label: "Buckets", icon: "◎" },
  { href: "/allocation", label: "Allocation", icon: "◫" },
  { href: "/analysis", label: "Analysis", icon: "◈" },
  { href: "/strategy", label: "Strategy", icon: "⚡" },
  { href: "/goals", label: "Goals", icon: "◉" },
  { href: "/assets", label: "Assets", icon: "◆" },
  { href: "/net-worth", label: "Net Worth", icon: "◇" },
  { href: "/categories", label: "Categories", icon: "▤" },
  { href: "/periods", label: "Periods", icon: "▥" },
  { href: "/obligations", label: "Obligations", icon: "▦" },
];

export function MobileMenu({ open, onClose }: MobileMenuProps) {
  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[200] md:hidden" onClick={onClose}>
      {/* Dimmed background */}
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />

      {/* Menu panel — slides up from bottom */}
      <div
        className="absolute bottom-0 left-0 right-0 bg-[var(--surface)] rounded-t-2xl max-h-[80vh] overflow-y-auto"
        style={{ paddingBottom: "env(safe-area-inset-bottom, 0px)" }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Handle bar */}
        <div className="flex justify-center pt-3 pb-2">
          <div className="w-10 h-1 rounded-full bg-[var(--border)]" />
        </div>

        <div className="px-4 pb-2">
          <h2 className="text-sm font-semibold text-[var(--muted)] uppercase tracking-wider">Navigation</h2>
        </div>

        <nav className="px-2 pb-4 grid grid-cols-3 gap-1">
          {ALL_NAV.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              onClick={onClose}
              className="flex flex-col items-center gap-1.5 p-3 rounded-xl text-center hover:bg-[var(--bg)] transition-colors"
            >
              <span className="w-10 h-10 rounded-xl bg-[var(--bg)] flex items-center justify-center text-lg text-[var(--text)]">
                {item.icon}
              </span>
              <span className="text-xs font-medium text-[var(--text)] leading-tight">{item.label}</span>
            </Link>
          ))}
        </nav>
      </div>
    </div>
  );
}
