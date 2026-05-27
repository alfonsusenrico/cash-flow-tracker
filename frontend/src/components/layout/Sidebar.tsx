"use client";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/dashboard", label: "Summary", icon: "⊞" },
  { href: "/ledger", label: "Transactions", icon: "↕" },
  { href: "/accounts", label: "Accounts", icon: "🏦" },
  { href: "/obligations", label: "Payables", icon: "⇅" },
  { href: "/buckets", label: "Buckets", icon: "🪣" },
  { href: "/allocation", label: "Allocation", icon: "📊" },
  { href: "/analysis", label: "Analysis", icon: "📈" },
  { href: "/strategy", label: "Strategy", icon: "⚡" },
  { href: "/goals", label: "Goals", icon: "🎯" },
  { href: "/net-worth", label: "Net Worth", icon: "💰" },
  { href: "/categories", label: "Categories", icon: "🏷" },
];

const QUICK_ACTIONS = [
  { href: "/ledger?action=add", label: "Add Transaction", icon: "+" },
  { href: "/ledger?action=transfer", label: "Switch", icon: "⇄" },
  { href: "/net-worth?action=record", label: "Record Net Worth", icon: "📌" },
  { href: "/buckets?action=new", label: "New Bucket", icon: "🪣" },
];

export function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();

  return (
    <aside className="fixed left-0 top-0 h-screen w-[200px] flex flex-col z-50" style={{ background: "var(--sidebar)" }}>
      {/* Logo */}
      <div className="px-4 py-5 flex items-center gap-2.5">
        <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center text-white font-bold text-sm">$</div>
        <span className="text-white font-bold text-base">Financial Manager</span>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2 py-2 overflow-y-auto space-y-0.5">
        {NAV.map((item) => {
          const active = pathname === item.href || (item.href !== "/dashboard" && pathname.startsWith(item.href));
          return (
            <Link
              key={item.href}
              href={item.href}
              prefetch
              onMouseEnter={() => router.prefetch(item.href)}
              onFocus={() => router.prefetch(item.href)}
              className={cn(
                "flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors",
                active
                  ? "bg-[var(--sidebar-active)] text-white font-medium"
                  : "text-[var(--sidebar-text)] hover:bg-[var(--sidebar-hover)] hover:text-white"
              )}
            >
              <span className="w-4 text-center text-base leading-none">{item.icon}</span>
              <span>{item.label}</span>
            </Link>
          );
        })}
      </nav>

      {/* Quick Actions */}
      <div className="px-3 py-3 border-t border-white/10">
        <p className="text-xs text-[var(--sidebar-text)] uppercase tracking-wider mb-2 px-1">Quick Actions</p>
        <div className="space-y-0.5">
          {QUICK_ACTIONS.map((a) => (
            <Link
              key={a.href}
              href={a.href}
              prefetch
              onMouseEnter={() => router.prefetch(a.href)}
              onFocus={() => router.prefetch(a.href)}
              className="flex items-center gap-2.5 px-3 py-1.5 rounded-lg text-xs text-[var(--sidebar-text)] hover:bg-[var(--sidebar-hover)] hover:text-white transition-colors"
            >
              <span className="w-4 text-center">{a.icon}</span>
              <span>{a.label}</span>
            </Link>
          ))}
        </div>
      </div>

      {/* Footer */}
      <div className="px-4 py-3 border-t border-white/10">
        <p className="text-xs text-[var(--sidebar-text)]">v2.0.0</p>
        <p className="text-xs text-[var(--sidebar-text)] opacity-60">Self-hosted</p>
      </div>
    </aside>
  );
}
