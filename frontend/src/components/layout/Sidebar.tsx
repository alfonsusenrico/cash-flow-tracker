"use client";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { cn } from "@/lib/utils";
import { Icon, type IconName } from "@/components/ui/Icon";

const NAV: { href: string; label: string; icon: IconName }[] = [
  { href: "/dashboard", label: "Summary", icon: "dashboard" },
  { href: "/ledger", label: "Transactions", icon: "ledger" },
  { href: "/accounts", label: "Accounts", icon: "accounts" },
  { href: "/obligations", label: "Payables", icon: "obligations" },
  { href: "/buckets", label: "Buckets", icon: "buckets" },
  { href: "/allocation", label: "Allocation", icon: "allocation" },
  { href: "/analysis", label: "Analysis", icon: "analysis" },
  { href: "/strategy", label: "Strategy", icon: "strategy" },
  { href: "/goals", label: "Goals", icon: "goals" },
  { href: "/net-worth", label: "Net Worth", icon: "netWorth" },
  { href: "/categories", label: "Categories", icon: "categories" },
];

const QUICK_ACTIONS: { href: string; label: string; icon: IconName }[] = [
  { href: "/ledger?action=add", label: "Add Transaction", icon: "plus" },
  { href: "/ledger?action=movement", label: "Move Accounts", icon: "move" },
  { href: "/net-worth?action=record", label: "Record Net Worth", icon: "netWorth" },
  { href: "/buckets?action=new", label: "New Bucket", icon: "buckets" },
];

function isActive(pathname: string, href: string) {
  return pathname === href || (href !== "/dashboard" && pathname.startsWith(href));
}

export function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();

  return (
    <>
      <aside className="fixed left-0 top-0 z-50 hidden h-screen w-[var(--sidebar-width)] flex-col border-r border-white/10 bg-[var(--sidebar)] lg:flex">
        <div className="flex items-center gap-3 px-4 py-4">
          <div className="flex h-8 w-8 items-center justify-center rounded-[var(--radius-md)] bg-[var(--primary)] text-sm font-bold tabular text-white">CF</div>
          <div className="min-w-0">
            <span className="block text-sm font-bold leading-tight text-white">Financial Manager</span>
            <span className="block text-[11px] text-[var(--sidebar-text)]">private finance</span>
          </div>
        </div>

        <nav className="flex-1 space-y-0.5 overflow-y-auto px-2 py-2">
          {NAV.map((item) => {
            const active = isActive(pathname, item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                prefetch
                onMouseEnter={() => router.prefetch(item.href)}
                onFocus={() => router.prefetch(item.href)}
                className={cn(
                  "flex items-center gap-3 rounded-[var(--radius-md)] px-3 py-2 text-sm outline-none transition-colors focus-visible:ring-2 focus-visible:ring-[var(--color-focus)]",
                  active
                    ? "bg-[var(--sidebar-active)] font-medium text-white"
                    : "text-[var(--sidebar-text)] hover:bg-[var(--sidebar-hover)] hover:text-white"
                )}
              >
                <Icon name={item.icon} className="h-4 w-4 shrink-0" />
                <span>{item.label}</span>
              </Link>
            );
          })}
        </nav>

        <div className="border-t border-white/10 px-3 py-3">
          <p className="mb-2 px-1 text-[11px] uppercase tracking-[0.12em] text-[var(--sidebar-text)]">Quick Actions</p>
          <div className="space-y-0.5">
            {QUICK_ACTIONS.map((a) => (
              <Link
                key={a.href}
                href={a.href}
                prefetch
                onMouseEnter={() => router.prefetch(a.href)}
                onFocus={() => router.prefetch(a.href)}
                className="flex items-center gap-2.5 rounded-[var(--radius-md)] px-3 py-1.5 text-xs text-[var(--sidebar-text)] outline-none transition-colors hover:bg-[var(--sidebar-hover)] hover:text-white focus-visible:ring-2 focus-visible:ring-[var(--color-focus)]"
              >
                <Icon name={a.icon} className="h-4 w-4 shrink-0" />
                <span>{a.label}</span>
              </Link>
            ))}
          </div>
        </div>

        <div className="border-t border-white/10 px-4 py-3">
          <p className="text-xs text-[var(--sidebar-text)]">© 2026 Alfonsus Enrico</p>
          <p className="text-xs text-[var(--sidebar-text)] opacity-60">Financial Manager</p>
        </div>
      </aside>

      <nav className="fixed bottom-0 left-0 right-0 z-50 grid grid-cols-5 border-t border-[var(--border)] bg-[var(--surface)]/95 px-1 py-1 backdrop-blur lg:hidden">
        {NAV.slice(0, 5).map((item) => {
          const active = isActive(pathname, item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex min-w-0 flex-col items-center gap-0.5 rounded-[var(--radius-md)] px-1 py-1.5 text-[10px] font-medium",
                active ? "bg-[var(--primary-light)] text-[var(--primary)]" : "text-[var(--muted)]"
              )}
            >
              <Icon name={item.icon} className="h-4 w-4" />
              <span className="max-w-full truncate">{item.label}</span>
            </Link>
          );
        })}
      </nav>
    </>
  );
}
