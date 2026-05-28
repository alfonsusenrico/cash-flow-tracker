"use client";
import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { MobileMenu } from "@/components/ui/MobileMenu";

const TABS = [
  { href: "/dashboard", label: "Home", icon: "⊞" },
  { href: "/ledger", label: "Txn", icon: "↕" },
  { href: "/goals", label: "Goals", icon: "◉" },
  { href: "/analysis", label: "Analysis", icon: "◈" },
  { href: null, label: "More", icon: "≡" },
];

export function BottomNav() {
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);

  return (
    <>
      <nav
        className="md:hidden fixed bottom-0 left-0 right-0 z-50 border-t bg-[var(--surface)] border-[var(--border)]"
        style={{ paddingBottom: "env(safe-area-inset-bottom, 0px)" }}
      >
        <div className="flex items-center justify-around h-14">
          {TABS.map((tab) => {
            if (tab.href === null) {
              return (
                <button
                  key="more"
                  onClick={() => setMenuOpen(true)}
                  className={cn(
                    "flex flex-col items-center justify-center gap-0.5 flex-1 h-full transition-colors",
                    menuOpen ? "text-primary" : "text-[var(--muted)]"
                  )}
                >
                  <span className="text-lg leading-none">{tab.icon}</span>
                  <span className="text-[10px] font-medium leading-tight">{tab.label}</span>
                </button>
              );
            }
            const active = pathname === tab.href || (tab.href !== "/dashboard" && pathname.startsWith(tab.href));
            return (
              <Link
                key={tab.href}
                href={tab.href}
                className={cn(
                  "flex flex-col items-center justify-center gap-0.5 flex-1 h-full transition-colors",
                  active ? "text-primary" : "text-[var(--muted)]"
                )}
              >
                <span className="text-lg leading-none">{tab.icon}</span>
                <span className="text-[10px] font-medium leading-tight">{tab.label}</span>
              </Link>
            );
          })}
        </div>
      </nav>

      <MobileMenu open={menuOpen} onClose={() => setMenuOpen(false)} />
    </>
  );
}
