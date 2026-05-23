"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/dashboard", label: "Summary" },
  { href: "/ledger", label: "Transactions" },
  { href: "/analysis", label: "Analysis" },
  { href: "/categories", label: "Categories" },
  { href: "/periods", label: "Periods" },
];

export default function AppLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();

  async function handleLogout() {
    await api.post("/auth/logout");
    router.push("/auth/login");
  }

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b bg-white dark:bg-gray-900 px-4 py-3 flex items-center justify-between">
        <span className="font-bold text-lg">Cash Flow Tracker</span>
        <nav className="hidden md:flex gap-1">
          {NAV.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "px-3 py-1.5 rounded text-sm font-medium transition-colors",
                pathname.startsWith(item.href)
                  ? "bg-brand-600 text-white"
                  : "text-gray-600 hover:bg-gray-100 dark:text-gray-300 dark:hover:bg-gray-800",
              )}
            >
              {item.label}
            </Link>
          ))}
        </nav>
        <button
          onClick={handleLogout}
          className="text-sm text-gray-500 hover:text-red-500 transition-colors"
        >
          Logout
        </button>
      </header>
      <main className="flex-1 bg-gray-50 dark:bg-gray-950">{children}</main>
    </div>
  );
}
