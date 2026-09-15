"use client";

import { usePathname } from "next/navigation";
import AppLayout from "@/components/layout/AppLayout";

const ROUTE_TITLES: Record<string, string> = {
  "/": "Pulse",
  "/insights": "Insights",
  "/accounts": "Accounts",
};

function getTitle(pathname: string) {
  const match = Object.entries(ROUTE_TITLES)
    .sort((a, b) => b[0].length - a[0].length)
    .find(([route]) => pathname === route || pathname.startsWith(`${route}/`));
  return match?.[1] ?? "Cash Flow Tracker";
}

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  if (pathname.startsWith("/auth")) {
    return <>{children}</>;
  }

  return <AppLayout title={getTitle(pathname)}>{children}</AppLayout>;
}
