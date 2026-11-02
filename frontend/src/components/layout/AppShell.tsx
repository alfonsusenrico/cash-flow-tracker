"use client";

import { usePathname } from "next/navigation";
import AppLayout from "@/components/layout/AppLayout";

/** Signed-in pages open inside the workspace; the login page stands alone. */
export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  if (pathname.startsWith("/auth")) {
    return <>{children}</>;
  }

  return <AppLayout>{children}</AppLayout>;
}
