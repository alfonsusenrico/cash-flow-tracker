"use client";

import { Suspense } from "react";
import { AccountsView } from "@/components/accounts/AccountsView";

export default function AccountsPage() {
  return (
    <Suspense fallback={null}>
      <AccountsView />
    </Suspense>
  );
}
