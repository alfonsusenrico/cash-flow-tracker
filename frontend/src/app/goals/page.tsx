"use client";

import { Suspense } from "react";
import { GoalsView } from "@/components/goals/GoalsView";

export default function GoalsAndDebtsPage() {
  return (
    <Suspense fallback={null}>
      <GoalsView />
    </Suspense>
  );
}
