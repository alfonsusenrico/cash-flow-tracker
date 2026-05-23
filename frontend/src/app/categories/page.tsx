"use client";

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { Category } from "@/types/domain";
import AppLayout from "@/components/layout/AppLayout";

export default function CategoriesPage() {
  const { data, isLoading } = useQuery<{ categories: Category[] }>({
    queryKey: ["categories"],
    queryFn: () => api.get("/categories"),
  });

  const groups = ["income", "expense", "transfer", "adjustment"] as const;

  return (
    <AppLayout>
      <div className="p-6 space-y-6">
        <h1 className="text-2xl font-bold">Categories</h1>
        {isLoading && <p className="text-gray-500">Loading…</p>}
        {groups.map((kind) => {
          const items = data?.categories.filter((c) => c.kind === kind && !c.is_archived) ?? [];
          if (!items.length) return null;
          return (
            <section key={kind}>
              <h2 className="text-sm font-semibold uppercase tracking-wide text-gray-500 mb-2 capitalize">
                {kind}
              </h2>
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                {items.map((cat) => (
                  <div
                    key={cat.category_id}
                    className="bg-white dark:bg-gray-800 rounded-lg px-3 py-2 shadow-sm text-sm font-medium"
                  >
                    {cat.name}
                  </div>
                ))}
              </div>
            </section>
          );
        })}
      </div>
    </AppLayout>
  );
}
