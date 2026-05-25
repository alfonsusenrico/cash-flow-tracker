"use client";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { Category } from "@/types/domain";

export default function CategoriesPage() {
  const { data, isLoading } = useQuery<{ categories: Category[] }>({
    queryKey: ["categories"],
    queryFn: () => api.get("/categories"),
  });
  const groups = ["income", "expense", "transfer", "adjustment"] as const;

  return (
    <div className="p-4 space-y-5 max-w-2xl mx-auto">
      <h1 className="text-xl font-bold">Categories</h1>
      {isLoading && <p className="text-[var(--muted)]">Loading…</p>}
      {groups.map((kind) => {
        const items = data?.categories.filter((c) => c.kind === kind && !c.is_archived) ?? [];
        if (!items.length) return null;
        return (
          <section key={kind}>
            <h2 className="text-xs font-semibold text-[var(--muted)] uppercase tracking-wide mb-2 capitalize">{kind}</h2>
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
              {items.map((cat) => (
                <div key={cat.category_id} className="bg-[var(--surface)] border border-[var(--border)] rounded-lg px-3 py-2 text-sm font-medium">
                  {cat.name}
                </div>
              ))}
            </div>
          </section>
        );
      })}
    </div>
  );
}
