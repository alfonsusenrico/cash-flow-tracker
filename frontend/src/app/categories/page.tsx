"use client";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import type { Category } from "@/types/domain";

const KIND_COLORS: Record<string, "green" | "red" | "blue" | "gray"> = {
  income: "green", expense: "red", transfer: "blue", adjustment: "gray",
};

const KIND_ICONS: Record<string, string> = {
  income: "💵", expense: "🛍️", transfer: "⇄", adjustment: "⚙️",
};

export default function CategoriesPage() {
  const [search, setSearch] = useState("");
  const { data } = useQuery<{ categories: Category[] }>({ queryKey: ["categories"], queryFn: () => api.get("/categories") });

  const categories = (data?.categories ?? []).filter((c) => !c.is_archived);
  const filtered = search ? categories.filter((c) => c.name.toLowerCase().includes(search.toLowerCase())) : categories;
  const groups = ["income", "expense", "transfer", "adjustment"] as const;

  return (
    <div className="p-5 space-y-4">
      <div className="flex items-center justify-between">
        <div className="relative flex-1 max-w-sm">
          <input placeholder="Search categories" value={search} onChange={(e) => setSearch(e.target.value)}
            className="w-full border border-[var(--border)] rounded-lg px-3 py-2 text-sm bg-[var(--surface)] pl-8" />
          <span className="absolute left-2.5 top-1/2 -translate-y-1/2 text-[var(--muted)] text-xs">🔍</span>
        </div>
        <div className="flex items-center gap-2 text-xs text-[var(--muted)]">
          <span>ⓘ</span>
          <span>Total Categories</span>
          <span className="font-bold text-[var(--text)]">{categories.length}</span>
        </div>
      </div>

      {groups.map((kind) => {
        const items = filtered.filter((c) => c.kind === kind);
        if (!items.length) return null;
        return (
          <Card key={kind} padding="sm">
            <div className="flex items-center justify-between mb-3 pb-2 border-b border-[var(--border)]">
              <div className="flex items-center gap-2">
                <span className="text-base">{KIND_ICONS[kind]}</span>
                <span className="font-semibold capitalize text-sm">{kind}</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-xs text-[var(--muted)]">{items.length} categories</span>
                <button type="button" disabled title="Collapsible category groups are coming soon" className="text-[var(--muted)] text-xs cursor-not-allowed">▲</button>
              </div>
            </div>
            <table className="w-full text-xs">
              <thead>
                <tr className="text-[var(--muted)]">
                  <th className="text-left pb-2 font-medium">Category</th>
                  <th className="text-left pb-2 font-medium">Description</th>
                  <th className="text-right pb-2 font-medium">Type</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border)]">
                {items.map((cat) => (
                  <tr key={cat.category_id} className="hover:bg-[var(--bg)] transition-colors">
                    <td className="py-2">
                      <div className="flex items-center gap-2">
                        <div className={`w-6 h-6 rounded-md flex items-center justify-center text-xs bg-${KIND_COLORS[kind] === "green" ? "green" : KIND_COLORS[kind] === "red" ? "red" : "blue"}-100`}>
                          {KIND_ICONS[kind]}
                        </div>
                        <span className="font-medium text-[var(--text)]">{cat.name}</span>
                      </div>
                    </td>
                    <td className="py-2 text-[var(--muted)]">—</td>
                    <td className="py-2 text-right">
                      <Badge variant={KIND_COLORS[kind]}>{kind.charAt(0).toUpperCase() + kind.slice(1)}</Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        );
      })}

      <p className="text-xs text-[var(--muted)] text-center py-2">
        ⓘ Categories are system-wide and read-only to maintain data consistency.
      </p>
    </div>
  );
}
