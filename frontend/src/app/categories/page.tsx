"use client";
import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import type { Category } from "@/types/domain";

const KIND_COLORS: Record<string, "green" | "red" | "blue" | "gray"> = {
  income: "green", expense: "red", transfer: "blue", adjustment: "gray",
};

const KIND_ICONS: Record<string, string> = {
  income: "💵", expense: "🛍️", transfer: "⇄", adjustment: "⚙️",
};

export default function CategoriesPage() {
  const qc = useQueryClient();
  const [search, setSearch] = useState("");
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [editing, setEditing] = useState<Category | null>(null);
  const [form, setForm] = useState({ name: "", kind: "expense", icon: "", color: "" });
  const [err, setErr] = useState("");
  const { data } = useQuery<{ categories: Category[] }>({ queryKey: ["categories"], queryFn: () => api.get("/categories") });

  const invalidate = () => qc.invalidateQueries({ queryKey: ["categories"] });
  const saveMut = useMutation({
    mutationFn: () => {
      const payload = { name: form.name, kind: form.kind, icon: form.icon || null, color: form.color || null, is_archived: false };
      return editing ? api.put(`/categories/${editing.category_id}`, payload) : api.post("/categories", payload);
    },
    onSuccess: () => { invalidate(); setModal(null); setEditing(null); },
    onError: (e: Error) => setErr(e.message),
  });
  const archiveMut = useMutation({
    mutationFn: (categoryId: string) => api.del(`/categories/${categoryId}`),
    onSuccess: () => { invalidate(); setModal(null); setEditing(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const categories = (data?.categories ?? []).filter((c) => !c.is_archived);
  const filtered = search ? categories.filter((c) => c.name.toLowerCase().includes(search.toLowerCase())) : categories;
  const groups = ["income", "expense", "transfer", "adjustment"] as const;
  const groupLabel = (kind: string) => kind === "transfer" ? "Movement" : kind.charAt(0).toUpperCase() + kind.slice(1);

  function openCreate() {
    setEditing(null);
    setForm({ name: "", kind: "expense", icon: "", color: "" });
    setErr("");
    setModal("create");
  }

  function openEdit(category: Category) {
    setEditing(category);
    setForm({ name: category.name, kind: category.kind, icon: category.icon ?? "", color: category.color ?? "" });
    setErr("");
    setModal("edit");
  }

  return (
    <div className="workbench-page space-y-4">
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
          <Button size="sm" variant="primary" onClick={openCreate}>+ Add Category</Button>
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
                <span className="font-semibold text-sm">{groupLabel(kind)}</span>
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
                        <div className="w-6 h-6 rounded-md flex items-center justify-center text-xs bg-[var(--bg)]">
                          {cat.icon || KIND_ICONS[kind]}
                        </div>
                        <span className="font-medium text-[var(--text)]">{cat.name}</span>
                      </div>
                    </td>
                    <td className="py-2 text-[var(--muted)]">{cat.parent_category_id ? "Subcategory" : "Universal or custom category"}</td>
                    <td className="py-2 text-right">
                      <div className="flex items-center justify-end gap-2">
                        <Badge variant={KIND_COLORS[kind]}>{groupLabel(kind)}</Badge>
                        <button type="button" onClick={() => openEdit(cat)} className="text-[var(--muted)] hover:text-[var(--text)]" title="Edit category">✏️</button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        );
      })}

      <p className="text-xs text-[var(--muted)] text-center py-2">
        ⓘ Universal categories are starter categories. Add your own terms or archive ones you do not use.
      </p>

      <Modal open={modal !== null} onClose={() => setModal(null)} title={editing ? "Edit Category" : "Add Category"}>
        <form onSubmit={(e) => { e.preventDefault(); saveMut.mutate(); }} className="space-y-3">
          <Input label="Category Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required placeholder="e.g. Rent, Client Payment" />
          <Select label="Type" value={form.kind} onChange={(e) => setForm({ ...form, kind: e.target.value })}>
            <option value="income">Income</option>
            <option value="expense">Expense</option>
            <option value="transfer">Movement</option>
            <option value="adjustment">Adjustment</option>
          </Select>
          <Input label="Icon (optional)" value={form.icon} onChange={(e) => setForm({ ...form, icon: e.target.value })} placeholder="e.g. 🏠" />
          <Input label="Color (optional)" value={form.color} onChange={(e) => setForm({ ...form, color: e.target.value })} placeholder="e.g. #16a34a" />
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            {editing && (
              <Button type="button" variant="danger" onClick={() => confirm(`Archive "${editing.name}"?`) && archiveMut.mutate(editing.category_id)} disabled={archiveMut.isPending}>
                Archive
              </Button>
            )}
            <Button type="button" variant="secondary" onClick={() => setModal(null)}>Cancel</Button>
            <Button type="submit" variant="primary" className="flex-1" disabled={saveMut.isPending}>
              {saveMut.isPending ? "Saving..." : "Save"}
            </Button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
