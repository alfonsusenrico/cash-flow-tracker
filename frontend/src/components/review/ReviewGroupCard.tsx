"use client";

import Link from "next/link";
import { useId, useState, type FormEvent } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { KAKEIBO_PILLARS, kakeiboLabel } from "@/lib/kakeibo";
import { MERCHANT_MAX_LENGTH } from "@/lib/provenance";
import {
  dateRangeLabel,
  groupKindLabel,
  reviewReasonLabel,
  type ReviewAction,
  type ReviewGroup,
} from "@/lib/review";

export interface CategoryOption {
  id: string;
  name: string;
  kind: string;
  kakeibo_type?: string | null;
  is_archived?: boolean;
}

export interface GroupActionRequest {
  action: ReviewAction;
  ids: string[];
  fields?: Record<string, string | null>;
  label: string;
}

interface ReviewGroupCardProps {
  group: ReviewGroup;
  categories: CategoryOption[];
  pending: boolean;
  onAction: (request: GroupActionRequest) => Promise<boolean>;
}

type Panel = "none" | "category" | "rename";

const FIELD = "min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]";
const SECONDARY = "min-h-11 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 text-xs font-semibold text-[var(--text)] transition-colors hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50";
const PRIMARY = "min-h-11 rounded-xl bg-[#1E201E] px-4 text-xs font-bold text-white transition-transform active:scale-95 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50 dark:border dark:border-white/15";

export function ReviewGroupCard({ group, categories, pending, onAction }: ReviewGroupCardProps) {
  const { bal } = useAppCtx();
  const baseId = useId();
  const [panel, setPanel] = useState<Panel>("none");
  const [categoryId, setCategoryId] = useState(group.category?.id ?? "");
  const [pillar, setPillar] = useState("");
  const [merchant, setMerchant] = useState(group.kind === "merchant" ? group.label : "");
  const [notes, setNotes] = useState("");
  const [formError, setFormError] = useState("");
  const ids = group.transactions.map((item) => item.id);
  const range = dateRangeLabel(group.first_date, group.last_date);
  const choices = categories.filter((category) => category.kind === group.type && !category.is_archived);
  const headingId = `${baseId}-heading`;

  const togglePanel = (next: Panel) => {
    setFormError("");
    setPanel((current) => (current === next ? "none" : next));
  };

  const submitCategory = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!categoryId) {
      setFormError("Pilih kategori.");
      return;
    }
    const done = await onAction({
      action: "recategorise",
      ids,
      fields: { category_id: categoryId, ...(group.type === "expense" && pillar ? { kakeibo_type: pillar } : {}) },
      label: group.label,
    });
    if (done) setPanel("none");
  };

  const submitRename = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const fields: Record<string, string | null> = {};
    if (merchant.trim()) fields.merchant = merchant.trim();
    if (notes.trim()) fields.notes = notes.trim();
    if (Object.keys(fields).length === 0) {
      setFormError("Isi nama toko atau catatan.");
      return;
    }
    const done = await onAction({ action: "rename", ids, fields, label: group.label });
    if (done) setPanel("none");
  };

  return (
    <article aria-labelledby={headingId} className="space-y-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-xs">
      <div className="flex flex-wrap items-start justify-between gap-x-3 gap-y-1">
        <div className="min-w-0 flex-1">
          <p className="text-[11px] font-semibold uppercase tracking-wider text-[var(--muted)]">{groupKindLabel(group.kind)}</p>
          <h3 id={headingId} className="break-words text-sm font-bold text-[var(--text)]">{group.label}</h3>
          <p className="mt-0.5 text-xs text-[var(--muted)] tabular">
            {[`${group.count} transaksi`, range, group.category?.name ? `Kategori: ${group.category.name}` : null].filter(Boolean).join(" · ")}
          </p>
        </div>
        <div className="shrink-0 text-right">
          <span className="block text-sm font-bold tabular text-[var(--text)]">
            {group.type === "income" ? "+" : group.type === "expense" ? "-" : ""}{bal(group.total)}
          </span>
          <span className="block text-[11px] text-[var(--muted)]">
            {group.type === "movement" ? "dipindahkan" : group.type === "income" ? "masuk" : "keluar"}
          </span>
        </div>
      </div>

      <ul className="flex flex-wrap gap-1.5 text-[11px]" aria-label="Status dan alasan">
        {group.needs_review_count > 0 && (
          <li className="rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 font-semibold text-amber-800 dark:text-amber-300">
            {group.needs_review_count} perlu ditinjau
          </li>
        )}
        {group.auto_count > 0 && (
          <li className="rounded-full border border-[var(--border)] bg-[var(--surface-raised)]/60 px-2 py-0.5 font-semibold text-[var(--text-secondary)]">
            {group.auto_count} otomatis, belum ditinjau
          </li>
        )}
        {group.reasons.map((reason) => (
          <li key={reason} className="rounded-full border border-[var(--border)] px-2 py-0.5 text-[var(--text-secondary)]">
            {reviewReasonLabel(reason)}
          </li>
        ))}
      </ul>

      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          disabled={pending}
          onClick={() => onAction({ action: "accept", ids, label: group.label })}
          className={PRIMARY}
        >
          Terima semua ({group.count})
        </button>
        {group.editable && (
          <>
            <button
              type="button"
              disabled={pending}
              aria-expanded={panel === "category"}
              aria-controls={`${baseId}-category`}
              onClick={() => togglePanel("category")}
              className={SECONDARY}
            >
              Ubah kategori semua
            </button>
            <button
              type="button"
              disabled={pending}
              aria-expanded={panel === "rename"}
              aria-controls={`${baseId}-rename`}
              onClick={() => togglePanel("rename")}
              className={SECONDARY}
            >
              Ganti nama
            </button>
          </>
        )}
      </div>

      {panel === "category" && (
        <form id={`${baseId}-category`} onSubmit={submitCategory} className="grid gap-3 rounded-xl border border-[var(--border)] p-3 sm:grid-cols-[1fr_1fr_auto] sm:items-end">
          <div className="space-y-1">
            <label htmlFor={`${baseId}-category-select`} className="block text-[11px] font-medium text-[var(--muted)]">Kategori</label>
            <select id={`${baseId}-category-select`} value={categoryId} onChange={(event) => setCategoryId(event.target.value)} className={FIELD}>
              <option value="">Pilih kategori</option>
              {choices.map((category) => (
                <option key={category.id} value={category.id}>{category.name}</option>
              ))}
            </select>
          </div>
          {group.type === "expense" ? (
            <div className="space-y-1">
              <label htmlFor={`${baseId}-pillar`} className="block text-[11px] font-medium text-[var(--muted)]">Pilar Kakeibo</label>
              <select id={`${baseId}-pillar`} value={pillar} onChange={(event) => setPillar(event.target.value)} className={FIELD}>
                <option value="">Ikuti kategori</option>
                {KAKEIBO_PILLARS.map((item) => (
                  <option key={item.value} value={item.value}>{item.label}</option>
                ))}
              </select>
            </div>
          ) : <div />}
          <button type="submit" disabled={pending} className={PRIMARY}>Simpan untuk {group.count}</button>
          <p className="text-[11px] text-[var(--muted)] sm:col-span-3">
            Pilihan ini juga diingat untuk transaksi berikutnya dari {group.kind === "merchant" ? group.label : "teks yang sama"}.
          </p>
        </form>
      )}

      {panel === "rename" && (
        <form id={`${baseId}-rename`} onSubmit={submitRename} className="grid gap-3 rounded-xl border border-[var(--border)] p-3 sm:grid-cols-[1fr_1fr_auto] sm:items-end">
          <div className="space-y-1">
            <label htmlFor={`${baseId}-merchant`} className="block text-[11px] font-medium text-[var(--muted)]">Nama toko</label>
            <input id={`${baseId}-merchant`} value={merchant} maxLength={MERCHANT_MAX_LENGTH} onChange={(event) => setMerchant(event.target.value)} className={FIELD} />
          </div>
          <div className="space-y-1">
            <label htmlFor={`${baseId}-notes`} className="block text-[11px] font-medium text-[var(--muted)]">Catatan</label>
            <input id={`${baseId}-notes`} value={notes} maxLength={500} onChange={(event) => setNotes(event.target.value)} placeholder="Biarkan kosong untuk tetap" className={FIELD} />
          </div>
          <button type="submit" disabled={pending} className={PRIMARY}>Simpan untuk {group.count}</button>
        </form>
      )}
      {formError && <p role="alert" className="text-[11px] font-medium text-rose-600 dark:text-rose-400">{formError}</p>}

      <details className="group/rows">
        <summary className="inline-flex min-h-11 cursor-pointer items-center rounded-lg text-xs font-semibold text-[var(--text-secondary)] hover:text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]">
          Lihat {group.count} transaksi
        </summary>
        <ul className="mt-1 divide-y divide-[var(--border)]">
          {group.transactions.map((item) => (
            <li key={item.id} className="flex items-center justify-between gap-3 py-2 text-xs">
              <div className="min-w-0">
                <p className="truncate font-medium text-[var(--text)]">{item.notes || item.merchant || item.category_name || "Tanpa keterangan"}</p>
                <p className="text-[11px] text-[var(--muted)] tabular">
                  {[
                    item.date ? new Date(item.date).toLocaleDateString("id-ID", { day: "numeric", month: "short", year: "numeric" }) : null,
                    item.account_name,
                    item.category_name ?? "Tanpa kategori",
                    item.kakeibo_type ? kakeiboLabel(item.kakeibo_type) : null,
                  ].filter(Boolean).join(" · ")}
                </p>
              </div>
              <div className="flex shrink-0 items-center gap-2">
                <span className="font-semibold tabular text-[var(--text)]">
                  {item.type === "income" ? "+" : "-"}{bal(item.amount)}
                </span>
                <Link
                  href={`/ledger?edit=${item.id}&return=review`}
                  aria-label={`Ubah transaksi ${item.notes || item.merchant || group.label}`}
                  className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-lg px-2 font-semibold text-[var(--text)] underline-offset-2 hover:underline focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                >
                  Ubah
                </Link>
              </div>
            </li>
          ))}
        </ul>
      </details>
    </article>
  );
}
