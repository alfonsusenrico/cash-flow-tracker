"use client";

import Link from "next/link";
import { transaksiHref } from "@/lib/triage";
import { useId, useState, type FormEvent } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Choice, Picker } from "@/components/review/triage/Choice";
import { ItemHead, longWhen } from "@/components/review/triage/ItemHead";
import { KAKEIBO_PILLARS, kakeiboLabel } from "@/lib/kakeibo";
import { MERCHANT_MAX_LENGTH } from "@/lib/provenance";
import { dateRangeLabel, groupKindLabel, type ReviewAction, type ReviewGroup } from "@/lib/review";

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
  headingId: string;
  categories: CategoryOption[];
  pending: boolean;
  /** "Dicatat otomatis": the accept reads "Sudah benar" and the item is optional. */
  automatic?: boolean;
  onAction: (request: GroupActionRequest) => Promise<boolean>;
}

type Panel = "none" | "category" | "rename";

const ROWS_SHOWN = 10;

/** One merchant or signature group: one answer settles every row, each row keeps "Ubah" (D2, OD16). */
export function ReviewGroupCard({ group, headingId, categories, pending, automatic = false, onAction }: ReviewGroupCardProps) {
  const { bal } = useAppCtx();
  const baseId = useId();
  const [panel, setPanel] = useState<Panel>("none");
  const [allRows, setAllRows] = useState(false);
  const [categoryId, setCategoryId] = useState(group.category?.id ?? "");
  const [pillar, setPillar] = useState("");
  const [merchant, setMerchant] = useState(group.kind === "merchant" ? group.label : "");
  const [notes, setNotes] = useState("");
  const [formError, setFormError] = useState("");
  const ids = group.transactions.map((item) => item.id);
  const range = dateRangeLabel(group.first_date, group.last_date);
  const choices = categories.filter((category) => category.kind === group.type && !category.is_archived);
  const rows = allRows ? group.transactions : group.transactions.slice(0, ROWS_SHOWN);
  const latest = group.transactions[0];

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

  const counts = [
    group.needs_review_count > 0 ? `${group.needs_review_count} perlu ditinjau` : null,
    group.auto_count > 0 ? `${group.auto_count} otomatis, belum ditinjau` : null,
  ].filter(Boolean).join(" · ");

  return (
    <>
      <ItemHead
        headingId={headingId}
        kind={automatic
          ? { tone: "neutral", icon: "check", text: "Dicatat otomatis" }
          : { tone: "warn", icon: "alert-triangle", text: "Perlu jawaban" }}
        title={group.label}
        account={latest?.account_name}
        date={latest?.date}
        source={latest?.source}
        amount={{ text: `${group.type === "income" ? "+" : "−"}${bal(group.total)}` }}
        reasons={group.reasons}
      />
      <div className="ws-tri-note">
        <small>{groupKindLabel(group.kind)} · {counts}</small>
        <p className="tabular">
          {[`${group.count} transaksi`, range, group.category?.name ? `Kategori: ${group.category.name}` : "Kategori campur"]
            .filter(Boolean).join(" · ")}
        </p>
      </div>
      {automatic && (
        <p className="ws-notice">Opsional. Transaksi ini sudah tercatat dan tetap tersimpan meskipun dilewati.</p>
      )}

      <div role="group" aria-label="Pilihan" className="ws-choices">
        <Choice
          n={1}
          primary
          disabled={pending}
          label={automatic ? "Sudah benar" : `Terima semua (${group.count})`}
          sub={automatic ? "Tandai sudah dicek" : "Kategorinya sudah benar"}
          onClick={() => void onAction({ action: "accept", ids, label: group.label })}
        />
        <Choice
          n={2}
          disabled={pending}
          label="Ubah kategori"
          sub="Kategori dan pilar Kakeibo"
          expanded={panel === "category"}
          controls={panel === "category" ? `${baseId}-category` : undefined}
          onClick={() => togglePanel("category")}
        />
        <Choice
          n={3}
          disabled={pending}
          label="Ganti nama"
          sub="Nama toko dan catatan"
          expanded={panel === "rename"}
          controls={panel === "rename" ? `${baseId}-rename` : undefined}
          onClick={() => togglePanel("rename")}
        />
      </div>

      {panel === "category" && (
        <Picker id={`${baseId}-category`} title={`Ubah kategori ${group.count} transaksi`}>
          <form onSubmit={submitCategory} className="ws-form-row">
            <label className="ws-field">
              Kategori
              <select value={categoryId} onChange={(event) => setCategoryId(event.target.value)}>
                <option value="">Pilih kategori</option>
                {choices.map((category) => (
                  <option key={category.id} value={category.id}>{category.name}</option>
                ))}
              </select>
            </label>
            {group.type === "expense" && (
              <label className="ws-field">
                Pilar Kakeibo
                <select value={pillar} onChange={(event) => setPillar(event.target.value)}>
                  <option value="">Ikuti kategori</option>
                  {KAKEIBO_PILLARS.map((item) => (
                    <option key={item.value} value={item.value}>{item.label}</option>
                  ))}
                </select>
              </label>
            )}
            <button type="submit" disabled={pending} className="ws-btn ws-btn-ink">Simpan untuk {group.count}</button>
          </form>
          <p className="ws-caption">
            Pilihan ini juga diingat untuk transaksi berikutnya dari {group.kind === "merchant" ? group.label : "teks yang sama"}.
          </p>
        </Picker>
      )}

      {panel === "rename" && (
        <Picker id={`${baseId}-rename`} title={`Ganti nama ${group.count} transaksi`}>
          <form onSubmit={submitRename} className="ws-form-row">
            <label className="ws-field">
              Nama toko
              <input value={merchant} maxLength={MERCHANT_MAX_LENGTH} onChange={(event) => setMerchant(event.target.value)} />
            </label>
            <label className="ws-field">
              Catatan
              <input value={notes} maxLength={500} onChange={(event) => setNotes(event.target.value)} placeholder="Biarkan kosong untuk tetap" />
            </label>
            <button type="submit" disabled={pending} className="ws-btn ws-btn-ink">Simpan untuk {group.count}</button>
          </form>
        </Picker>
      )}
      {formError && <p role="alert" className="ws-alert">{formError}</p>}

      <ul className="ws-rows" aria-label={`Transaksi ${group.label}`}>
        {rows.map((item) => (
          <li key={item.id}>
            <span className="ws-rows-t">
              {item.notes || item.merchant || item.category_name || "Tanpa keterangan"}
              <span className="tabular">
                {[longWhen(item.date), item.account_name, item.category_name ?? "Tanpa kategori",
                  item.kakeibo_type ? kakeiboLabel(item.kakeibo_type) : null,
                  item.review_status === "needs_review" ? "perlu ditinjau" : "otomatis"].filter(Boolean).join(" · ")}
              </span>
            </span>
            <span className="flex shrink-0 items-center gap-2">
              <span className="tabular font-medium">{item.type === "income" ? "+" : "−"}{bal(item.amount)}</span>
              <Link
                href={transaksiHref(item.id, automatic ? "otomatis" : "jawab")}
                aria-label={`Ubah transaksi ${item.notes || item.merchant || group.label}`}
                className="ws-btn ws-btn-sm"
              >
                Ubah
              </Link>
            </span>
          </li>
        ))}
      </ul>
      {group.transactions.length > ROWS_SHOWN && !allRows && (
        <button type="button" className="ws-btn ws-btn-sm mt-2" onClick={() => setAllRows(true)}>
          Lihat semua {group.transactions.length}
        </button>
      )}
    </>
  );
}
