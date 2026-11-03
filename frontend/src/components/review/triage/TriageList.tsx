"use client";

import type { RefObject } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { ChipGroup } from "@/components/workspace/primitives";
import { FOLDER_LABELS, itemTitle, type FolderKey, type Triage, type TriageItem } from "@/lib/triage";

const WHEN = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });

function itemDate(item: TriageItem): string | null {
  switch (item.kind) {
    case "question": return item.entry.transaction.date;
    case "group": return item.group.last_date;
    case "suggestion": return item.group.last_date;
    default: return item.item.date;
  }
}

function itemLine(item: TriageItem): string {
  switch (item.kind) {
    case "question": return "Apa ini? · Pembayaran tanpa nama toko";
    case "group": {
      const { group } = item;
      const parts = item.folder === "otomatis" ? ["Dicatat otomatis"] : ["Perlu jawaban"];
      parts.push(group.count > 1 ? `${group.count} transaksi` : group.category?.name ?? "Kategori belum pasti");
      return parts.join(" · ");
    }
    case "awaiting": {
      const suggestion = item.item.topup_suggestion;
      const product = suggestion?.choices.find((choice) => choice.id === suggestion.target_account_id)?.name;
      return product ? `Top up ${product}?` : "Pasangan belum ditemukan";
    }
    case "ambiguous": return `${item.item.candidates.length} kemungkinan pasangan`;
    case "suggestion": return `Saran · ${item.group.category.name}`;
  }
}

function itemAmount(item: TriageItem, bal: (amount: number) => string): { text: string; className: string } {
  switch (item.kind) {
    case "question": return { text: `−${bal(item.entry.transaction.amount)}`, className: "" };
    case "group":
    case "suggestion": return { text: `${item.group.type === "income" ? "+" : "−"}${bal(item.group.total)}`, className: item.group.type === "income" ? "ws-amt-in" : "" };
    default: return { text: `⇄ ${bal(item.item.amount)}`, className: "ws-amt-tr" };
  }
}

function Avatar({ item }: { item: TriageItem }) {
  if (item.kind === "awaiting" || item.kind === "ambiguous") {
    return <span className="ws-ava ws-tone-tr" aria-hidden="true"><Icon name="arrows-right-left" /></span>;
  }
  if (item.kind === "suggestion") return <span className="ws-ava ws-tone-neutral" aria-hidden="true"><Icon name="sparkles" /></span>;
  if (item.kind === "group" && item.folder === "otomatis") return <span className="ws-ava ws-tone-ok" aria-hidden="true"><Icon name="check" /></span>;
  return <span className="ws-ava ws-tone-warn" aria-hidden="true">{item.kind === "question" ? "?" : "!"}</span>;
}

export function TriageRows({ items, selectedId, onOpen, label }: {
  items: TriageItem[];
  selectedId: string | null;
  onOpen: (item: TriageItem) => void;
  label: string;
}) {
  const { bal } = useAppCtx();
  return (
    <ul className="ws-tri-ul" aria-label={label}>
      {items.map((item) => {
        const date = itemDate(item);
        const amount = itemAmount(item, bal);
        return (
          <li key={item.id}>
            <button
              type="button"
              className="ws-tri-row"
              data-row={item.id}
              aria-current={item.id === selectedId ? "true" : undefined}
              onClick={() => onOpen(item)}
            >
              <Avatar item={item} />
              <span className="ws-tri-t">
                <span className="ws-tri-top">
                  <b>{itemTitle(item)}</b>
                  {date && <span className="ws-tri-when tabular">{WHEN.format(new Date(date))}</span>}
                </span>
                <span className="ws-tri-bot">
                  <span className="ws-tri-line">{itemLine(item)}</span>
                  <span className={`tabular ${amount.className}`}>{amount.text}</span>
                </span>
              </span>
            </button>
          </li>
        );
      })}
    </ul>
  );
}

export interface TriageListProps {
  triage: Triage;
  folder: FolderKey;
  items: TriageItem[];
  selectedId: string | null;
  onFolder: (folder: FolderKey) => void;
  onOpen: (item: TriageItem) => void;
  foldOpen: { otomatis: boolean; saran: boolean };
  onFold: (key: "otomatis" | "saran", open: boolean) => void;
  onSearchSuggestions: () => void;
  searchOpen: boolean;
  listRef: RefObject<HTMLElement>;
}

/** The left pane: header counts from the badge, folder chips, rows and the optional folds (D1, D3). */
export function TriageList({
  triage, folder, items, selectedId, onFolder, onOpen, foldOpen, onFold, onSearchSuggestions, searchOpen, listRef,
}: TriageListProps) {
  const needed = folder === "semua" || folder === "apa" || folder === "jawab" || folder === "pindah";
  const total = triage.counts.apa + triage.counts.jawab + triage.counts.pindah;
  const options: { value: FolderKey; label: string; count: number }[] = [
    { value: "semua", label: FOLDER_LABELS.semua, count: total },
    { value: "apa", label: FOLDER_LABELS.apa, count: triage.counts.apa },
    { value: "jawab", label: FOLDER_LABELS.jawab, count: triage.counts.jawab },
    { value: "pindah", label: FOLDER_LABELS.pindah, count: triage.counts.pindah },
  ];
  const automatic = triage.folders.otomatis;
  const suggestions = triage.folders.saran;
  return (
    <section ref={listRef} className="ws-card ws-tri-list" aria-labelledby="triage-list-heading">
      <div className="ws-tri-head">
        <h2 id="triage-list-heading">
          Perlu ditinjau
          <span className="ws-cnt">
            <span aria-hidden="true">{triage.attention}</span>
            <span className="sr-only">{triage.attention} transaksi</span>
          </span>
        </h2>
        <span className="ws-caption tabular">{triage.unreviewed} dicatat otomatis · opsional</span>
      </div>
      <div className="ws-tri-folders">
        <ChipGroup label="Saring Tinjau" options={options} selected={needed ? [folder] : []} onToggle={onFolder} />
      </div>
      {triage.truncated && (
        <p className="ws-notice mx-[18px]">Hanya 1.000 transaksi terbaru yang ditampilkan. Selesaikan yang ada untuk melihat sisanya.</p>
      )}
      {needed && (items.length ? (
        <TriageRows items={items} selectedId={selectedId} onOpen={onOpen} label={`${FOLDER_LABELS[folder]}: yang perlu dijawab`} />
      ) : (
        <p className="ws-tri-empty">
          <Icon name="check" className="h-4 w-4" />
          {total ? "Folder ini sudah kosong." : "Tidak ada yang perlu dijawab."}
        </p>
      ))}

      {(automatic.length > 0 || triage.autoInMixedGroups > 0) && (
        <details className="ws-tri-fold" open={foldOpen.otomatis || folder === "otomatis"} onToggle={(event) => onFold("otomatis", event.currentTarget.open)}>
          <summary>
            <Icon name="chevron-right" />
            <span><b>{FOLDER_LABELS.otomatis}</b> <span className="ws-cnt">{triage.counts.otomatis}</span></span>
            <span className="ws-opt">opsional · boleh dilewati</span>
          </summary>
          <p className="ws-caption">
            Dicatat dari aturan atau AI. Pengecekan opsional.
            {triage.autoInMixedGroups > 0 && ` ${triage.autoInMixedGroups} lainnya ikut kelompok di Perlu jawaban.`}
          </p>
          {automatic.length > 0 && (
            <TriageRows items={automatic} selectedId={selectedId} onOpen={onOpen} label="Dicatat otomatis" />
          )}
        </details>
      )}
      {suggestions.length > 0 && (
        <details className="ws-tri-fold" open={foldOpen.saran || folder === "saran"} onToggle={(event) => onFold("saran", event.currentTarget.open)}>
          <summary>
            <Icon name="chevron-right" />
            <span><b>{FOLDER_LABELS.saran}</b> <span className="ws-cnt">{triage.counts.saran}</span></span>
            <span className="ws-opt">opsional</span>
          </summary>
          <p className="ws-caption">Usulan untuk transaksi lama. Tidak dihitung sebagai yang perlu dijawab.</p>
          <TriageRows items={suggestions} selectedId={selectedId} onOpen={onOpen} label="Saran dari riwayat" />
        </details>
      )}

      <div className="ws-tri-foot">
        <button type="button" className="ws-btn ws-btn-sm self-start" aria-expanded={searchOpen} onClick={onSearchSuggestions}>
          <Icon name="sparkles" className="h-4 w-4" />
          Cari saran dari riwayat
        </button>
        <p className="ws-tri-hints">
          <span>Klik atau Tab ke daftar, lalu</span>
          <span><kbd className="ws-kbd">J</kbd><kbd className="ws-kbd">K</kbd> pindah</span>
          <span><kbd className="ws-kbd">E</kbd> terima</span>
          <span><kbd className="ws-kbd">T</kbd> top up</span>
          <span><kbd className="ws-kbd">Z</kbd> urungkan</span>
          <span><kbd className="ws-kbd">?</kbd> semua</span>
        </p>
      </div>
    </section>
  );
}
