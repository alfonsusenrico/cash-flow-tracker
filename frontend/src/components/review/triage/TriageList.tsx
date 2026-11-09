"use client";

import type { ReactNode, RefObject } from "react";
import type { IconName } from "@/components/ui/Icon";
import { ListRow } from "@/components/workspace/lists";
import { EmptyState } from "@/components/workspace/primitives";
import { Rail, RailItem, RailRule } from "@/components/workspace/rail";
import type { Words } from "@/lib/i18n/types";
import { isOptionalFolder, NEEDED_FOLDERS, OPTIONAL_FOLDERS, type FolderKey, type Triage, type TriageItem } from "@/lib/triage";
import { SignedAmount } from "./ItemBody";
import { dateTime, folderName, itemDate, itemTitle } from "./copy";

/**
 * The folder rail (apply-markas-tinjau D4, D5): "Semua" and the needed folders with their row counts, the rule, then
 * the optional folders. A folder without items is hidden unless it is "Semua" or current; below 768 px the rail is
 * one line of chips.
 */
export function TriageRail({ triage, folder, onFolder, words }: {
  triage: Triage;
  folder: FolderKey;
  onFolder: (folder: FolderKey) => void;
  words: Words;
}) {
  const { t, format } = words;
  const count = (key: FolderKey) =>
    key === "semua" ? triage.counts.apa + triage.counts.jawab + triage.counts.pindah : triage.counts[key];
  const shown = (key: FolderKey) => key === "semua" || key === folder || triage.folders[key].length > 0;
  const item = (key: FolderKey) => (
    <RailItem
      key={key}
      label={folderName(t, key)}
      count={format.integer(count(key))}
      countName={t("review.folders.countName", { count: count(key) })}
      current={key === folder}
      onSelect={() => onFolder(key)}
    />
  );
  const optional = OPTIONAL_FOLDERS.filter(shown);
  return (
    <Rail label={t("review.folders.label")} chips className="tj-rail">
      {(["semua", ...NEEDED_FOLDERS] as FolderKey[]).filter(shown).map(item)}
      {optional.length > 0 && <RailRule />}
      {optional.map(item)}
    </Rail>
  );
}

function rowIcon(item: TriageItem): IconName {
  if (item.kind === "question") return "help";
  if (item.folder === "lewat") return "archive";
  if (item.folder === "otomatis") return "check";
  return "alert";
}

function rowAmount(item: TriageItem): ReactNode {
  switch (item.kind) {
    case "question":
      return <SignedAmount amount={item.entry.transaction.amount} direction={item.entry.transaction.type} />;
    case "group":
    case "suggestion":
      return <SignedAmount amount={item.group.total} direction={item.group.type} />;
    case "awaiting":
    case "ambiguous":
      return <SignedAmount amount={item.item.amount} direction="transfer" />;
    case "seen_twice": {
      const { amount, type } = item.item;
      if (amount === null) return undefined;
      return <SignedAmount amount={amount} direction={type === "income" ? "income" : type === "expense" ? "expense" : "transfer"} />;
    }
  }
}

function rowMeta(item: TriageItem, folder: FolderKey, { t, format }: Words): string {
  const when = dateTime(format, itemDate(item));
  const kind = t(`review.list.kind.${item.folder}`);
  if (folder !== "semua") return when ?? kind;
  return when ? t("review.list.kindWhen", { kind, when }) : kind;
}

export interface TriageListProps {
  triage: Triage;
  folder: FolderKey;
  items: TriageItem[];
  selectedId: string | null;
  onOpen: (item: TriageItem) => void;
  listRef: RefObject<HTMLDivElement>;
  words: Words;
  /** What an empty folder shows instead of the folder's empty state, such as the phone's done state. */
  empty?: ReactNode;
}

/** The item list (D6): whole-row items with the icon column, title, kind and time, and the signed amount. */
export function TriageList({ triage, folder, items, selectedId, onOpen, listRef, words, empty }: TriageListProps) {
  const { t } = words;
  const caption =
    folder === "otomatis"
      ? [
          t("review.folders.caption.otomatis"),
          triage.autoInMixedGroups > 0 ? t("review.folders.caption.mixed", { count: triage.autoInMixedGroups }) : null,
        ]
          .filter(Boolean)
          .join(" ")
      : isOptionalFolder(folder)
        ? t(`review.folders.caption.${folder}`)
        : null;
  return (
    <div ref={listRef} className="tj-list">
      {caption && <p className="t-meta tj-caption">{caption}</p>}
      {items.length ? (
        <ul className="list" aria-label={t("review.list.label", { folder: folderName(t, folder) })}>
          {items.map((item) => (
            <li key={item.id}>
              <ListRow
                layout="stacked"
                rowKey={item.id}
                selected={item.id === selectedId}
                current={item.id === selectedId}
                icon={rowIcon(item)}
                title={itemTitle(t, item)}
                meta={rowMeta(item, folder, words)}
                amount={rowAmount(item)}
                onClick={() => onOpen(item)}
              />
            </li>
          ))}
        </ul>
      ) : (
        (empty ?? <EmptyState title={t("review.states.emptyFolder.title")}>{t("review.states.emptyFolder.line")}</EmptyState>)
      )}
    </div>
  );
}
