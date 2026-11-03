import type { CycleSummary } from "@/components/beranda/types";
import type { Capture } from "@/lib/capture";
import type { FolderKey } from "@/lib/triage";
import type { ReportReadiness } from "@/lib/reportReady";
import type { BillsRange } from "./intents";

export type CardKind = "cycle" | "kakeibo" | "bills" | "waiting" | "report";

/** One Tinjau item as the waiting card lists it, with its deep link. */
export interface WaitingCardItem {
  id: string;
  title: string;
  amount: number;
  folder: FolderKey;
  folderLabel: string;
  href: string;
}

export interface WaitingCardData {
  /** The badge's `attention`: rows, so it can exceed the number of items listed. */
  attention: number;
  items: WaitingCardItem[];
}

export type CardData =
  | { kind: "cycle" | "kakeibo" | "bills"; summary: CycleSummary }
  | { kind: "waiting"; waiting: WaitingCardData }
  | { kind: "report"; report: ReportReadiness };

/** A card holds the figures it was built from, so an earlier answer stays true to its moment (D3). */
export interface CardSnapshot {
  kind: CardKind;
  range?: BillsRange;
  status: "ready" | "error";
  asOf: string;
  data: CardData | null;
}

/** Labels of the suggestion chips; each one sends its fixed query through the same resolver (D4). */
export type ChipKey = "cycle" | "waiting" | "report" | "bills_week" | "capture";

export interface UserMessage {
  id: string;
  role: "user";
  at: string;
  text: string;
  /** Kept per sent message, so "Kirim ulang" never records twice. */
  clientCaptureId: string;
  failed: boolean;
}

export interface AssistantMessage {
  id: string;
  role: "assistant";
  at: string;
  /** The assistant's own words (lib/tanya/replies.ts). */
  text: string | null;
  /** A neutral system notice: errors and limits. */
  notice: string | null;
  card: CardSnapshot | null;
  capture: Capture | null;
  chips: ChipKey[];
  /** The text that "Catat sebagai transaksi" sends straight to capture. */
  captureText?: string;
}

export type ThreadMessage = UserMessage | AssistantMessage;
