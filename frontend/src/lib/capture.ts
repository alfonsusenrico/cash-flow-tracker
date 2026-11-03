import { api } from "@/lib/api";

/** The capture text limit, the same as the server's (`/capture/text` and `/capture/parse`). */
export const CAPTURE_TEXT_LIMIT = 200;
/** Every question type the web can answer, so time questions are asked here too. */
export const CAPTURE_QUESTION_TYPES = ["amount", "time", "kind", "account", "category"];

const TIME = new Intl.DateTimeFormat("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });
const DAY = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" });

/** What the deterministic parser reads in a text (`POST /capture/parse`); nothing is written. */
export interface CapturePreview {
  amount: number | null;
  problems: string[];
  kind: "expense" | "income" | "movement" | null;
  description: string | null;
  occurred_at: string | null;
  time_phrase: string | null;
  time_options: string[];
  account_hint: { account_id: string; label: string } | null;
  category_hint: { category_id: string; label: string; pillar: string | null } | null;
}

export interface CaptureQuestion {
  question_id: string;
  type: string;
  prompt: string;
  options: { id: string; label: string }[];
  allows_text: boolean;
}

/** The recorded row of a capture, as `capture_view` returns it. */
export interface CaptureTransaction {
  id: string;
  type: string;
  amount: number;
  date: string;
  account_name: string;
  merchant: string | null;
  notes: string | null;
  category_name: string | null;
  kakeibo: string | null;
  review_status: string | null;
}

export interface Capture {
  id: string;
  status: "recorded" | "needs_detail" | "undone" | string;
  question: CaptureQuestion | null;
  summary: { amount: number | null; description: string | null; time_label: string | null };
  transaction?: CaptureTransaction | null;
  undo_until: string | null;
}

/** The device's local time with its offset, which the server uses to read "jam 2" and "tadi". */
export function deviceTime(): string {
  const now = new Date();
  const offset = -now.getTimezoneOffset();
  const sign = offset >= 0 ? "+" : "-";
  const pad = (value: number) => String(Math.floor(Math.abs(value))).padStart(2, "0");
  const local = new Date(now.getTime() + offset * 60_000).toISOString().slice(0, 19);
  return `${local}${sign}${pad(offset / 60)}:${pad(offset % 60)}`;
}

export function newId(): string {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : "10000000-1000-4000-8000-100000000000".replace(/[018]/g, (c) => (Number(c) ^ (Math.random() * 16) >> (Number(c) / 4)).toString(16));
}

export function timeText(preview: CapturePreview): { text: string; missing: boolean } {
  if (preview.occurred_at) {
    const sameDay = DAY.format(new Date(preview.occurred_at)) === DAY.format(new Date());
    const clock = TIME.format(new Date(preview.occurred_at));
    if (!preview.time_phrase) return { text: `Waktu: sekarang, ${clock}`, missing: false };
    return { text: sameDay ? `Jam ${clock}` : `${preview.time_phrase}, ${clock}`, missing: false };
  }
  return { text: "Jam: ditanyakan setelah dicatat", missing: true };
}

/** Whether the capture's own undo window (10 s plus grace on the server) is still open. */
export function undoStillOpen(until: string | null): boolean {
  return Boolean(until) && Date.parse(until as string) > Date.now();
}

/** Whether the parser refused the text for its amount: none, or more than one. */
export function amountRefused(preview: CapturePreview | null): boolean {
  return Boolean(preview && (preview.problems.includes("missing_amount") || preview.problems.includes("several_amounts")));
}

export function parseCapture(text: string, signal?: AbortSignal): Promise<{ preview: CapturePreview }> {
  return api.post<{ preview: CapturePreview }>("/capture/parse", { text, device_time: deviceTime() }, signal);
}

/** Records the text. The same `clientCaptureId` on a retry never records twice. */
export function submitCapture(text: string, clientCaptureId: string): Promise<{ capture: Capture }> {
  return api.post<{ capture: Capture }>("/capture/text", {
    client_capture_id: clientCaptureId, text, device_time: deviceTime(), question_types: CAPTURE_QUESTION_TYPES,
  });
}

export function answerCapture(capture: Capture, answer: Record<string, string>): Promise<{ capture: Capture }> {
  return api.post<{ capture: Capture }>(`/capture/${capture.id}/answer`, {
    question_id: capture.question?.question_id, reply_id: newId(), ...answer,
  });
}

export function undoCapture(captureId: string): Promise<{ capture: Capture }> {
  return api.post<{ capture: Capture }>(`/capture/${captureId}/undo`);
}
