import { ApiError } from "@/lib/api";
import { amountRefused, type Capture, type CapturePreview } from "@/lib/capture";
import { hasDigit, looksLikeQuestion, matchIntent, normaliseText, type BillsRange, type IntentMatch } from "./intents";
import {
  ASK_AMOUNT_TEXT, HELP_TEXT, MIXED_TEXT, SEVERAL_AMOUNTS_TEXT, UNKNOWN_TEXT, intentIntro, recordedText,
} from "./replies";
import type { AssistantMessage, CardData, CardKind, CardSnapshot, ChipKey } from "./types";

/**
 * Tanya's resolver chain (add-tanya-conversation D2): the intent table, then the capture route, then the
 * unknown reply. Each resolver returns an outcome or passes. A later server resolver
 * (`add-tanya-llm-answers`) slots in before `unknownResolver` and returns the same card kinds.
 */

export const SUGGESTION_CHIPS: readonly ChipKey[] = ["cycle", "waiting", "report", "bills_week"];

export const NOTICE_RATE_LIMITED = "Terlalu banyak catatan dalam satu menit. Coba lagi sebentar lagi.";
export const NOTICE_PARSE_FAILED = "Pesan ini tidak dapat diproses. Coba lagi.";

export interface TurnDeps {
  parse: (text: string) => Promise<{ preview: CapturePreview }>;
  submit: (text: string, clientCaptureId: string) => Promise<{ capture: Capture }>;
  loadCard: (kind: CardKind) => Promise<CardData>;
  now: () => Date;
}

export interface ResolverInput {
  text: string;
  normalised: string;
  clientCaptureId: string;
  /** "Catat sebagai transaksi": skip the intent table and the question rule, on the owner's explicit choice. */
  forceCapture: boolean;
  deps: TurnDeps;
}

export type AssistantDraft = Omit<AssistantMessage, "id" | "role" | "at">;

export interface TurnOutcome {
  reply: AssistantDraft | null;
  /** The user's message did not reach the server; "Kirim ulang" resends it with the same id. */
  sendFailed?: boolean;
  /** The text goes back into the composer (rate limit, parse failure), nothing was recorded. */
  restoreText?: boolean;
  /** The resolver that answered, for tests and the request log. */
  route: "intent" | "capture" | "ask_amount" | "unknown" | "error";
}

export type Resolver = (input: ResolverInput) => Promise<TurnOutcome | null>;

function draft(fields: Partial<AssistantDraft>): AssistantDraft {
  return { text: null, notice: null, card: null, capture: null, chips: [], ...fields };
}

/** Builds a card from a fresh fetch; a failed fetch is an error card, never a figure of 0 (D9). */
export async function buildCard(kind: CardKind, range: BillsRange | undefined, deps: Pick<TurnDeps, "loadCard" | "now">): Promise<CardSnapshot> {
  const asOf = deps.now().toISOString();
  try {
    return { kind, range, status: "ready", asOf, data: await deps.loadCard(kind) };
  } catch {
    return { kind, range, status: "error", asOf, data: null };
  }
}

function cardKind(match: IntentMatch): CardKind | null {
  return match.intent === "help" ? null : match.intent;
}

export const intentResolver: Resolver = async ({ text, normalised, forceCapture, deps }) => {
  if (forceCapture) return null;
  const match = matchIntent(normalised);
  if (!match) return null;
  const kind = cardKind(match);
  if (!kind) return { route: "intent", reply: draft({ text: HELP_TEXT, chips: [...SUGGESTION_CHIPS] }) };
  const card = await buildCard(kind, match.range, deps);
  const mixed = hasDigit(normalised);
  return {
    route: "intent",
    reply: draft({
      text: mixed ? `${intentIntro(kind, match.range)} ${MIXED_TEXT}` : intentIntro(kind, match.range),
      card,
      chips: mixed ? ["capture"] : [],
      captureText: mixed ? text : undefined,
    }),
  };
};

function isRateLimited(failure: unknown): boolean {
  return failure instanceof ApiError && failure.status === 429;
}

function isNetworkFailure(failure: unknown): boolean {
  return !(failure instanceof ApiError);
}

export const captureResolver: Resolver = async ({ text, normalised, clientCaptureId, forceCapture, deps }) => {
  if (!forceCapture && looksLikeQuestion(normalised)) return null;
  let preview: CapturePreview;
  try {
    preview = (await deps.parse(text)).preview;
  } catch (failure) {
    return { route: "error", restoreText: true, reply: draft({ notice: isRateLimited(failure) ? NOTICE_RATE_LIMITED : NOTICE_PARSE_FAILED }) };
  }
  if (amountRefused(preview)) {
    if (!forceCapture && !preview.kind && !preview.description) return null;
    const several = preview.problems.includes("several_amounts");
    return { route: "ask_amount", reply: draft({ text: several ? SEVERAL_AMOUNTS_TEXT : ASK_AMOUNT_TEXT }) };
  }
  if (preview.amount === null) return forceCapture ? { route: "ask_amount", reply: draft({ text: ASK_AMOUNT_TEXT }) } : null;
  try {
    const { capture } = await deps.submit(text, clientCaptureId);
    const toReview = capture.transaction?.review_status === "needs_review";
    const said = capture.status === "recorded" ? recordedText(toReview) : null;
    return { route: "capture", reply: draft({ text: said, capture }) };
  } catch (failure) {
    if (isRateLimited(failure)) return { route: "error", restoreText: true, reply: draft({ notice: NOTICE_RATE_LIMITED }) };
    if (isNetworkFailure(failure)) return { route: "error", sendFailed: true, reply: null };
    const message = failure instanceof Error && failure.message ? failure.message : "Belum dapat dicatat. Coba lagi.";
    return { route: "error", sendFailed: true, reply: draft({ notice: message }) };
  }
};

export const unknownResolver: Resolver = async () => ({
  route: "unknown",
  reply: draft({ text: UNKNOWN_TEXT, chips: [...SUGGESTION_CHIPS] }),
});

export const RESOLVERS: readonly Resolver[] = [intentResolver, captureResolver, unknownResolver];

/** One sent text to one outcome, through the chain in order. Empty text resolves to nothing. */
export async function runTurn(
  text: string,
  clientCaptureId: string,
  deps: TurnDeps,
  options: { forceCapture?: boolean; resolvers?: readonly Resolver[] } = {},
): Promise<TurnOutcome> {
  const normalised = normaliseText(text);
  if (!normalised) return { route: "unknown", reply: null };
  const input: ResolverInput = { text: text.trim().slice(0, 200), normalised, clientCaptureId, forceCapture: Boolean(options.forceCapture), deps };
  for (const resolver of options.resolvers ?? RESOLVERS) {
    const outcome = await resolver(input);
    if (outcome) return outcome;
  }
  return unknownResolver(input) as Promise<TurnOutcome>;
}

