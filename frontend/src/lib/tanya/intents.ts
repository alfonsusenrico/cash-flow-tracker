/**
 * Tanya's intent table (add-tanya-conversation D2): a fixed, ordered list of word-boundary patterns in
 * Indonesian and English. The first entry with a matching pattern wins, so the more specific intents come
 * before `cycle`, whose words ("siklus", "ringkasan") also appear in the others' questions. Nothing here
 * reads an amount; amounts only ever come from the server parser.
 */

export type IntentName = "help" | "report" | "bills" | "waiting" | "kakeibo" | "cycle";
export type BillsRange = "week" | "window";

export interface IntentMatch {
  intent: IntentName;
  /** Only for `bills`: the next 7 days, or the cycle summary's window. */
  range?: BillsRange;
}

/** The same cap as capture, so a long paste is cut the same way everywhere. */
export const TANYA_TEXT_LIMIT = 200;

interface IntentEntry {
  intent: IntentName;
  patterns: RegExp[];
}

export const INTENT_TABLE: readonly IntentEntry[] = [
  {
    intent: "help",
    patterns: [/\bbantuan\b/, /\bbisa apa\b/, /\bcontoh\b/, /\bhelp\b/, /\bwhat can you do\b/],
  },
  {
    intent: "report",
    patterns: [/\blaporan\b/, /\brapor\b/, /\breports?\b/],
  },
  {
    intent: "bills",
    patterns: [/\btagihan\b/, /\blangganan\b/, /\bcicilan\b/, /\bjatuh tempo\b/, /\bsetoran\b/, /\bbills?\b/, /\bdue\b/,
      /\bupcoming payments?\b/, /\bsubscriptions?\b/],
  },
  {
    intent: "waiting",
    patterns: [/\b(me)?nunggu\b/, /\btinjau(an)?\b/, /\byang perlu (di)?jawab\b/, /\bbelum dikenali\b/,
      /\bwaiting\b/, /\bto review\b/, /\bpending\b/],
  },
  {
    intent: "kakeibo",
    patterns: [/\bkakeibo\b/, /\bpilar\b/, /\bkebutuhan\b/, /\bkeinginan\b/, /\bbudaya\b/, /\btak terduga\b/,
      /\bke ?mana (aja |saja )?uang/, /\bpillars?\b/, /\bneeds and wants\b/, /\bwhere did my money go\b/],
  },
  {
    intent: "cycle",
    patterns: [/\baman\b/, /\bsisa\b/, /\bringkas(an)?\b/, /\bsiklus\b/, /\bposisi (uang|keuangan)\b/, /\bbisa pakai berapa\b/,
      /\bsafe to spend\b/, /\bsummary\b/, /\bhow much (is |do i have )?left\b/, /\bthis cycle\b/],
  },
];

const WEEK = [/\bminggu ini\b/, /\b7 hari\b/, /\bthis week\b/];
const QUESTION_START = /^(berapa|apa|apakah|kapan|gimana|bagaimana|how|what|when)\b/;

/**
 * Trimmed, lower-cased (Indonesian rules), whitespace collapsed, trailing punctuation dropped except "?",
 * and cut at the capture limit.
 */
export function normaliseText(text: string): string {
  const collapsed = text.slice(0, TANYA_TEXT_LIMIT).toLocaleLowerCase("id-ID").replace(/\s+/g, " ").trim();
  return collapsed.replace(/[.,!;:]+$/, "").trim();
}

export function matchIntent(normalised: string): IntentMatch | null {
  for (const entry of INTENT_TABLE) {
    if (!entry.patterns.some((pattern) => pattern.test(normalised))) continue;
    if (entry.intent === "bills") return { intent: "bills", range: WEEK.some((pattern) => pattern.test(normalised)) ? "week" : "window" };
    return { intent: entry.intent };
  }
  return null;
}

/** A question is never recorded, even when it holds a number ("berapa keluar 2 hari ini?"). */
export function looksLikeQuestion(normalised: string): boolean {
  return normalised.endsWith("?") || QUESTION_START.test(normalised);
}

export function hasDigit(normalised: string): boolean {
  return /\d/.test(normalised);
}
