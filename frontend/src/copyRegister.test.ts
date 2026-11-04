import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

/**
 * The copy register (redesign-transactions-workspace D20): labels, headings, buttons, statuses, toasts,
 * errors, empty states and explanations are neutral standard Indonesian. Only the assistant's own
 * conversation keeps the casual voice; those strings are listed here one by one.
 */
const SLANG = /\b(saranku|buat baru|buat diterima|buat mulai|nggak|ngga|gak|udah|beres|nangkep|ketangkep|nyambung|nyambungin|bikin|aja|banget|dimatiin|kamu|aku|nunggu|gajian|lagi bikin|cuma|nebak|dobel|ketemu|benerin|lanjutin|nuntun|tunjukin|ngecek)\b|\b\w+-mu\b/i;

const CONVERSATION = [
  // Beranda's hero figure as the owner approved it in the preview (refine-workspace-layout D5, OD96).
  "Aman dipakai sampai gajian",
  // The connect-phone guide's prose.
  "HP-mu siap! Mulai sekarang transaksi tercatat sendiri.",
  "Tenang, aplikasi di HP-mu bakal nunjukin cara benerinnya.",
  "Biar nggak dimatiin diam-diam",
  "Aku berhenti ngecek dulu. Lanjutin di aplikasi HP-mu, ya.",
];

const MONEY_ROOTS = ["components/accounts", "components/goals", "components/settings", "app/accounts", "app/goals", "app/settings",
  "app/auth/login/page.tsx", "components/ui/InvestmentTradeModal.tsx", "components/ui/InvestmentTopupModal.tsx",
  "components/ui/AmountTrackingSwitchModal.tsx", "components/ui/ReconciliationFlag.tsx", "components/ui/DebtAllocationEditor.tsx",
  "components/ui/SalarySettingsSection.tsx", "components/ui/SenderAliasesSection.tsx", "components/recurring/RecurringRulesModal.tsx",
  "components/dashboard/EmergencyCoverage.tsx", "lib/habits.ts"];

const ROOTS = ["components/workspace", "components/beranda", "components/review", "components/transactions", "components/capture",
  "components/ui/DeviceHealthChecklist.tsx", "components/ui/DevicePairingDialog.tsx", "components/ui/DevicePairingSection.tsx",
  "app/ledger/page.tsx", "app/review/page.tsx", "app/page.tsx", "lib/deviceHealth.ts", "lib/reviewUndo.ts", "lib/triage.ts",
  "lib/provenance.ts", "lib/ledgerFilters.ts", "lib/historySuggestions.ts",
  "lib/capture.ts",
  // Analisis and Laporan (redesign-analysis-and-reports D13).
  "components/analysis", "components/report", "components/insights", "app/insights/page.tsx", "app/laporan/page.tsx",
  "lib/reportLead.ts",
  // Rekening & Kantong, Target & Tagihan, Pengaturan, their modals and the sign-in page (redesign-money-and-settings D16).
  ...MONEY_ROOTS];

function sources(entry: string): string[] {
  const full = path.join(__dirname, entry);
  if (statSync(full).isFile()) return [full];
  return readdirSync(full).flatMap((name) => sources(path.join(entry, name)));
}

/** Lines of code with user-visible text; comments are left out. */
function visibleLines(file: string): { line: number; text: string }[] {
  return readFileSync(file, "utf8").split("\n").map((text, index) => ({ line: index + 1, text })).filter(({ text }) => {
    const trimmed = text.trim();
    return trimmed && !trimmed.startsWith("//") && !trimmed.startsWith("*") && !trimmed.startsWith("/*") && !trimmed.startsWith("{/*");
  });
}

function offenders(files: string[]): string[] {
  return files.flatMap((file) => visibleLines(file)
    .filter(({ text }) => SLANG.test(text.replace(/\/\/.*$/, "")) && !CONVERSATION.some((allowed) => text.includes(allowed)))
    .map(({ line, text }) => `${path.relative(__dirname, file)}:${line}: ${text.trim()}`));
}

describe("copy register", () => {
  const files = ROOTS.flatMap(sources).filter((file) => /\.tsx?$/.test(file) && !/\.test\.tsx?$/.test(file));

  it("keeps slang out of every label-type string outside the assistant's conversation", () => {
    expect(files.length).toBeGreaterThan(20);
    expect(offenders(files)).toEqual([]);
  });

  // Spending Terms (refine-workspace-layout D4): "Pengeluaran" and "Ditabung & investasi" name the figures on the
  // pages this change owns; "Keluar" and "Belanja" are no longer figure labels there. Analisis and Laporan follow
  // in compare-whole-months. "Keluar" as sign-out and "Uang keluar" in row text are not figure labels.
  it("names spending figures Pengeluaran, never Keluar or Belanja, on Beranda, Transaksi, Tinjau and the money pages", () => {
    const roots = ["components/beranda", "components/transactions", "components/review", "components/capture", "components/accounts",
      "components/goals", "app/ledger/page.tsx", "app/page.tsx"];
    const owned = roots.flatMap(sources).filter((file) => /\.tsx?$/.test(file) && !/\.test\.tsx?$/.test(file));
    const figureLabel = /(label|title|text)(=|: )["`](Keluar|Belanja)\b|<dt>(Keluar|Belanja)\b|>(Keluar|Belanja)<\/button|"(Keluar|Belanja) siklus/;
    const found = owned.flatMap((file) => visibleLines(file)
      .filter(({ text }) => figureLabel.test(text))
      .map(({ line, text }) => `${path.relative(__dirname, file)}:${line}: ${text.trim()}`));
    expect(found).toEqual([]);
  });

  it("writes Analisis and Laporan impersonally, without the formal Anda", () => {
    const roots = ["components/analysis", "components/report", "components/insights", "app/insights/page.tsx", "app/laporan/page.tsx", "lib/reportLead.ts"];
    const analysis = roots.flatMap(sources).filter((file) => /\.tsx?$/.test(file) && !/\.test\.tsx?$/.test(file));
    const formal = analysis.flatMap((file) => visibleLines(file)
      .filter(({ text }) => /\bAnda\b/.test(text))
      .map(({ line, text }) => `${path.relative(__dirname, file)}:${line}: ${text.trim()}`));
    expect(formal).toEqual([]);
  });

  it("writes the money pages, Pengaturan and their modals impersonally, without the formal Anda", () => {
    const money = MONEY_ROOTS.flatMap(sources).filter((file) => /\.tsx?$/.test(file) && !/\.test\.tsx?$/.test(file));
    const formal = money.flatMap((file) => visibleLines(file)
      .filter(({ text }) => /\bAnda\b/.test(text))
      .map(({ line, text }) => `${path.relative(__dirname, file)}:${line}: ${text.trim()}`));
    expect(formal).toEqual([]);
    expect(SLANG.test("Anda") || /\bAnda\b/.test('label="Rekening Anda"')).toBe(true);
  });

  it("catches a planted informal label", () => {
    const planted = path.join(__dirname, "copyRegister.fixture.txt");
    expect(SLANG.test('label="Udah dicek"')).toBe(true);
    expect(SLANG.test('label="Sudah dicek"')).toBe(false);
    expect(offenders([planted])).toEqual(['copyRegister.fixture.txt:2: <button type="button">Udah dicek</button>']);
  });
});
