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
  // Beranda's written summary sentence (option F narrative).
  "siklus gajimu{who}. Sejak gajian{keluar.total ? \" kamu keluar \" : \" \"}",
  "Bulan ini{keluar.total ? \" kamu keluar \" : \" \"}",
  "belum bisa aku hitung",
  "hal nunggu jawabanmu (",
  "Semua transaksi udah beres, nggak ada yang nunggu jawaban.",
  // The connect-phone guide's prose.
  "HP-mu siap! Mulai sekarang transaksi tercatat sendiri.",
  "Tenang, aplikasi di HP-mu bakal nunjukin cara benerinnya.",
  "Biar nggak dimatiin diam-diam",
  "Aku berhenti ngecek dulu. Lanjutin di aplikasi HP-mu, ya.",
];

const ROOTS = ["components/workspace", "components/beranda", "components/review", "components/transactions", "components/capture",
  "components/ui/DeviceHealthChecklist.tsx", "components/ui/DevicePairingDialog.tsx", "components/ui/DevicePairingSection.tsx",
  "app/ledger/page.tsx", "app/review/page.tsx", "app/page.tsx", "lib/deviceHealth.ts", "lib/reviewUndo.ts", "lib/triage.ts",
  "lib/provenance.ts", "lib/ledgerFilters.ts", "lib/historySuggestions.ts",
  // Analisis and Laporan (redesign-analysis-and-reports D13).
  "components/analysis", "components/report", "components/insights", "app/insights/page.tsx", "app/laporan/page.tsx",
  "lib/reportLead.ts"];

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

  it("writes Analisis and Laporan impersonally, without the formal Anda", () => {
    const roots = ["components/analysis", "components/report", "components/insights", "app/insights/page.tsx", "app/laporan/page.tsx", "lib/reportLead.ts"];
    const analysis = roots.flatMap(sources).filter((file) => /\.tsx?$/.test(file) && !/\.test\.tsx?$/.test(file));
    const formal = analysis.flatMap((file) => visibleLines(file)
      .filter(({ text }) => /\bAnda\b/.test(text))
      .map(({ line, text }) => `${path.relative(__dirname, file)}:${line}: ${text.trim()}`));
    expect(formal).toEqual([]);
  });

  it("catches a planted informal label", () => {
    const planted = path.join(__dirname, "copyRegister.fixture.txt");
    expect(SLANG.test('label="Udah dicek"')).toBe(true);
    expect(SLANG.test('label="Sudah dicek"')).toBe(false);
    expect(offenders([planted])).toEqual(['copyRegister.fixture.txt:2: <button type="button">Udah dicek</button>']);
  });
});
