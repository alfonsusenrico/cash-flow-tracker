import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import ts from "typescript";
import { describe, expect, it } from "vitest";
import en from "@/messages/en.json";
import id from "@/messages/id.json";

/**
 * The copy register, one per language (ui-copy-register, add-language-system D16). Translated areas are checked in
 * their catalogues: labels, headings, buttons, statuses, toasts, errors, empty states and explanations are neutral
 * standard Indonesian and plain English, and only the `conversation` namespace may use the conversational voice.
 * Areas not translated yet are still checked in their sources, and translated sources must hold no literal copy.
 */
type Catalogue = { [key: string]: string | Catalogue };

const SLANG_ID = /\b(saranku|buat baru|buat diterima|buat mulai|nggak|ngga|gak|udah|beres|nangkep|ketangkep|nyambung|nyambungin|bikin|aja|banget|dimatiin|kamu|aku|nunggu|gajian|lagi bikin|cuma|nebak|dobel|ketemu|benerin|lanjutin|nuntun|tunjukin|ngecek)\b|\b\w+-mu\b/i;
const SLANG_EN = /\b(gonna|wanna|gotta|yep|nope|kinda|sorta|stuff|awesome|oops|hey|ok|okay)\b/i;
const JARGON_EN = /\b(outflow|inflow|ledger|reconcil\w*|liabilit\w*|runway|cost basis)\b/i;
const YOU_EN = /\b(you|your|yours|you're)\b/i;
/** "Spend" as a noun and "Expenses" or "Spent" never name a figure; the verb in "safe to spend" is allowed. */
const SPEND_EN = /\b(expenses?|spent)\b|(?<!to )\bspend\b/i;
/** Indonesian spending figures are "Pengeluaran"; "Keluar ke …" and "Keluar dari …" are a movement's direction, not a figure. */
const SPENDING_ID = /\b(Keluar(?! (ke|dari) )|Belanja)\b/;
/** "Keluar" in Peta Aliran's node detail is one account's direction (money in, money out), not a spending figure. */
const DIRECTION_KEYS = new Set(["analysis.flowDetail.out", "analysis.flowDetail.outTo"]);
/** Laporan's third question asks how much was spent; it is a question, not the spending figure's name (apply-markas-laporan D19). */
const QUESTION_KEYS = new Set(["report.questions.spending"]);

/**
 * "Aman dipakai sampai gajian" and the two phrases built on it are the hero figure's name as the owner approved it
 * (refine-workspace-layout OD96, apply-markas-beranda D3), so their "gajian" is allowed outside the conversation.
 */
const APPROVED_ID_KEYS = new Set(["terms.safeToSpendUntilPayday", "home.breakdownButton", "home.daysUntilPayday"]);

function messages(catalogue: Catalogue, prefix = ""): [string, string][] {
  return Object.entries(catalogue).flatMap(([key, value]) => {
    const keyPath = prefix ? `${prefix}.${key}` : key;
    return typeof value === "string" ? [[keyPath, value] as [string, string]] : messages(value, keyPath);
  });
}

const inConversation = (key: string) => key.startsWith("conversation.");

function indonesianOffenders(catalogue: Catalogue): string[] {
  return messages(catalogue)
    .filter(([key, value]) => !inConversation(key) && !APPROVED_ID_KEYS.has(key) && SLANG_ID.test(value))
    .map(([key]) => key);
}

function englishOffenders(catalogue: Catalogue): string[] {
  return messages(catalogue)
    .filter(([key]) => !inConversation(key))
    .flatMap(([key, value]) => {
      const found: string[] = [];
      if (SLANG_EN.test(value)) found.push(`${key}: slang`);
      if (JARGON_EN.test(value)) found.push(`${key}: jargon`);
      if (YOU_EN.test(value)) found.push(`${key}: you`);
      return found;
    });
}

/**
 * The glossary and the messages of Tinjau, Transaksi, Analisis and Laporan in both languages (apply-markas-tinjau D18,
 * apply-markas-analisis D22, apply-markas-laporan D19); in Indonesian also Beranda's and the Rincian's messages, which
 * hold their figure labels.
 */
function spendingTermOffenders(catalogue: Catalogue, language: "en" | "id"): string[] {
  const rule = language === "en" ? SPEND_EN : SPENDING_ID;
  return messages(catalogue)
    .filter(([key]) => /^(terms|review|transactions|analysis|report)\./.test(key) || (language === "id" && /^(home|breakdown)\./.test(key)))
    .filter(([key, value]) => rule.test(value) && !(language === "id" && DIRECTION_KEYS.has(key)) && !QUESTION_KEYS.has(key))
    .map(([key]) => key);
}

function normalizedTerm(value: string): string {
  return value
    .toLowerCase()
    .replace(/\s+and\s+/g, " & ")
    .split(/\s+/)
    .map((word) => word.replace(/ies$/, "y").replace(/(?<!s)s$/, ""))
    .join(" ");
}

/** Two glossary names that differ only by letter case, a plural or "&" against "and". */
function nearDuplicateTerms(catalogue: Catalogue): string[] {
  const terms = messages((catalogue.terms as Catalogue) ?? {});
  const seen = new Map<string, [string, string]>();
  const found: string[] = [];
  for (const [key, value] of terms) {
    const normal = normalizedTerm(value);
    const earlier = seen.get(normal);
    if (!earlier) seen.set(normal, [key, value]);
    else if (earlier[1] !== value) found.push(`terms.${earlier[0]} ~ terms.${key}`);
  }
  return found;
}

function sources(entry: string): string[] {
  const full = path.join(__dirname, entry);
  if (statSync(full).isFile()) return [full];
  return readdirSync(full).flatMap((name) => sources(path.join(entry, name)));
}

const isSource = (file: string) => /\.tsx?$/.test(file) && !/\.test\.tsx?$/.test(file) && !/\.d\.json\.ts$/.test(file);

/** Lines of code with user-visible text; comments are left out. */
function visibleLines(file: string): { line: number; text: string }[] {
  return readFileSync(file, "utf8").split("\n").map((text, index) => ({ line: index + 1, text })).filter(({ text }) => {
    const trimmed = text.trim();
    return trimmed && !trimmed.startsWith("//") && !trimmed.startsWith("*") && !trimmed.startsWith("/*") && !trimmed.startsWith("{/*");
  });
}

/** The conversation strings still written in sources not translated yet: the connect-phone guide's prose. */
const SOURCE_CONVERSATION = [
  "HP-mu siap! Mulai sekarang transaksi tercatat sendiri.",
  "Tenang, aplikasi di HP-mu bakal nunjukin cara benerinnya.",
  "Biar nggak dimatiin diam-diam",
  "Aku berhenti ngecek dulu. Lanjutin di aplikasi HP-mu, ya.",
];

function sourceOffenders(files: string[]): string[] {
  return files.flatMap((file) => visibleLines(file)
    .filter(({ text }) => SLANG_ID.test(text.replace(/\/\/.*$/, "")) && !SOURCE_CONVERSATION.some((allowed) => text.includes(allowed)))
    .map(({ line, text }) => `${path.relative(__dirname, file)}:${line}: ${text.trim()}`));
}

/** The areas a redesign phase has translated (add-language-system D10); every phase adds its own here. */
const TRANSLATED_ROOTS = [
  "components/workspace", "components/layout", "components/beranda", "app/page.tsx", "app/layout.tsx", "app/auth/login/page.tsx",
  "components/recurring/PendingScheduledBanner.tsx", "components/ui/PeriodStepper.tsx", "components/ui/Modal.tsx",
  "components/ui/ConfirmActionButton.tsx", "components/settings/TestSessionBanner.tsx", "i18n", "lib/i18n", "lib/format.ts",
  // Tinjau (apply-markas-tinjau D18).
  "components/review", "app/review", "lib/triage.ts", "lib/review.ts", "lib/reviewUndo.ts", "lib/historySuggestions.ts",
  // Transaksi (apply-markas-transaksi D20).
  "app/ledger", "components/transactions", "components/ui/ExpenseTopupConversionModal.tsx", "lib/ledgerFilters.ts", "lib/ledgerDays.ts",
  // Analisis (apply-markas-analisis D22).
  "app/insights", "components/analysis", "components/categories", "components/charts",
  // Rekening & Kantong and its money dialogs (apply-markas-rekening D16).
  "app/accounts", "components/accounts", "components/ui/InvestmentTradeModal.tsx", "components/ui/InvestmentTopupModal.tsx",
  "components/ui/AmountTrackingSwitchModal.tsx",
  // Laporan (apply-markas-laporan D19).
  "app/laporan", "components/report",
];
/** Indonesian page copy that lives in a translated root until its page's phase moves it (D10). */
const UNTRANSLATED_FILES = ["components/workspace/untranslatedPages.ts"];

const MONEY_ROOTS = ["components/goals", "components/settings", "app/goals", "app/settings", "components/ui/ReconciliationFlag.tsx", "components/ui/DebtAllocationEditor.tsx",
  "components/ui/SalarySettingsSection.tsx", "components/ui/SenderAliasesSection.tsx", "components/recurring/RecurringRulesModal.tsx",
  "components/dashboard/EmergencyCoverage.tsx", "lib/habits.ts"];

const UNTRANSLATED_ROOTS = ["components/capture",
  "components/ui/DeviceHealthChecklist.tsx", "components/ui/DevicePairingDialog.tsx", "components/ui/DevicePairingSection.tsx",
  "lib/deviceHealth.ts", "lib/provenance.ts", "lib/capture.ts", "lib/movementFormSchema.ts", "lib/untranslatedRowLabels.ts",
  // Rekening & Kantong, Target & Tagihan, Pengaturan and their modals (redesign-money-and-settings D16).
  ...MONEY_ROOTS];

/** Attributes and properties whose string is shown or announced. */
const VISIBLE_NAMES = /^(aria-label|aria-description|title|placeholder|alt|label|heading|meta|subtitle|closeLabel|menuLabel|caption|emptyLabel|line|text|detail|help|rule)$/;
/** Literal values that are not copy: key names, a directive and the product name. */
const NOT_COPY = new Set(["use client", "Ctrl K", "Meta+K Control+K", "CashFlow", "English", "Bahasa Indonesia"]);
/** A class list, a CSS value or a query: every token is lower-case code-like, and one has a separator. */
function looksLikeCode(text: string): boolean {
  const tokens = text.trim().split(/\s+/);
  return tokens.every((token) => /^[!-]?[a-z0-9\-:/_.[\]()%#&>=*+,'"]*$/.test(token)) && tokens.some((token) => /[-:[\]()=]/.test(token));
}

/** Literal copy in a source: JSX text, visible attributes and properties, and prose-like strings (D10, D16). */
function literalCopy(fileName: string, source: string): string[] {
  const file = ts.createSourceFile(fileName, source, ts.ScriptTarget.Latest, true, fileName.endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS);
  const found: string[] = [];
  const report = (node: ts.Node, text: string) => {
    const { line } = file.getLineAndCharacterOfPosition(node.getStart());
    found.push(`${fileName}:${line + 1}: ${text.trim().slice(0, 80)}`);
  };
  const isDeveloperText = (node: ts.Node): boolean => {
    for (let parent = node.parent; parent; parent = parent.parent) {
      if (ts.isNewExpression(parent) && /Error$/.test(parent.expression.getText())) return true;
      if (ts.isCallExpression(parent) && /^console\./.test(parent.expression.getText())) return true;
      if (ts.isImportDeclaration(parent) || ts.isExportDeclaration(parent)) return true;
    }
    return false;
  };
  const visit = (node: ts.Node) => {
    if (ts.isJsxText(node) && /[A-Za-zÀ-ÿ]{2,}/.test(node.getText()) && !NOT_COPY.has(node.getText().trim())) report(node, node.getText());
    if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
      const text = node.text;
      const parent = node.parent;
      const name =
        (ts.isJsxAttribute(parent) && parent.name.getText()) ||
        (ts.isPropertyAssignment(parent) && parent.initializer === node && parent.name.getText()) ||
        null;
      const letters = /[A-Za-zÀ-ÿ]{2,}/.test(text);
      const isKeyArgument = ts.isCallExpression(parent) && /(^|\.)(t|rich|useTranslations|getTranslations)$/.test(parent.expression.getText());
      const visible = name !== null && VISIBLE_NAMES.test(name);
      const prose = /[A-Za-zÀ-ÿ]{2,}\s+[A-Za-zÀ-ÿ]{2,}/.test(text) && !looksLikeCode(text);
      if (letters && !isKeyArgument && !NOT_COPY.has(text) && !isDeveloperText(node) && (visible || prose)) report(node, text);
    }
    // A template is copy when its fixed text has words; key paths, routes, queries and cookies are not.
    const keyTemplate = ts.isTemplateExpression(node) && ts.isCallExpression(node.parent) && /(^|\.)(t|rich)$/.test(node.parent.expression.getText());
    if (ts.isTemplateExpression(node) && !keyTemplate && !isDeveloperText(node)) {
      const parts = [node.head.text, ...node.templateSpans.map((span) => span.literal.text)].join(" ");
      const code = /[/=?#]/.test(parts) || looksLikeCode(parts);
      if (/[A-Za-zÀ-ÿ]{3,}/.test(parts.replace(/\b(Rp|ID|US)\b/g, "")) && !code) report(node, parts);
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  return found;
}

describe("catalogue copy register", () => {
  const idCatalogue = id as unknown as Catalogue;
  const enCatalogue = en as unknown as Catalogue;

  it("keeps Indonesian slang out of every message outside the conversation namespace", () => {
    expect(indonesianOffenders(idCatalogue)).toEqual([]);
  });

  it("keeps English labels plain: no slang, no finance jargon, no you or your outside the conversation", () => {
    expect(englishOffenders(enCatalogue)).toEqual([]);
  });

  it("names the spending figure Spending and Pengeluaran in both glossaries", () => {
    expect((enCatalogue.terms as Catalogue).spending).toBe("Spending");
    expect((idCatalogue.terms as Catalogue).spending).toBe("Pengeluaran");
    expect(spendingTermOffenders(enCatalogue, "en")).toEqual([]);
    expect(spendingTermOffenders(idCatalogue, "id")).toEqual([]);
  });

  it("has one name per concept: no near-duplicate glossary terms", () => {
    expect(nearDuplicateTerms(enCatalogue)).toEqual([]);
    expect(nearDuplicateTerms(idCatalogue)).toEqual([]);
  });

  it("keeps Beranda's approved Indonesian labels, lines and captions in the catalogue", () => {
    const values = messages(idCatalogue).map(([, value]) => value).join("\n");
    const BERANDA_STRINGS = [
      "Tidak ada yang perlu dijawab.", "Perlu dipilih di Tinjau", "Lihat transaksi ", "hari lagi sampai gajian",
      "Tanggal gaji berikutnya belum diketahui", "Gaji baru belum tercatat", "Sumber gaji belum diatur", "Siklus gaji belum ada",
      "sudah lebih", "Tabungan bertambah", "Tabungan berkurang", "termasuk setoran", "Lebih dari uang masuk", "sejak gaji", "Buka Tinjau",
      "Belum ada transaksi hari ini dan kemarin.", "Daftar Tinjau belum bisa dimuat.", "Aktivitas belum bisa dimuat.",
      "Angka Beranda belum dapat dimuat", "Belum bisa dihitung", "Menunggu jawaban", "Jawab di Tinjau",
      "Masuk dikurangi Pengeluaran, Ditabung & investasi dan tagihan terjadwal sampai akhir",
    ];
    expect(BERANDA_STRINGS.filter((label) => !values.includes(label))).toEqual([]);
  });

  it("fails on one planted line per language and names the key", () => {
    const planted = JSON.parse(readFileSync(path.join(__dirname, "copyRegister.fixture.json"), "utf8")) as Record<string, Catalogue>;
    expect(indonesianOffenders(planted.id)).toEqual(["home.checked"]);
    expect(spendingTermOffenders(planted.enFigure, "en")).toEqual(["terms.expenses"]);
    expect(englishOffenders(planted.enYour)).toEqual(["home.yourMoney: you"]);
    expect(englishOffenders(planted.enLedger)).toEqual(["home.openLedger: jargon"]);
    expect(nearDuplicateTerms(planted.nearDuplicate)).toEqual(["terms.savedInvested ~ terms.savedAndInvested"]);
    expect(spendingTermOffenders(planted.idReview, "id")).toEqual(["review.hero.label"]);
    expect(spendingTermOffenders(planted.enReview, "en")).toEqual(["review.hero.label"]);
    expect(englishOffenders(planted.enReview)).toEqual(["review.answer.typeOwn: you"]);
    expect(spendingTermOffenders(planted.idTransactions, "id")).toEqual(["transactions.hero.label"]);
    expect(spendingTermOffenders(planted.enTransactions, "en")).toEqual(["transactions.hero.label"]);
    expect(englishOffenders(planted.enTransactions)).toEqual(["transactions.page.title: jargon"]);
    expect(spendingTermOffenders(planted.idAnalysis, "id")).toEqual(["analysis.daily.title"]);
    expect(spendingTermOffenders(planted.enAnalysis, "en")).toEqual(["terms.spent"]);
    expect(spendingTermOffenders(planted.idReport, "id")).toEqual(["report.hero.label"]);
    expect(spendingTermOffenders(planted.enReport, "en")).toEqual(["terms.expenses", "report.history.title"]);
  });
});

describe("translated sources", () => {
  const translated = TRANSLATED_ROOTS.flatMap(sources).filter(isSource);
  const literalFiles = translated.filter((file) => !UNTRANSLATED_FILES.includes(path.relative(__dirname, file)));

  it("hold no literal copy: every word comes from the catalogue", () => {
    expect(literalFiles.length).toBeGreaterThan(30);
    const found = literalFiles.flatMap((file) => literalCopy(path.relative(__dirname, file), readFileSync(file, "utf8")));
    expect(found).toEqual([]);
  });

  it("catch a planted literal label in a translated root", () => {
    const planted = 'export function Planted() {\n  return <button aria-label="Tutup">Catat transaksi</button>;\n}\nconst caption = `sejak ${start}`;\n';
    expect(literalCopy("Planted.tsx", planted)).toEqual(["Planted.tsx:2: Tutup", "Planted.tsx:2: Catat transaksi", "Planted.tsx:4: sejak"]);
    expect(literalCopy("Keys.tsx", 'const label = t("home.needsAction");\nconst cls = "ws-btn ws-btn-quiet";\n')).toEqual([]);
  });

  // Glossary (D4): every Beranda figure label is a `terms.*` key, so a figure has exactly one name.
  const FIGURE_LABEL = /<(FigureBlock|HeroFigure|PanelBlock)\b[^>]*?\s(label|heading)=\{t\("([^"]+)"\)/g;

  function figureKeys(source: string): string[] {
    return [...source.matchAll(FIGURE_LABEL)].map((match) => match[3]);
  }

  it("labels every Beranda figure with a glossary term", () => {
    const beranda = sources("components/beranda").filter(isSource).map((file) => readFileSync(file, "utf8")).join("\n");
    const keys = figureKeys(beranda);
    expect(keys.length).toBeGreaterThanOrEqual(6);
    expect(keys.filter((key) => !key.startsWith("terms."))).toEqual([]);
    expect(figureKeys('<FigureBlock label={t("home.needsAction")} figure={x} />')).toEqual(["home.needsAction"]);
  });
});

describe("source copy register for areas not translated yet", () => {
  const files = UNTRANSLATED_ROOTS.flatMap(sources).filter(isSource);

  it("keeps slang out of every label-type string outside the assistant's conversation", () => {
    expect(files.length).toBeGreaterThan(20);
    expect(sourceOffenders(files)).toEqual([]);
  });

  // Spending Terms (refine-workspace-layout D4): "Pengeluaran" and "Ditabung & investasi" name the figures; "Keluar"
  // and "Belanja" are no longer figure labels. "Keluar" as sign-out and "Uang keluar" in row text are not figure labels.
  it("names spending figures Pengeluaran, never Keluar or Belanja, on Transaksi, Tinjau and the money pages", () => {
    const roots = ["components/transactions", "components/capture", "components/accounts",
      "components/goals", "app/ledger/page.tsx"];
    const owned = roots.flatMap(sources).filter(isSource);
    const figureLabel = /(label|title|text)(=|: )["`](Keluar|Belanja)\b|<dt>(Keluar|Belanja)\b|>(Keluar|Belanja)<\/button|"(Keluar|Belanja) siklus/;
    const found = owned.flatMap((file) => visibleLines(file)
      .filter(({ text }) => figureLabel.test(text))
      .map(({ line, text }) => `${path.relative(__dirname, file)}:${line}: ${text.trim()}`));
    expect(found).toEqual([]);
  });

  it("writes the money pages, Pengaturan and their modals impersonally, without the formal Anda", () => {
    const money = MONEY_ROOTS.flatMap(sources).filter(isSource);
    const formal = money.flatMap((file) => visibleLines(file)
      .filter(({ text }) => /\bAnda\b/.test(text))
      .map(({ line, text }) => `${path.relative(__dirname, file)}:${line}: ${text.trim()}`));
    expect(formal).toEqual([]);
    expect(/\bAnda\b/.test('label="Rekening Anda"')).toBe(true);
  });

  it("catches a planted informal label", () => {
    const planted = path.join(__dirname, "copyRegister.fixture.txt");
    expect(SLANG_ID.test('label="Udah dicek"')).toBe(true);
    expect(SLANG_ID.test('label="Sudah dicek"')).toBe(false);
    expect(sourceOffenders([planted])).toEqual(['copyRegister.fixture.txt:2: <button type="button">Udah dicek</button>']);
  });
});
