import { describe, expect, it } from "vitest";
import { TANYA_TEXT_LIMIT, hasDigit, looksLikeQuestion, matchIntent, normaliseText, type IntentName } from "./intents";

const PHRASES: Record<IntentName, string[]> = {
  cycle: [
    "aman hari ini berapa", "sisa uang", "ringkasan siklus ini", "posisi keuangan", "bisa pakai berapa hari ini",
    "Ringkasan siklus ini", "safe to spend", "summary please", "how much is left", "this cycle", "berapa sisa?",
  ],
  waiting: [
    "yang nunggu jawaban", "menunggu jawaban (3)", "ada yang perlu dijawab", "tinjau", "yang belum dikenali",
    "Menunggu jawaban", "what is waiting", "anything to review", "pending questions",
  ],
  bills: [
    "tagihan bulan ini", "langganan apa aja", "cicilan laptop", "yang jatuh tempo", "setoran tabungan", "Tagihan minggu ini",
    "upcoming bills", "what is due", "upcoming payments", "my subscriptions",
  ],
  kakeibo: [
    "kakeibo", "pembagian pilar", "kebutuhan vs keinginan", "budaya bulan ini", "pengeluaran tak terduga",
    "ke mana aja uangku", "kemana uang", "pillars", "needs and wants", "where did my money go",
  ],
  report: ["laporan terbaru", "Laporan terbaru", "rapor bulanan", "mana laporan bulan lalu", "monthly report", "show the report", "reports"],
  help: ["bantuan", "kamu bisa apa", "contoh", "help", "what can you do", "Bantuan dong"],
};

describe("intent table", () => {
  for (const [intent, phrases] of Object.entries(PHRASES) as [IntentName, string[]][]) {
    it(`matches every listed ${intent} phrase`, () => {
      expect(phrases.length).toBeGreaterThanOrEqual(6);
      for (const phrase of phrases) expect(matchIntent(normaliseText(phrase))?.intent, phrase).toBe(intent);
    });
  }

  it("matches nothing for near misses, which then reach capture or the unknown reply", () => {
    for (const phrase of ["bayar listrik 200rb", "kopi 18rb", "halo", "parkir tadi", "makan siang 35rb", "gaji masuk 8jt", "terima kasih"]) {
      expect(matchIntent(normaliseText(phrase)), phrase).toBeNull();
    }
  });

  it("sends the more specific question to its own intent before the cycle words", () => {
    expect(matchIntent(normaliseText("kakeibo siklus ini"))?.intent).toBe("kakeibo");
    expect(matchIntent(normaliseText("tagihan siklus ini"))?.intent).toBe("bills");
    expect(matchIntent(normaliseText("laporan siklus ini"))?.intent).toBe("report");
  });

  it("asks bills for the next 7 days only when the text says so", () => {
    expect(matchIntent(normaliseText("Tagihan minggu ini"))).toEqual({ intent: "bills", range: "week" });
    expect(matchIntent(normaliseText("tagihan 7 hari"))).toEqual({ intent: "bills", range: "week" });
    expect(matchIntent(normaliseText("bills this week"))).toEqual({ intent: "bills", range: "week" });
    expect(matchIntent(normaliseText("tagihan"))).toEqual({ intent: "bills", range: "window" });
  });

  it("normalises case, spacing and trailing punctuation but keeps a question mark", () => {
    expect(normaliseText("  RINGKASAN   Siklus\tini!! ")).toBe("ringkasan siklus ini");
    expect(normaliseText("berapa sisa?")).toBe("berapa sisa?");
    expect(normaliseText("Tagihan minggu ini.")).toBe("tagihan minggu ini");
    expect(normaliseText("   ")).toBe("");
  });

  it("cuts the text at the capture limit", () => {
    expect(normaliseText("a".repeat(500))).toHaveLength(TANYA_TEXT_LIMIT);
  });

  it("treats a trailing question mark or a question word as a question", () => {
    for (const phrase of ["berapa keluar 2 hari ini?", "berapa keluar 2 hari ini", "apa saja 3 tagihan", "kapan gaji", "gimana 5rb", "how much 20k", "parkir 5rb?"]) {
      expect(looksLikeQuestion(normaliseText(phrase)), phrase).toBe(true);
    }
    for (const phrase of ["parkir 5rb jam 2", "kopi 18rb tadi pagi", "apartemen 2jt"]) {
      expect(looksLikeQuestion(normaliseText(phrase)), phrase).toBe(false);
    }
  });

  it("notices a digit for the mixed-text chip", () => {
    expect(hasDigit("tagihan listrik 200rb")).toBe(true);
    expect(hasDigit("tagihan listrik")).toBe(false);
  });

  it("matches a 200-character text in under 1 ms", () => {
    const text = normaliseText(`${"catatan panjang tanpa kata kunci ".repeat(10)}xyz`);
    const runs = 2000;
    const start = performance.now();
    for (let i = 0; i < runs; i++) matchIntent(text);
    expect((performance.now() - start) / runs).toBeLessThan(1);
  });
});
