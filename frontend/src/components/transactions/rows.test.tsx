import { readFileSync } from "node:fs";
import path from "node:path";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { testWords } from "@/lib/i18n/testing";
import { GENERIC_ROW_NOTES, PLACEHOLDER_NOTES, isPlaceholderNote } from "@/lib/placeholderNotes";
import {
  KindAmount, RowAmount, creditPurchaseLine, movementDirection, movementRoute, routeIncomplete, rowCategory, rowKind, rowLabelsFor,
  rowMeta, rowPhoneLine, rowStatus, rowTitle,
} from "./rows";
import type { RowFacts } from "./types";

vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ hideBalances: false, bal: (amount: number) => `Rp ${amount}` }) }));
vi.mock("next-intl", async (original) => ({ ...(await original<typeof import("next-intl")>()), useTranslations: () => (key: string) => key, useLocale: () => "id" }));

const id = testWords("id");
const en = testWords("en");
const labels = rowLabelsFor(id.t);
const english = rowLabelsFor(en.t);

// Synthetic accounts: "Main Pocket" and "Tabungan" are pockets of Jago; "ATM" is a pocket of BCA; Gopay has no parent.
// The owner renamed the movement category to "Transfer": the key, not the name, makes a row a movement.
function leg(extra: Partial<RowFacts>): RowFacts {
  return {
    id: "row", account_id: "main", account_name: "Main Pocket", account_parent_name: "Jago", category_name: "Transfer",
    category_system_key: "internal_movement",
    type: "expense", amount: 500_000, date: "2026-11-04T01:39:00+07:00", notes: null, ...extra,
  };
}

const outboundLeg = leg({
  movement_id: "mv", movement_role: "outbound", account_name: "ATM", account_parent_name: "BCA",
  transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null,
});
const inboundLeg = leg({
  type: "income", movement_id: "mv", movement_role: "inbound", account_name: "Tabungan", account_parent_name: "Jago",
  transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null,
});
const spend = leg({ category_name: "Makan di Luar", category_system_key: null, merchant: "Kedai Awan", kakeibo_type: "want", review_status: "reviewed" });

describe("movementRoute", () => {
  it("reads a merged row from its own account to the target", () => {
    const merged = { ...outboundLeg, is_consolidated_transfer: true, target_account_name: "Gopay", target_account_parent_name: null };
    expect(movementRoute(merged)).toEqual({ source: "BCA · ATM", target: "Gopay" });
    expect(rowTitle(merged, labels)).toBe("BCA · ATM → Gopay");
  });

  it("reads an inferred pair the same way, with the target's bank", () => {
    const inferred = leg({ is_inferred_transfer: true, target_account_name: "Tabungan", target_account_parent_name: "Jago" });
    expect(rowTitle(inferred, labels)).toBe("Jago · Main Pocket → Jago · Tabungan");
  });

  it("reads an inbound leg from its partner to its account, not backwards", () => {
    expect(movementRoute(inboundLeg)).toEqual({ source: "Gopay", target: "Jago · Tabungan" });
    expect(rowTitle(inboundLeg, labels)).toBe("Gopay → Jago · Tabungan");
  });

  it("leaves the unknown side of an unpaired leg open, by its type", () => {
    const income = leg({ type: "income" });
    expect(movementRoute(income)).toEqual({ source: null, target: "Jago · Main Pocket" });
    expect(rowTitle(income, labels)).toBe("? → Jago · Main Pocket");
    expect(routeIncomplete(income)).toBe(true);
  });

  it("keeps a row from an older API without parent fields readable", () => {
    const old = leg({ movement_id: "mv", movement_role: "inbound", type: "income", account_parent_name: undefined, transfer_target_account_name: "Gopay" });
    expect(rowTitle(old, labels)).toBe("Gopay → Main Pocket");
  });
});

describe("movementDirection", () => {
  it("says where the money came from or went in an account's own list, and when the partner is missing", () => {
    expect(movementDirection(inboundLeg, labels)).toBe("Masuk dari Gopay");
    expect(movementDirection(outboundLeg, labels)).toBe("Keluar ke Gopay");
    expect(movementDirection(leg({ type: "income" }), labels)).toBe("Pasangan belum ditemukan");
    expect(movementDirection(outboundLeg, english)).toBe("Out to Gopay");
  });
});

describe("transfer titles (owner feedback 2)", () => {
  it("titles a transfer by its note with the route as meta, and by its route without a note", () => {
    const noted = { ...outboundLeg, notes: "Isi GoPay" };
    expect(rowTitle(noted, labels)).toBe("Isi GoPay");
    expect(rowMeta(noted, labels, id.format)).toEqual({ text: "BCA · ATM → Gopay" });
    expect(rowPhoneLine(noted, labels, id.format)).toEqual({ text: "BCA · ATM → Gopay" });
    expect(rowTitle(outboundLeg, labels)).toBe("BCA · ATM → Gopay");
    expect(rowMeta(outboundLeg, labels, id.format)).toEqual({ text: "" });
  });

  it("treats the server's placeholder note as no note", () => {
    for (const note of PLACEHOLDER_NOTES) {
      expect(rowTitle({ ...outboundLeg, notes: note }, labels)).toBe("BCA · ATM → Gopay");
    }
    expect(isPlaceholderNote("  ")).toBe(true);
    expect(isPlaceholderNote("Pindah saldo ke GoPay")).toBe(false);
  });

  it("matches the notes the backend writes when the owner gives none", () => {
    for (const language of ["id", "en"]) {
      const file = path.resolve(__dirname, `../../../../backend/app/messages/${language}.json`);
      const notes = JSON.parse(readFileSync(file, "utf8")).notes as Record<string, string>;
      for (const key of ["movement_default", "movement", "topup"]) expect(PLACEHOLDER_NOTES).toContain(notes[key]);
    }
  });

  it("titles an expense by its note, then its merchant, then its category", () => {
    const base = leg({ category_name: "Langganan", category_system_key: null, account_name: "Subscription", account_parent_name: "Jago" });
    const both = { ...base, notes: "Top up saldo untuk development", merchant: "fal.ai" };
    expect(rowTitle(both, labels)).toBe("Top up saldo untuk development");
    expect(rowMeta(both, labels, id.format).text).toBe("fal.ai · Jago · Subscription");
    const named = { ...base, notes: "Top up saldo FAL.AI untuk development", merchant: "fal.ai" };
    expect(rowTitle(named, labels)).toBe("Top up saldo FAL.AI untuk development");
    expect(rowMeta(named, labels, id.format).text).toBe("Jago · Subscription");
    expect(rowTitle({ ...base, notes: null, merchant: "Kedai Awan" }, labels)).toBe("Kedai Awan");
    expect(rowMeta({ ...base, notes: null, merchant: "Kedai Awan" }, labels, id.format).text).toBe("Jago · Subscription");
    expect(rowTitle({ ...base, notes: "  ", merchant: null }, labels)).toBe("Langganan");
  });

  it("titles a row with only a generic notification note by its merchant", () => {
    const base = leg({ category_name: "Belanja", category_system_key: null, account_name: "ATM", account_parent_name: "BCA" });
    for (const note of GENERIC_ROW_NOTES) {
      expect(rowTitle({ ...base, notes: note, merchant: "Toko Contoh" }, labels)).toBe("Toko Contoh");
      expect(rowMeta({ ...base, notes: note, merchant: "Toko Contoh" }, labels, id.format).text).toBe("BCA · ATM");
      expect(rowTitle({ ...base, notes: note, merchant: null }, labels)).toBe(note);
    }
  });

  it("matches the generic notes the backend writes on a notification row", () => {
    for (const language of ["id", "en"]) {
      const file = path.resolve(__dirname, `../../../../backend/app/messages/${language}.json`);
      const notes = JSON.parse(readFileSync(file, "utf8")).notes as Record<string, string>;
      for (const key of ["transaction", "generic_expense", "generic_income", "generic_transfer_in", "generic_transfer_out", "sender"]) {
        expect(GENERIC_ROW_NOTES).toContain(notes[key]);
      }
    }
  });

  it("titles a top-up by its note, else by its product, else as an investment top-up", () => {
    const topup = leg({ investment_topup_id: "t1", movement_id: "t1", movement_role: "outbound", account_name: "RDN", account_parent_name: "Stockbit", transfer_target_account_name: "Reksa Dana A" });
    expect(rowTitle(topup, labels)).toBe("Top up Reksa Dana A");
    expect(rowTitle(topup, english)).toBe("Top-up Reksa Dana A");
    expect(rowTitle({ ...topup, notes: "Tambah reksa dana" }, labels)).toBe("Tambah reksa dana");
    expect(rowTitle({ ...topup, transfer_target_account_name: null }, labels)).toBe("Top up Investasi");
    expect(rowCategory(topup, labels)).toBe("Top up investasi");
  });
});

describe("rowCategory (D11)", () => {
  it("names the category, says when there is none yet, and words movements", () => {
    expect(rowCategory(spend, labels)).toBe("Makan di Luar");
    expect(rowCategory({ ...spend, category_name: null }, labels)).toBe("Tanpa kategori");
    expect(rowCategory({ ...spend, category_name: null, review_status: "needs_review" }, labels)).toBe("Belum ada kategori");
    expect(rowCategory(outboundLeg, labels)).toBe("Pindah saldo");
    expect(rowCategory({ ...spend, category_name: null, review_status: "needs_review" }, english)).toBe("No category yet");
  });
});

describe("the pillar column (owner feedback 3, D27)", () => {
  it("names each saving direction and shows a dash for a move that changes nothing", () => {
    expect(rowStatus({ ...outboundLeg, saving_flow: "saved" }, labels)).toEqual({ text: "Ditabung", bucket: null });
    expect(rowStatus({ ...outboundLeg, saving_flow: "taken" }, labels).text).toBe("Diambil dari tabungan");
    expect(rowStatus({ ...outboundLeg, saving_flow: "invested" }, labels).text).toBe("Investasi");
    expect(rowStatus({ ...outboundLeg, saving_flow: "debt_payment" }, labels).text).toBe("Bayar utang");
    expect(rowStatus({ ...outboundLeg, saving_flow: null }, labels).text).toBe("—");
    expect(rowStatus(outboundLeg, labels).text).toBe("—");
    expect(rowStatus({ ...outboundLeg, saving_flow: "taken" }, english).text).toBe("Taken from savings");
  });

  it("never says Tidak dihitung, keeps Uang masuk for income and the pillar for spending", () => {
    const income = leg({ type: "income", category_name: "Gaji", category_system_key: null, review_status: "reviewed" });
    expect(rowStatus(income, labels).text).toBe("Uang masuk");
    expect(rowStatus(spend, labels)).toMatchObject({ text: "Keinginan", bucket: "want" });
    expect(rowStatus({ ...spend, kakeibo_type: null }, labels)).toMatchObject({ text: "Belum diklasifikasi", bucket: "unclassified" });
    expect(rowStatus({ ...spend, review_status: "needs_review" }, labels)).toEqual({ text: "Menunggu jawaban", bucket: null, warning: true });
    expect(Object.values(labels.flow)).not.toContain("Tidak dihitung");
  });

  it("names an expense paid from savings in its meta", () => {
    expect(rowMeta({ ...spend, saving_flow: "paid_from_savings" }, labels, id.format)).toEqual({ text: "Jago · Main Pocket · Dibayar dari tabungan" });
  });
});

describe("credit rows (H5, D30)", () => {
  const purchase = leg({
    category_name: "Makan di Luar", category_system_key: null, merchant: "Kedai Awan", account_name: "SPayLater", account_parent_name: null,
    kakeibo_type: "want", review_status: "reviewed",
    credit: { role: "purchase", account_id: "spl", account_name: "SPayLater", installment_count: 1, installments_settled: 0, next_due_date: "2026-12-11", settled_on: null },
  });

  it("says that a purchase is paid later, with its provider and due date", () => {
    expect(rowMeta(purchase, labels, id.format)).toEqual({ text: "dibayar nanti · SPayLater · jatuh tempo 11 Des" });
    expect(rowMeta(purchase, english, en.format)).toEqual({ text: "pay later · SPayLater · due 11 Dec" });
    expect(rowPhoneLine(purchase, labels, id.format)).toEqual({ text: "dibayar nanti" });
    expect(rowStatus(purchase, labels).text).toBe("Keinginan");
  });

  it("counts instalments and says when a purchase is paid", () => {
    const split = { ...purchase, credit: { ...purchase.credit!, installment_count: 3, installments_settled: 1, next_due_date: "2027-01-11" } };
    expect(creditPurchaseLine(split, labels, id.format)).toBe("dibayar nanti · SPayLater · cicilan 2/3 · jatuh tempo 11 Jan");
    expect(creditPurchaseLine(split, english, en.format)).toBe("pay later · SPayLater · installment 2 of 3 · due 11 Jan");
    const settled = { ...purchase, credit: { ...purchase.credit!, settled_on: "2026-12-04", next_due_date: null } };
    expect(rowMeta(settled, labels, id.format)).toEqual({ text: "SPayLater · lunas 4 Des" });
    expect(rowPhoneLine(settled, labels, id.format)).toEqual({ text: "lunas 4 Des" });
  });

  it("adds bayar tagihan to a payment's route", () => {
    const payment = leg({
      movement_id: "mv", movement_role: "outbound", account_name: "Jago", account_parent_name: null, transfer_target_account_name: "SPayLater",
      saving_flow: "debt_payment", credit: { role: "payment", account_id: "spl", account_name: "SPayLater" },
    });
    expect(rowTitle(payment, labels)).toBe("Jago → SPayLater");
    expect(rowMeta(payment, labels, id.format)).toEqual({ text: "bayar tagihan" });
    expect(rowMeta({ ...payment, notes: "Bayar SPayLater" }, labels, id.format)).toEqual({ text: "Jago → SPayLater · bayar tagihan" });
    expect(rowMeta(payment, labels, id.format, true)).toEqual({ text: "Keluar ke SPayLater · bayar tagihan" });
    expect(rowPhoneLine(payment, labels, id.format)).toEqual({ text: "bayar tagihan" });
  });
});

describe("the one row amount (D29)", () => {
  const money = (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`;

  describe("a credit purchase", () => {
    const facts = { role: "purchase", account_id: "spl", account_name: "SPayLater", installment_count: 1, installments_settled: 0, next_due_date: "2026-12-11", settled_on: null } as const;
    const onCredit = (credit: Partial<typeof facts> | Record<string, unknown>) => leg({ category_system_key: null, category_name: "Makan", credit: { ...facts, ...credit } as never });

    it("is amber with a minus while unpaid", () => {
      render(<RowAmount row={onCredit({})} money={money} />);
      expect(screen.getByText("−Rp 500.000", { selector: ".c-credit" })).toBeInTheDocument();
    });

    it("stays amber while some instalments are still unpaid", () => {
      render(<RowAmount row={onCredit({ installment_count: 6, installments_settled: 2 })} money={money} />);
      expect(screen.getByText("−Rp 500.000", { selector: ".c-credit" })).toBeInTheDocument();
    });

    it("stays amber on an account with no payment day", () => {
      render(<RowAmount row={onCredit({ next_due_date: null, due_dates_unavailable: true })} money={money} />);
      expect(screen.getByText("−Rp 500.000", { selector: ".c-credit" })).toBeInTheDocument();
    });

    it("turns the expense red once fully paid", () => {
      render(<RowAmount row={onCredit({ settled_on: "2026-12-04", next_due_date: null })} money={money} />);
      expect(screen.getByText("−Rp 500.000", { selector: ".c-expense" })).toBeInTheDocument();
    });

    it("leaves a credit fee and a bank expense red", () => {
      const { container } = render(
        <>
          <RowAmount row={onCredit({ role: "fee" })} money={money} />
          <RowAmount row={spend} money={money} />
        </>,
      );
      expect(container.querySelectorAll(".c-expense")).toHaveLength(2);
      expect(container.querySelector(".c-credit")).toBeNull();
    });
  });

  it("paints spending red with a minus, income green with a plus, transfers sky and top-ups lavender", () => {
    const { container } = render(
      <>
        <RowAmount row={spend} money={money} />
        <RowAmount row={leg({ type: "income", category_system_key: null, category_name: "Gaji" })} money={money} />
        <RowAmount row={outboundLeg} money={money} />
        <RowAmount row={leg({ investment_topup_id: "t", movement_id: "t" })} money={money} />
        <KindAmount kind="plain" amount={65_000} money={money} />
      </>,
    );
    expect(screen.getByText("−Rp 500.000", { selector: ".c-expense" })).toBeInTheDocument();
    expect(screen.getByText("+Rp 500.000", { selector: ".c-income" })).toBeInTheDocument();
    expect(container.querySelector(".c-transfer")).toHaveTextContent(/^Rp 500\.000$/);
    expect(container.querySelector(".c-invest")).toHaveTextContent(/^Rp 500\.000$/);
    expect(screen.getByText("Rp 65.000")).toHaveClass("t-amt");
    expect(rowKind(spend)).toBe("expense");
  });
});
