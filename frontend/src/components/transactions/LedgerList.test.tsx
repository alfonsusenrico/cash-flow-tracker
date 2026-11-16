import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ListRow } from "@/components/workspace/lists";
import { testWords } from "@/lib/i18n/testing";
import { LedgerList, type ListSelection } from "./LedgerList";
import { rowLabelsFor, rowTags } from "./rows";
import type { LedgerRow } from "./types";

vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ hideBalances: false, bal: (amount: number) => `Rp ${amount}` }) }));

const id = testWords("id");

function ledgerRow(extra: Partial<LedgerRow>): LedgerRow {
  return {
    id: "row-1", account_id: "cash", account_name: "Cash", category_id: "food", category_name: "Makanan & Minuman", type: "expense",
    amount: 85_000, date: "2026-11-15T12:30:00+07:00", name: "Bayar makan di Kafe Contoh", goal_id: null, goal_name: null,
    obligation_id: null, obligation_name: null, receipt_path: null, created_at: "2026-11-15T12:30:00+07:00", kakeibo_type: "culture",
    review_status: "reviewed", ...extra,
  } as LedgerRow;
}

const selection: ListSelection = { active: false, selectedIds: [], disabled: () => false, toggle: vi.fn(), start: vi.fn() };

function show(rows: LedgerRow[]) {
  return render(
    <LedgerList
      groups={[{ date: "2026-11-15", rows, totals: null, remaining: 0 }]}
      openRow={vi.fn()}
      selection={selection}
      loadMore={null}
      openId={null}
      accountView={false}
      today="2026-11-16"
      labels={rowLabelsFor(id.t)}
      format={id.format}
      money={(amount) => `Rp ${amount}`}
    />,
  );
}

describe("ledger row title lines (improve-ledger-readability D1)", () => {
  it("gives the title its own line and puts the meta, the note and the tags on a second line, in that order", () => {
    show([ledgerRow({
      name: "Ganti uang transport dari Rani untuk fellowship", type: "income", account_name: "BCA", category_name: "Gaji", kakeibo_type: null,
      note: "fellowship coach", tags: [{ id: "tag-coach", name: "fellowship coach" }],
    })]);
    const row = screen.getByRole("button", { name: /Ganti uang transport/ });
    expect(row).toHaveClass("is-title-line");
    const main = row.querySelector(".r-main") as HTMLElement;
    expect(main.firstElementChild).toHaveClass("r-title");
    expect(main.firstElementChild).toHaveTextContent(/^Ganti uang transport dari Rani untuk fellowship$/);
    const line = main.querySelector(":scope > .r-sub") as HTMLElement;
    expect(line).toHaveTextContent(/^BCA · fellowship coach · Tag: fellowship coach$/);
    const parts = [...line.querySelectorAll(".r-meta, .r-note-item, .r-sep")].map((part) => part.className.split(" ")[0]);
    expect(parts).toEqual(["r-meta", "r-sep", "r-note-item", "r-sep", "r-note-item"]);
    expect(line.querySelector(".r-note-item:not(.is-tags) svg")).toBeInTheDocument();
    expect(line.querySelector(".r-note-item.is-tags svg")).toBeInTheDocument();
    // The separators are visual; the row's name reads the parts without them.
    expect([...line.querySelectorAll(".r-sep")].every((separator) => separator.getAttribute("aria-hidden") === "true")).toBe(true);
    // The phone keeps its own second line.
    expect(main.querySelector(":scope > .r-meta.ph-only")).toHaveTextContent("12.30 · Gaji");
  });

  it("puts the category above the pillar in one cell on every Transaksi row, and leaves other lists' columns apart", () => {
    show([ledgerRow({ id: "plain", name: "Parkir", account_name: "", tags: [] })]);
    const cell = screen.getByRole("button", { name: /Parkir/ }).querySelector(".r-class") as HTMLElement;
    expect([...cell.children].map((part) => part.className)).toEqual(["r-cat", "r-pillar"]);
    expect(cell.querySelector(".r-cat")).toHaveTextContent("Makanan & Minuman");
    expect(cell.querySelector(".r-pillar")).toHaveTextContent("Budaya");
    render(<ListRow onClick={vi.fn()} title="Kunci API" category="Rekening" status="Aktif" />);
    expect(screen.getByRole("button", { name: /Kunci API/ }).querySelector(".r-class")).toBeNull();
  });

  it("keeps a row without meta, note or tags on one line", () => {
    show([ledgerRow({ id: "plain", name: "Parkir", account_name: "", tags: [] })]);
    const row = screen.getByRole("button", { name: /Parkir/ });
    expect(row).not.toHaveClass("is-title-line");
    expect(row.querySelector(".r-sub")).toBeNull();
    expect(row.querySelector(".r-sep")).toBeNull();
  });

  it("leaves other lists' rows as they were: note and meta beside the title, no second line", () => {
    render(<ListRow onClick={vi.fn()} title="Kunci API" note="laptop" meta="Dibuat 2 Nov" />);
    const row = screen.getByRole("button", { name: /Kunci API/ });
    expect(row).not.toHaveClass("is-title-line");
    expect(row.querySelector(".r-sub")).toBeNull();
    expect([...(row.querySelector(".r-main") as HTMLElement).children].map((part) => part.className)).toEqual(["r-title", "r-note", "r-meta"]);
  });

  it("puts the meta alone on the second line when there is no note or tag", () => {
    show([ledgerRow({ tags: [] })]);
    const row = screen.getByRole("button", { name: /Bayar makan/ });
    expect(row).toHaveClass("is-title-line");
    expect(row.querySelector(".r-sub")).toHaveTextContent(/^Cash$/);
    expect(row.querySelector(".r-sep")).toBeNull();
  });
});

describe("ledger rows with tags (transaction-tags)", () => {
  it("shows the note and the tags beside the title, each with its icon, in one muted extras line", () => {
    show([ledgerRow({
      note: "untuk tim",
      tags: [{ id: "tag-coach", name: "fellowship coach" }, { id: "tag-malang", name: "Malang trip" }],
    })]);
    const row = screen.getByRole("button", { name: /Bayar makan di Kafe Contoh/ });
    const extras = row.querySelector(".r-note") as HTMLElement;
    const items = [...extras.querySelectorAll(".r-note-item")];
    expect(items).toHaveLength(2);
    expect(items[0].querySelector(".r-note-text")).toHaveTextContent("untuk tim");
    expect(items[1].querySelector(".r-note-text")).toHaveTextContent("fellowship coach, Malang trip");
    expect(items.every((item) => item.querySelector("svg[aria-hidden='true']"))).toBe(true);
    // A screen reader hears the tags as tags, not as part of the title.
    expect(row).toHaveAccessibleName(/Tag: fellowship coach, Malang trip/);
    expect(within(row).getByText("Bayar makan di Kafe Contoh")).toHaveClass("r-title");
  });

  it("shows tags alone, and nothing extra for a row without note or tags", () => {
    show([
      ledgerRow({ id: "tagged", tags: [{ id: "tag-coach", name: "fellowship coach" }] }),
      ledgerRow({ id: "plain", name: "Parkir", tags: [] }),
    ]);
    const tagged = screen.getByRole("button", { name: /Bayar makan/ });
    expect(tagged.querySelectorAll(".r-note-item")).toHaveLength(1);
    expect(tagged.querySelector(".r-note-item.is-tags")).toHaveTextContent("fellowship coach");
    expect(screen.getByRole("button", { name: /Parkir/ }).querySelector(".r-note")).toBeNull();
  });

  it("joins tag names, and has none for a row without tags or from an older API", () => {
    expect(rowTags(ledgerRow({ tags: [{ id: "a", name: "satu" }, { id: "b", name: "dua" }] }))).toBe("satu, dua");
    expect(rowTags(ledgerRow({ tags: [] }))).toBeNull();
    expect(rowTags(ledgerRow({}))).toBeNull();
  });
});
