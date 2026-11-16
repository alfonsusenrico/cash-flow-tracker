import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
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
