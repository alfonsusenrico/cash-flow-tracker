import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { renderWithIntl } from "@/lib/i18n/testing";
import { GainText, ProgressBar } from "@/components/workspace/figures";
import { BILL_STATUS_KEY, billStatusTone, creditFacts, isUnpaid } from "./credit";
import { badgeInk } from "./displayColours";
import type { AccountItem } from "./types";

const hidden = vi.hoisted(() => ({ on: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ hideBalances: hidden.on, bal: () => "Rp ••••••" }),
}));

const credit = (fields: Partial<AccountItem>): AccountItem => ({
  id: "c", name: "SPayLater", type: "credit", initial_balance: 0, balance: -893_000, is_archived: false, created_at: "2026-09-01", credit_owed: 893_000,
  credit_limit: 3_000_000, limit_left: 2_107_000, over_limit: false, payment_day: 5, statement_day: 25, next_due: null, ...fields,
});

/** WCAG relative-luminance contrast of two hex colours. */
function contrast(foreground: string, background: string): number {
  const luminance = (hex: string) => {
    const [r, g, b] = [1, 3, 5].map((at) => parseInt(hex.slice(at, at + 2), 16) / 255).map((value) => (value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4));
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };
  const [light, dark] = [luminance(foreground), luminance(background)].sort((a, b) => b - a);
  return (light + 0.05) / (dark + 0.05);
}

describe("GainText", () => {
  it("writes the triangle, the sign, the amount and a one-decimal percent with a hidden word", () => {
    hidden.on = false;
    const { container, rerender } = renderWithIntl(<GainText gain={{ amount: 396_000, cost: 4_950_000 }} />);
    expect(container).toHaveTextContent("▲ +Rp 396.000 · +8,0% untung");
    expect(container.querySelector(".gain")).toHaveClass("c-income");
    rerender(<GainText gain={{ amount: -164_000, cost: 1_532_710 }} />);
    expect(container).toHaveTextContent("▼ −Rp 164.000 · −10,7% rugi");
    expect(container.querySelector(".gain")).toHaveClass("c-expense");
    rerender(<GainText gain="unknown" />);
    expect(container).toHaveTextContent("Modal belum diisi");
    rerender(<GainText gain={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("masks the amount and its sign with hidden balances and keeps the percent", () => {
    hidden.on = true;
    const { container } = renderWithIntl(<GainText gain={{ amount: 396_000, cost: 4_950_000 }} />);
    expect(screen.getByRole("img", { name: "Disembunyikan" })).toHaveTextContent("Rp ••••••");
    expect(container).not.toHaveTextContent("396.000");
    expect(container).toHaveTextContent("+8,0%");
    hidden.on = false;
  });

  it("reads in English with the region's digits", () => {
    const { container } = renderWithIntl(<GainText gain={{ amount: 396_000, cost: 4_950_000 }} />, { language: "en" });
    expect(container).toHaveTextContent("▲ +Rp 396.000 · +8,0% gain");
  });
});

describe("ProgressBar", () => {
  it("is a decorative 4 px or 8 px track with a square fill capped at the track", () => {
    const { container, rerender } = renderWithIntl(<ProgressBar value={893} max={3000} />);
    const bar = container.querySelector(".prog")!;
    expect(bar).toHaveAttribute("aria-hidden", "true");
    expect(bar).not.toHaveClass("is-thick");
    expect((bar.firstChild as HTMLElement).style.width).toBe(`${(893 / 3000) * 100}%`);
    rerender(<ProgressBar value={4000} max={3000} thick />);
    expect(container.querySelector(".prog")).toHaveClass("is-thick");
    expect((container.querySelector(".prog")!.firstChild as HTMLElement).style.width).toBe("100%");
  });
});

describe("creditFacts", () => {
  it("reads owed, limit left, over limit, overpaid, no limit and the next bill from the account row", () => {
    expect(creditFacts(credit({}))).toMatchObject({ owed: 893_000, overpaid: 0, limit: 3_000_000, limitLeft: 2_107_000, overLimit: false, paymentDay: 5, statementDay: 25 });
    expect(creditFacts(credit({ over_limit: true, limit_left: -150_000 }))).toMatchObject({ overLimit: true, limitLeft: -150_000 });
    expect(creditFacts(credit({ balance: 12_000, credit_owed: 0 }))).toMatchObject({ owed: 0, overpaid: 12_000 });
    expect(creditFacts(credit({ credit_limit: null, limit_left: null }))).toMatchObject({ limit: null, limitLeft: null });
    expect(creditFacts(credit({ payment_day: null })).paymentDay).toBeNull();
    const due = { due_date: "2026-12-11", amount: 434_000, left_to_pay: 434_000, status: "due_today" as const };
    expect(creditFacts(credit({ next_due: due })).nextDue).toEqual(due);
  });

  it("names every bill status and tones only due today and overdue as warnings", () => {
    expect(Object.values(BILL_STATUS_KEY)).toEqual(["open", "upcoming", "dueToday", "overdue", "paid", "paidLate"]);
    expect(billStatusTone("due_today")).toBe("warning");
    expect(billStatusTone("overdue")).toBe("warning");
    expect(billStatusTone("paid_late")).toBe("muted");
    expect(billStatusTone("open")).toBe("plain");
    expect(isUnpaid("upcoming")).toBe(true);
    expect(isUnpaid("paid")).toBe(false);
  });
});

describe("InstitutionMark initials", () => {
  // The nine colours of the account form and the display hues; the mark sits on its own colour in both themes.
  const COLOURS = ["#2E7DF6", "#F08A24", "#16A34A", "#E0A800", "#7C6CF2", "#0FB5AE", "#E5484D", "#A855F7", "#64748B", "#D9467F", "#3b82f6", "#10b981", "#f59e0b"];
  it("takes white or ink, whichever reaches 4.5:1 on the colour", () => {
    for (const colour of COLOURS) {
      const ink = badgeInk(colour);
      expect(contrast(ink, colour), `${ink} on ${colour}`).toBeGreaterThanOrEqual(4.5);
    }
  });
});
