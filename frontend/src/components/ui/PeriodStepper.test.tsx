import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { renderWithIntl } from "@/lib/i18n/testing";
import { PeriodStepper } from "./PeriodStepper";

/** The stepper follows the interface language on every page (add-language-system D17, monthly-navigation-stepper). */
describe("PeriodStepper", () => {
  it("keeps the server's Indonesian label in Indonesian", () => {
    renderWithIntl(<PeriodStepper unit="Bulan" label="Oktober 2026" period={{ start: "2026-10-01", end: "2026-10-31" }} offset={-1} onChange={vi.fn()} />);
    expect(screen.getByText("Oktober 2026")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Bulan sebelumnya" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Bulan ini" })).toBeInTheDocument();
  });

  it("reads Oct 2026 in English with no language override", () => {
    const { container } = renderWithIntl(
      <PeriodStepper unit="Bulan" label="Oktober 2026" period={{ start: "2026-10-01T00:00:00+07:00", end: "2026-10-31T23:59:59+07:00" }} offset={-1} onChange={vi.fn()} />,
      { language: "en" },
    );
    expect(screen.getByText("Oct 2026")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Previous month" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Next month" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "This month" })).toBeInTheDocument();
    expect(container.querySelector("[lang]")).toBeNull();
  });

  it("names a cycle by its dates in English and says Loading while the label is not known", () => {
    const { rerender } = renderWithIntl(<PeriodStepper unit="Siklus" label="25 Sep – 24 Okt" period={{ start: "2026-09-25", end: "2026-10-24" }} offset={0} onChange={vi.fn()} />, { language: "en" });
    expect(screen.getByText("25 Sep – 24 Oct")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Previous cycle" })).toBeInTheDocument();
    rerender(<PeriodStepper unit="Siklus" label={null} offset={0} onChange={vi.fn()} />);
    expect(screen.getByText("Loading…")).toBeInTheDocument();
  });

  it("names a running cycle as a range to its last day in English, today without one (improve-ledger-readability D5)", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-11-16T05:00:00+07:00"));
    try {
      const { rerender } = renderWithIntl(
        <PeriodStepper unit="Siklus" label="30 Okt – 29 Nov" period={{ start: "2026-10-30", end: "2026-11-29" }} offset={0} onChange={vi.fn()} />,
        { language: "en" },
      );
      expect(screen.getByText("30 Oct – 29 Nov")).toBeInTheDocument();
      rerender(<PeriodStepper unit="Siklus" label="Siklus sejak 30 Okt" period={{ start: "2026-10-30", end: null }} offset={0} onChange={vi.fn()} />);
      expect(screen.getByText("30 Oct – 16 Nov")).toBeInTheDocument();
      expect(screen.queryByText(/since/)).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it("is the SPEC stepper: a labelled group of two icon buttons around a live label, with the reset kept", () => {
    const { container } = renderWithIntl(<PeriodStepper unit="Siklus" label="23 Sep – 22 Okt" offset={-1} minOffset={-1} onChange={vi.fn()} />);
    const group = screen.getByRole("group", { name: "Periode" });
    expect(group).toHaveClass("stepper");
    expect(container.querySelector(".stepper .cur")).toHaveAttribute("aria-live", "polite");
    expect(screen.getByRole("button", { name: "Siklus sebelumnya" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Siklus berikutnya" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Siklus ini" })).toHaveClass("ws-btn-quiet");
  });
});
