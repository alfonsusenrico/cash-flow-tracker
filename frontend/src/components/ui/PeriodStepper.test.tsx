import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { UntranslatedRegion } from "@/components/workspace/untranslated";
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

  it("reads Oct 2026 in English on an untranslated Transaksi, marked as English inside the Indonesian page", () => {
    const { container } = renderWithIntl(
      <div lang="id">
        <UntranslatedRegion active>
          <PeriodStepper unit="Bulan" label="Oktober 2026" period={{ start: "2026-10-01T00:00:00+07:00", end: "2026-10-31T23:59:59+07:00" }} offset={-1} onChange={vi.fn()} />
        </UntranslatedRegion>
      </div>,
      { language: "en" },
    );
    expect(screen.getByText("Oct 2026")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Previous month" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Next month" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "This month" })).toBeInTheDocument();
    expect(container.querySelector("[lang='en']")).toContainElement(screen.getByText("Oct 2026"));
  });

  it("names a cycle by its dates in English and says Loading while the label is not known", () => {
    const { rerender } = renderWithIntl(<PeriodStepper unit="Siklus" label="Siklus 25 Sep – 24 Okt" period={{ start: "2026-09-25", end: "2026-10-24" }} offset={0} onChange={vi.fn()} />, { language: "en" });
    expect(screen.getByText("Cycle 25 Sept – 24 Oct")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Previous cycle" })).toBeInTheDocument();
    rerender(<PeriodStepper unit="Siklus" label={null} offset={0} onChange={vi.fn()} />);
    expect(screen.getByText("Loading…")).toBeInTheDocument();
  });

  it("is the SPEC stepper: a labelled group of two icon buttons around a live label, with the reset kept", () => {
    const { container } = renderWithIntl(<PeriodStepper unit="Siklus" label="Siklus 23 Sep – 22 Okt" offset={-1} minOffset={-1} onChange={vi.fn()} />);
    const group = screen.getByRole("group", { name: "Periode" });
    expect(group).toHaveClass("stepper");
    expect(container.querySelector(".stepper .cur")).toHaveAttribute("aria-live", "polite");
    expect(screen.getByRole("button", { name: "Siklus sebelumnya" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Siklus berikutnya" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Siklus ini" })).toHaveClass("ws-btn-quiet");
  });
});
