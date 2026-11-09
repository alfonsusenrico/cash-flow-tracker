import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { MaskedMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { UntranslatedRegion } from "@/components/workspace/untranslated";
import { renderWithIntl } from "@/lib/i18n/testing";
import { ConfirmActionButton } from "./ConfirmActionButton";

vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ hideBalances: true, bal: () => "Rp ••••••" }) }));

/** Shared primitives follow the interface language on every page (add-language-system D17). */
describe("shared primitives in English", () => {
  it("words the confirm step of a dialog in English", async () => {
    const user = userEvent.setup();
    renderWithIntl(
      <ShellDialog open onClose={vi.fn()} title="Delete" width={560} closeLabel="Close">
        <ConfirmActionButton label="Remove" confirmation="Remove this?" onConfirm={vi.fn()}>
          x
        </ConfirmActionButton>
      </ShellDialog>,
      { language: "en" },
    );
    await user.click(screen.getByRole("button", { name: "Remove" }));
    expect(screen.getByRole("button", { name: "Yes, continue" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument();
  });

  it("marks no dialog Indonesian, even one opened from an untranslated page (apply-markas-catat D21)", () => {
    renderWithIntl(
      <UntranslatedRegion active>
        <ShellDialog open onClose={vi.fn()} title="Record a transaction" width={560} closeLabel="Close">
          <p>Write a sentence</p>
        </ShellDialog>
      </UntranslatedRegion>,
      { language: "en" },
    );
    expect(screen.getByRole("heading", { name: "Record a transaction" })).not.toHaveAttribute("lang");
    expect(screen.getByText("Write a sentence").closest("[lang]")).toBeNull();
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
  });

  it("names a hidden amount Hidden, in English even inside an Indonesian page", () => {
    renderWithIntl(
      <div lang="id">
        <UntranslatedRegion active>
          <MaskedMoney text="Rp 1.234" />
        </UntranslatedRegion>
      </div>,
      { language: "en" },
    );
    const mask = screen.getByRole("img", { name: "Hidden" });
    expect(mask).toHaveTextContent("Rp ••••••");
    expect(mask).toHaveAttribute("lang", "en");
  });
});
