import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { MaskedMoney } from "@/components/workspace/money";
import { UntranslatedRegion } from "@/components/workspace/untranslated";
import { renderWithIntl } from "@/lib/i18n/testing";
import { ConfirmActionButton } from "./ConfirmActionButton";
import { Modal } from "./Modal";

vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ hideBalances: true, bal: () => "Rp ••••••" }) }));

/** Shared primitives follow the interface language on every page (add-language-system D17). */
describe("shared primitives in English", () => {
  it("names the modal's close button and the confirm step in English", async () => {
    const user = userEvent.setup();
    renderWithIntl(
      <Modal open onClose={vi.fn()} title="Delete">
        <ConfirmActionButton label="Remove" confirmation="Remove this?" onConfirm={vi.fn()}>
          x
        </ConfirmActionButton>
      </Modal>,
      { language: "en" },
    );
    expect(screen.getByRole("button", { name: "Close" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Remove" }));
    expect(screen.getByRole("button", { name: "Yes, continue" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument();
  });

  it("marks a dialog of a later phase Indonesian with the notice line, and its close button English", () => {
    renderWithIntl(
      <Modal open onClose={vi.fn()} title="Catat transaksi" untranslated>
        <p>Isi manual</p>
      </Modal>,
      { language: "en" },
    );
    expect(screen.getByRole("heading", { name: "Catat transaksi" })).toHaveAttribute("lang", "id");
    expect(screen.getByText("This page is in Indonesian for now.")).toHaveAttribute("lang", "en");
    expect(screen.getByText("Isi manual").closest("[lang]")).toHaveAttribute("lang", "id");
    expect(screen.getByRole("button", { name: "Close" })).toHaveAttribute("lang", "en");
  });

  it("shows the same dialog in Indonesian without a notice", () => {
    renderWithIntl(
      <Modal open onClose={vi.fn()} title="Catat transaksi" untranslated>
        <p>Isi manual</p>
      </Modal>,
    );
    expect(screen.getByRole("heading", { name: "Catat transaksi" })).not.toHaveAttribute("lang");
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Tutup" })).toBeInTheDocument();
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
