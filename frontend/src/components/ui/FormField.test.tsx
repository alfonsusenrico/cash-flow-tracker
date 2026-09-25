import { render, screen } from "@testing-library/react";
import { axe } from "vitest-axe";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import { ChoiceGroup, FormErrorSummary, FormField, MonetaryInput, PendingSubmitButton } from "./FormField";

describe("FormField", () => {
  it("associates label, description, and error with its control", async () => {
    const { container } = render(
      <>
        <FormField label="Nominal" description="Dalam rupiah" error="Nominal wajib diisi" required>
          {(props) => <input {...props} />}
        </FormField>
        <FormErrorSummary message="Periksa kembali isian." />
      </>,
    );
    const input = screen.getByRole("textbox", { name: /Nominal/ });
    expect(input).toHaveAttribute("aria-invalid", "true");
    expect(input).toHaveAccessibleDescription(/Dalam rupiah Nominal wajib diisi/);
    expect(screen.getByText("Dalam rupiah")).not.toBeVisible();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("shows optional guidance from an information control and keeps errors visible", async () => {
    const user = userEvent.setup();
    render(
      <FormField label="Nominal" description="Dalam rupiah" error="Nominal wajib diisi">
        {(props) => <input {...props} />}
      </FormField>,
    );

    const help = screen.getByRole("button", { name: "Info Nominal" });
    expect(help).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByText("Nominal wajib diisi")).toBeVisible();

    await user.click(help);
    expect(help).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText("Dalam rupiah")).toBeVisible();

    await user.keyboard("{Escape}");
    expect(help).toHaveFocus();
    expect(help).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByText("Nominal wajib diisi")).toBeVisible();
  });

  it("exposes selected and disabled choice states and prevents pending resubmission", async () => {
    const { container } = render(
      <form>
        <ChoiceGroup
          label="Kakeibo"
          name="kakeibo"
          value="saving"
          onChange={() => {}}
          options={[
            { value: "need", label: "Need" },
            { value: "saving", label: "Saving" },
            { value: "want", label: "Want", disabled: true },
          ]}
        />
        <PendingSubmitButton pending pendingLabel="Saving…">
          Save
        </PendingSubmitButton>
      </form>,
    );
    expect(screen.getByRole("radio", { name: "Saving" })).toBeChecked();
    expect(screen.getByRole("radio", { name: "Want" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Saving…" })).toBeDisabled();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("formats a shared monetary field and associates its error", async () => {
    function Example() {
      const [amount, setAmount] = useState("");
      return <MonetaryInput id="money" name="amount" label="Saldo Awal" value={amount} onChange={setAmount} error="Periksa saldo" />;
    }
    const { container } = render(<Example />);
    const input = screen.getByRole("textbox", { name: "Saldo Awal" });
    await userEvent.setup().type(input, "1000000");
    expect(input).toHaveValue("1.000.000");
    expect(input).toHaveAttribute("aria-invalid", "true");
    expect(input).toHaveAccessibleDescription("Periksa saldo");
    expect((await axe(container)).violations).toHaveLength(0);
  });
});
