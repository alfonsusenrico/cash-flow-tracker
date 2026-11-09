import { render, screen } from "@testing-library/react";
import { axe } from "vitest-axe";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import { FormField, MonetaryInput } from "./FormField";

describe("FormField", () => {
  it("associates label, description, and error with its control", async () => {
    const { container } = render(
      <FormField label="Nominal" description="Dalam rupiah" error="Nominal wajib diisi" required>
        {(props) => <input {...props} />}
      </FormField>,
    );
    const input = screen.getByRole("textbox", { name: /Nominal/ });
    expect(input).toHaveAttribute("aria-invalid", "true");
    expect(input).toHaveAccessibleDescription(/Dalam rupiah Nominal wajib diisi/);
    expect(screen.getByText("Dalam rupiah")).toBeVisible();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
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
