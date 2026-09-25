import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { AccountOptions } from "./AccountOptions";

describe("AccountOptions", () => {
  it("keeps pocket actions outside a scroll area and restores focus on Escape", async () => {
    const user = userEvent.setup();
    render(
      <div data-testid="pockets" className="overflow-y-auto">
        <AccountOptions name="Tabungan">
          <button type="button">Sesuaikan Saldo</button>
        </AccountOptions>
      </div>,
    );

    const trigger = screen.getByRole("button", { name: "Opsi Tabungan" });
    await user.click(trigger);
    const menu = screen.getByRole("group", { name: "Opsi Tabungan" });
    expect(screen.getByTestId("pockets")).not.toContainElement(menu);
    expect(screen.getByRole("button", { name: "Sesuaikan Saldo" })).toHaveFocus();

    fireEvent.scroll(menu);
    expect(menu).toBeInTheDocument();

    await user.keyboard("{Escape}");
    expect(menu).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });
});
