import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { describe, expect, it, vi } from "vitest";
import { ConfirmActionButton } from "./ConfirmActionButton";

describe("ConfirmActionButton", () => {
  it("announces destructive confirmation and requires an explicit choice", async () => {
    const user = userEvent.setup();
    const onConfirm = vi.fn();
    const { container } = render(
      <ConfirmActionButton
        label="Archive account"
        confirmation="Archive BCA account?"
        onConfirm={onConfirm}
      >
        Archive
      </ConfirmActionButton>,
    );

    await user.click(screen.getByRole("button", { name: "Archive account" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Archive BCA account?");
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Batal" }));
    expect(onConfirm).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Archive account" }));
    await user.click(screen.getByRole("button", { name: "Ya, lanjutkan" }));
    expect(onConfirm).toHaveBeenCalledOnce();
  });

  it("keeps the trigger's quiet styling off the destructive confirm button", async () => {
    const user = userEvent.setup();
    render(
      <ConfirmActionButton label="Arsipkan target" confirmation="Arsipkan target?" onConfirm={vi.fn()} className="text-[var(--muted)] hover:bg-rose-500/10">
        Arsipkan
      </ConfirmActionButton>,
    );

    await user.click(screen.getByRole("button", { name: "Arsipkan target" }));
    const confirm = screen.getByRole("button", { name: "Ya, lanjutkan" });
    expect(confirm).toHaveClass("bg-[var(--danger-solid)]", "text-[var(--danger-contrast)]");
    expect(confirm).not.toHaveClass("text-[var(--muted)]");
  });
});
