import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { describe, expect, it, vi } from "vitest";
import { Modal } from "./Modal";

describe("Modal", () => {
  it("names the dialog, traps focus, closes with Escape, and restores focus", async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true }));
    const { container, rerender } = render(
      <>
        <button>Open movement</button>
        <Modal open={false} onClose={onClose} title="Pindah saldo">
          <input aria-label="Nominal" data-autofocus />
          <button>Simpan</button>
        </Modal>
      </>,
    );
    const trigger = screen.getByRole("button", { name: "Open movement" });
    trigger.focus();
    rerender(
      <>
        <button>Open movement</button>
        <Modal open onClose={onClose} title="Pindah saldo">
          <input aria-label="Nominal" data-autofocus />
          <button>Simpan</button>
        </Modal>
      </>,
    );
    expect(await screen.findByRole("dialog", { name: "Pindah saldo" })).toBeVisible();
    expect(await screen.findByLabelText("Nominal")).toHaveFocus();
    expect(document.body.style.overflow).toBe("hidden");
    await user.tab({ shift: true });
    expect(screen.getByRole("button", { name: "Tutup" })).toHaveFocus();
    await user.tab({ shift: true });
    expect(screen.getByRole("button", { name: "Simpan" })).toHaveFocus();
    expect((await axe(document.body)).violations).toHaveLength(0);
    await user.keyboard("{Escape}");
    expect(onClose).toHaveBeenCalledOnce();

    rerender(<button>Open movement</button>);
    expect(screen.getByRole("button", { name: "Open movement" })).toHaveFocus();
    expect(document.body.style.overflow).toBe("");
    expect(container).toBeTruthy();
    vi.unstubAllGlobals();
  });

  it("avoids focusing a text input when opened as a mobile sheet", async () => {
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: false }));
    const { unmount } = render(
      <Modal open onClose={() => {}} title="Pindah saldo">
        <input aria-label="Nominal" data-autofocus />
      </Modal>,
    );
    expect(await screen.findByRole("dialog", { name: "Pindah saldo" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Tutup" })).toHaveFocus();
    expect(screen.getByLabelText("Nominal")).not.toHaveFocus();
    unmount();
    vi.unstubAllGlobals();
  });
});
