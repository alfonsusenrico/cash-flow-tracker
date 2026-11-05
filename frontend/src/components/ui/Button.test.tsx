import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Button } from "./Button";

// Button hierarchy of theme-presets-switcher "Modern Button Styling System" (option-6 SPEC §7).
describe("Button", () => {
  it("paints the primary action coral with its contrast ink, hover, pressed and a 1 px press", () => {
    render(<Button variant="primary">Simpan</Button>);
    const button = screen.getByRole("button", { name: "Simpan" });
    for (const name of ["bg-[var(--accent)]", "text-[var(--accent-ink)]", "hover:bg-[var(--accent-hover)]", "active:bg-[var(--accent-pressed)]", "active:translate-y-px"]) {
      expect(button.className).toContain(name);
    }
    expect(button.className).not.toMatch(/--ink\)|ring-|shadow-/);
  });

  it("keeps the secondary and quiet buttons on neutral tokens with the control radius and 14/500 text", () => {
    render(
      <>
        <Button variant="secondary">Batal</Button>
        <Button variant="ghost" size="sm">Kelola</Button>
      </>,
    );
    const secondary = screen.getByRole("button", { name: "Batal" });
    expect(secondary.className).toContain("border-[var(--line-strong)]");
    expect(secondary.className).toContain("rounded-md");
    expect(secondary.className).toContain("text-sm font-medium");
    const quiet = screen.getByRole("button", { name: "Kelola" });
    expect(quiet.className).toContain("text-[var(--muted)]");
    expect(quiet.className).toContain("min-h-7");
  });

  it("uses no focus ring utility, so keyboard focus is the global neutral outline", () => {
    render(<Button variant="danger">Ya, hapus</Button>);
    expect(screen.getByRole("button", { name: "Ya, hapus" }).className).not.toMatch(/focus|ring/);
  });
});
