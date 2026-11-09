import { render, screen, within } from "@testing-library/react";
import { axe } from "vitest-axe";
import { describe, expect, it, vi } from "vitest";
import { Answer, AnswerList, NestedAnswers, pressChoice, pressMarked } from "./Answers";

function Item({ onPress = vi.fn(), nested = false, pending = false }: { onPress?: (name: string) => void; nested?: boolean; pending?: boolean }) {
  return (
    <div data-testid="item">
      <h2 id="question">Kategori untuk Toko Serba Ada?</h2>
      <AnswerList labelledBy="question">
        <Answer n={1} primary label="Terima semua (2)" detail="Kategorinya sudah benar" disabled={pending} onClick={() => onPress("accept")} />
        <Answer n={2} topup label="Ini top up investasi" detail="Masuk ke produk, dihitung Tabungan" expanded={nested} controls="products" onClick={() => onPress("topup")} />
        <Answer n={10} label="Rekening ke-10" onClick={() => onPress("tenth")} />
      </AnswerList>
      {nested && (
        <NestedAnswers id="products" title="Ini top up investasi. Ke produk mana?">
          <Answer n={1} label="Reksa Dana A" detail="Dicatat per nominal" onClick={() => onPress("fund-a")} />
        </NestedAnswers>
      )}
    </div>
  );
}

describe("numbered answers (apply-markas-tinjau D8)", () => {
  it("makes each answer one whole-row button named by its label and detail, with a key cap only for 1–9", () => {
    render(<Item />);
    const group = screen.getByRole("group", { name: "Kategori untuk Toko Serba Ada?" });
    const answers = within(group).getAllByRole("button");
    expect(answers).toHaveLength(3);
    expect(answers[0]).toHaveAccessibleName("Terima semua (2) Kategorinya sudah benar");
    expect(answers.map((answer) => answer.querySelector("kbd")?.textContent ?? null)).toEqual(["1", "2", null]);
    expect(answers[0].querySelector("kbd")).toHaveAttribute("aria-hidden", "true");
    expect(answers[0]).toHaveClass("answer");
    expect(answers[0].querySelectorAll("button")).toHaveLength(0);
  });

  it("marks E and T and names the keys of each answer", () => {
    render(<Item nested />);
    const [accept, topup, tenth] = within(screen.getByRole("group", { name: "Kategori untuk Toko Serba Ada?" })).getAllByRole("button");
    expect(accept).toHaveAttribute("data-primary-choice");
    expect(accept).toHaveAttribute("aria-keyshortcuts", "1 E");
    expect(topup).toHaveAttribute("data-topup");
    expect(topup).toHaveAttribute("aria-keyshortcuts", "2 T");
    expect(topup).toHaveAttribute("aria-expanded", "true");
    expect(tenth).not.toHaveAttribute("aria-keyshortcuts");
    expect(tenth).toHaveAttribute("data-choice", "10");
  });

  it("presses the nth answer of the last open nested list, else of the item, and the marked ones", () => {
    const pressed: string[] = [];
    const { rerender } = render(<Item onPress={(name) => pressed.push(name)} />);
    const root = screen.getByTestId("item");
    expect(pressChoice(root, 2)).toBe(true);
    expect(pressChoice(root, 7)).toBe(false);
    rerender(<Item onPress={(name) => pressed.push(name)} nested />);
    expect(pressChoice(root, 1)).toBe(true);
    expect(pressMarked(root, "data-primary-choice")).toBe(true);
    expect(pressMarked(root, "data-topup")).toBe(true);
    expect(pressed).toEqual(["topup", "fund-a", "accept", "topup"]);
  });

  it("does not press a disabled answer while a request is pending", () => {
    const onPress = vi.fn();
    render(<Item onPress={onPress} pending />);
    const root = screen.getByTestId("item");
    expect(screen.getByRole("button", { name: /Terima semua/ })).toBeDisabled();
    expect(pressChoice(root, 1)).toBe(false);
    expect(pressMarked(root, "data-primary-choice")).toBe(false);
    expect(onPress).not.toHaveBeenCalled();
  });

  it("has no axe violations with a nested list open", async () => {
    const { container } = render(<Item nested />);
    expect(screen.getByRole("group", { name: "Ini top up investasi. Ke produk mana?" })).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });
});
