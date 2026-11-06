import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Icon } from "./Icon";

function markup(name: string) {
  return render(<Icon name={name} />).container.innerHTML;
}

describe("Icon", () => {
  it.each(["coffee", "users", "receipt", "arrows-right-left"])("draws the seeded category icon %s instead of the fallback tag", (name) => {
    expect(markup(name)).not.toBe(markup("tag"));
  });

  it("falls back to the tag for an unknown name", () => {
    expect(markup("unknown-icon")).toBe(markup("tag"));
  });

  it("draws on the 24 grid with a 1.5 px round stroke and stays hidden from assistive technology", () => {
    const { container } = render(<Icon name="bell" />);
    const svg = container.querySelector("svg")!;
    expect(svg).toHaveAttribute("viewBox", "0 0 24 24");
    expect(svg).toHaveAttribute("stroke-width", "1.5");
    expect(svg).toHaveAttribute("stroke-linecap", "round");
    expect(svg).toHaveAttribute("stroke-linejoin", "round");
    expect(svg).toHaveAttribute("aria-hidden", "true");
  });

  it("uses the prototype's drawings for the shell and keeps the app's names as aliases", () => {
    const pathOf = (name: string) => render(<Icon name={name} />).container.querySelector("path")?.getAttribute("d");
    expect(pathOf("home")).toBe("M3 10.5 12 3l9 7.5V21h-6v-6H9v6H3z");
    expect(pathOf("trend")).toBe("M4 17l5-5 4 4 7-8");
    expect(pathOf("sliders")).toBe(pathOf("settings"));
    expect(pathOf("panel-left")).toBe(pathOf("panel"));
    expect(pathOf("eye-off")).toBe("M3 3l18 18M10.6 5.1A10 10 0 0 1 12 5c6.5 0 10 7 10 7a17 17 0 0 1-3.2 4.1M6.6 6.6C3.8 8.4 2 12 2 12s3.5 7 10 7c1.9 0 3.5-.6 4.9-1.4M9.9 9.9a3 3 0 0 0 4.2 4.2");
  });

  it("draws the dotted icons as three filled dots", () => {
    const { container } = render(<Icon name="more" />);
    const dots = container.querySelectorAll("circle");
    expect(dots).toHaveLength(3);
    expect(dots[0]).toHaveAttribute("fill", "currentColor");
    expect(dots[0]).toHaveAttribute("r", "1.6");
  });
});
