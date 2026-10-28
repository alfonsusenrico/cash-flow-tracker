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
});
