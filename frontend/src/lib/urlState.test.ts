import { afterEach, describe, expect, it, vi } from "vitest";
import { pushSearch, replaceSearch, sameRouteClick } from "./urlState";

afterEach(() => window.history.replaceState(null, "", "/"));

const click = (extra: Partial<{ metaKey: boolean; button: number }> = {}) => {
  const event = { preventDefault: vi.fn(), metaKey: false, ctrlKey: false, shiftKey: false, altKey: false, button: 0, ...extra };
  return event;
};

describe("filter URL writes", () => {
  it("clears back to the plain path from a URL with a query (the Next.js defect it avoids)", () => {
    window.history.replaceState({ __NA: true, tree: "next" }, "", "/ledger?type=income");
    replaceSearch("/ledger");
    expect(`${window.location.pathname}${window.location.search}`).toBe("/ledger");
    // Next's own keys would make Next.js skip its useSearchParams sync.
    expect(window.history.state).toBeNull();
  });

  it("adds an entry for a phone sheet so back returns to the list", () => {
    window.history.replaceState(null, "", "/ledger?period=cycle");
    pushSearch("/ledger?period=cycle&edit=a", { ledgerSheet: "a" });
    expect(window.history.state).toEqual({ ledgerSheet: "a" });
    expect(window.location.search).toBe("?period=cycle&edit=a");
  });

  it("handles a navigation link to the same path itself, and leaves other paths and modified clicks alone", () => {
    window.history.replaceState(null, "", "/ledger?type=income");
    const same = click();
    sameRouteClick("/ledger?period=cycle", "/ledger")(same);
    expect(same.preventDefault).toHaveBeenCalled();
    expect(window.location.search).toBe("?period=cycle");

    const other = click();
    sameRouteClick("/review", "/ledger")(other);
    expect(other.preventDefault).not.toHaveBeenCalled();

    const newTab = click({ metaKey: true });
    sameRouteClick("/ledger?period=cycle", "/ledger")(newTab);
    expect(newTab.preventDefault).not.toHaveBeenCalled();
  });
});
