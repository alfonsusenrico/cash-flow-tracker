import { describe, expect, it } from "vitest";
import { planDay } from "./planDay";

describe("planDay", () => {
  it("formats plan dates like the rest of the app", () => {
    expect(planDay("2026-11-03")).toBe("3 Nov");
    expect(planDay("2026-12-31")).toBe("31 Des");
  });
});
