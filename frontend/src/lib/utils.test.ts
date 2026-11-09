import { describe, expect, it } from "vitest";
import { localDatetimeToISO, toDatetimeLocal } from "./utils";

describe("financial date-time input", () => {
  it("retains nonzero seconds when opening and submitting a stored timestamp", () => {
    const timestamp = "2026-09-20T10:34:27.000Z";
    const inputValue = toDatetimeLocal(timestamp);

    expect(inputValue).toMatch(/T\d{2}:\d{2}:27$/);
    expect(new Date(localDatetimeToISO(inputValue)).toISOString()).toBe(timestamp);
  });
});
