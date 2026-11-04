import { describe, expect, it } from "vitest";
import { greetingName, localDatetimeToISO, toDatetimeLocal } from "./utils";

describe("financial date-time input", () => {
  it("retains nonzero seconds when opening and submitting a stored timestamp", () => {
    const timestamp = "2026-09-20T10:34:27.000Z";
    const inputValue = toDatetimeLocal(timestamp);

    expect(inputValue).toMatch(/T\d{2}:\d{2}:27$/);
    expect(new Date(localDatetimeToISO(inputValue)).toISOString()).toBe(timestamp);
  });
});

describe("greeting name", () => {
  it("capitalises the username and has no built-in name while the profile loads", () => {
    expect(greetingName({ username: "raka01" })).toBe("Raka01");
    expect(greetingName(null)).toBeNull();
    expect(greetingName({ username: "  " })).toBeNull();
  });
});

describe("greetingName", () => {
  it("greets with the display name", () => {
    expect(greetingName({ name: "Raka", username: "beranda_contoh" })).toBe("Raka");
    expect(greetingName({ name: "Raka Contoh", username: "beranda_contoh" })).toBe("Raka");
  });
  it("falls back to the capitalised username without a name", () => {
    expect(greetingName({ name: "  ", username: "beranda_contoh" })).toBe("Beranda_contoh");
    expect(greetingName({ username: null })).toBeNull();
  });
});
