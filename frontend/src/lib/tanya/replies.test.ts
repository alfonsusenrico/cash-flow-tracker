import { describe, expect, it } from "vitest";
import { greeting, partOfDay } from "./replies";

describe("assistant replies", () => {
  it("names the part of the day by the local hour", () => {
    const buckets = Array.from({ length: 24 }, (_, hour) => partOfDay(hour));
    expect(buckets.slice(4, 11).every((part) => part === "Pagi")).toBe(true);
    expect(buckets.slice(11, 15).every((part) => part === "Siang")).toBe(true);
    expect(buckets.slice(15, 19).every((part) => part === "Sore")).toBe(true);
    expect([...buckets.slice(19), ...buckets.slice(0, 4)].every((part) => part === "Malam")).toBe(true);
  });

  it("greets by first name, or without one", () => {
    expect(greeting(9, "Raka Contoh")).toBe("Pagi, Raka.");
    expect(greeting(20, "  ")).toBe("Malam.");
    expect(greeting(13, null)).toBe("Siang.");
  });
});
