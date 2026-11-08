import { describe, expect, it } from "vitest";
import { chunkIds, newReplyId, reviewErrorMessage } from "./review";

describe("review helpers", () => {
  it("splits large groups into batches the backend accepts", () => {
    const ids = Array.from({ length: 450 }, (_, index) => `id-${index}`);
    expect(chunkIds(ids).map((chunk) => chunk.length)).toEqual([200, 200, 50]);
  });

  it("keeps the message the API client rendered, and has none without one", () => {
    expect(reviewErrorMessage({ detail: { code: "stale_question", message: "Pertanyaan ini sudah dijawab atau berubah." } })).toBe("Pertanyaan ini sudah dijawab atau berubah.");
    expect(reviewErrorMessage({ detail: { code: "x", message: "Pesan server." } })).toBe("Pesan server.");
    expect(reviewErrorMessage(new Error("Gagal"))).toBe("Gagal");
    expect(reviewErrorMessage({ detail: { code: "x" } })).toBeNull();
  });

  it("creates version 4 reply ids", () => {
    expect(newReplyId()).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });
});
