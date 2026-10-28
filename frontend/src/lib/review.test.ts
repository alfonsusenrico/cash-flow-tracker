import { describe, expect, it } from "vitest";
import { chunkIds, newReplyId, reviewErrorMessage, reviewReasonLabel } from "./review";

describe("review helpers", () => {
  it("labels known reasons in Indonesian and shows unknown codes as sent", () => {
    expect(reviewReasonLabel("uncertain_category")).toBe("Kategori belum pasti");
    expect(reviewReasonLabel("awaiting_counterpart")).toBe("Menunggu rekening pasangan");
    expect(reviewReasonLabel("new_reason")).toBe("new_reason");
  });

  it("splits large groups into batches the backend accepts", () => {
    const ids = Array.from({ length: 450 }, (_, index) => `id-${index}`);
    expect(chunkIds(ids).map((chunk) => chunk.length)).toEqual([200, 200, 50]);
  });

  it("explains coded errors and keeps backend messages", () => {
    expect(reviewErrorMessage({ detail: { code: "stale_question" } })).toMatch(/sudah dijawab/);
    expect(reviewErrorMessage({ detail: { code: "x", message: "Pesan server." } })).toBe("Pesan server.");
    expect(reviewErrorMessage(new Error("Gagal"))).toBe("Gagal");
  });

  it("creates version 4 reply ids", () => {
    expect(newReplyId()).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });
});
