import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";

afterEach(() => vi.unstubAllGlobals());

describe("API financial errors", () => {
  it("explains a settled request rejected for insufficient funds", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: false,
      status: 409,
      json: async () => ({
        detail: {
          code: "insufficient_funds",
          required_amount: 250_000,
          available_amount: 100_000,
          account_id: "account-1",
        },
      }),
    }));

    await expect(api.post("/transactions", {})).rejects.toMatchObject({
      status: 409,
      message: "Saldo tidak mencukupi. Tersedia Rp 100.000 dari Rp 250.000 yang dibutuhkan.",
      detail: { code: "insufficient_funds", account_id: "account-1" },
    });
  });

  it("explains an attempted sale above the owned position", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: false,
      status: 409,
      json: async () => ({
        detail: {
          code: "insufficient_units",
          available_units: 12,
          required_units: 15,
        },
      }),
    }));

    await expect(api.post("/transactions", {})).rejects.toMatchObject({
      message: "Unit investasi tidak mencukupi. Tersedia 12 dari 15 unit yang dibutuhkan.",
    });
  });
});
