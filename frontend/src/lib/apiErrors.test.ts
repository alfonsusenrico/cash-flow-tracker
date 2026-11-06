import { afterEach, describe, expect, it, vi } from "vitest";
import en from "@/messages/en.json";
import id from "@/messages/id.json";
import { api } from "./api";
import { configureApiMessages, configureApiRegion } from "./i18n/runtime";

/** Error codes rendered from the `errors` catalogue in the interface language (add-language-system D8). */
function reject(detail: unknown, status = 400) {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status, json: async () => ({ detail }) }));
}

afterEach(() => {
  vi.unstubAllGlobals();
  configureApiMessages("id", id);
  configureApiRegion("ID");
});

describe("API error codes", () => {
  it.each([
    ["id", "Nama pengguna atau kata sandi salah."],
    ["en", "Wrong username or password."],
  ] as const)("renders invalid_credentials in %s", async (language, message) => {
    configureApiMessages(language, language === "en" ? en : id);
    reject({ code: "invalid_credentials", params: {}, message: "Invalid username or password" }, 401);
    await expect(api.post("/auth/login", {})).rejects.toMatchObject({ status: 401, message });
  });

  it("renders a parameterised code with its params", async () => {
    configureApiMessages("en", en);
    reject({ code: "password_too_short", params: { min: 8 }, message: "Password must be at least 8 characters" });
    await expect(api.post("/auth/register", {})).rejects.toMatchObject({ message: "Password must have at least 8 characters" });
    configureApiMessages("id", id);
    await expect(api.post("/auth/register", {})).rejects.toMatchObject({ message: "Kata sandi minimal 8 karakter" });
  });

  it("renders every code the sign-in, sign-up and settings endpoints send, in both languages", async () => {
    const codes = ["too_many_attempts", "invalid_credentials", "username_required", "password_too_short", "invalid_invite_code",
      "username_taken", "display_name_empty", "no_settings_to_update", "invalid_language", "invalid_region"];
    for (const catalogue of [en, id]) {
      for (const code of codes) expect(Object.keys(catalogue.errors)).toContain(code);
    }
  });

  it("renders the insufficient-funds amounts for the account's region", async () => {
    configureApiMessages("en", en);
    configureApiRegion("US");
    reject({ code: "insufficient_funds", required_amount: 250_000, available_amount: 100_000 }, 409);
    await expect(api.post("/transactions", {})).rejects.toMatchObject({
      message: "Not enough balance. Rp 100,000 available of the Rp 250,000 needed.",
    });
  });

  it("names FastAPI's schema errors with form_invalid", async () => {
    configureApiMessages("en", en);
    reject([{ loc: ["body", "currency"], msg: "Input should be 'IDR' or 'USD'", type: "literal_error" }], 422);
    await expect(api.patch("/auth/settings", {})).rejects.toMatchObject({ status: 422, message: "Some fields are not valid. Check them and try again." });
  });

  it("shows the server's message for an unknown code and a plain-string detail as it is", async () => {
    configureApiMessages("en", en);
    reject({ code: "something_new", params: {}, message: "Something new went wrong" });
    await expect(api.get("/x")).rejects.toMatchObject({ message: "Something new went wrong" });
    reject("Category not found", 404);
    await expect(api.get("/x")).rejects.toMatchObject({ message: "Category not found" });
  });
});
