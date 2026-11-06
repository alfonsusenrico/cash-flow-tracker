import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { readLanguageCookie } from "@/lib/i18n/cookie";
import LoginPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn() } }));
const router = vi.hoisted(() => ({ refresh: vi.fn() }));
vi.mock("next/navigation", () => ({ useRouter: () => router }));

describe("registration form", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockRejectedValue(new Error("No session"));
    vi.mocked(api.post).mockRejectedValue(new Error("Registration unavailable"));
  });

  it("submits the optional display name and enforces the stated password minimum", async () => {
    const user = userEvent.setup();
    const { container } = render(<LoginPage />);
    await user.click(screen.getByRole("button", { name: "Daftar sekarang" }));
    expect((await axe(container)).violations).toHaveLength(0);

    await user.type(screen.getByRole("textbox", { name: "Nama Tampilan" }), "Raka Purnama");
    await user.type(screen.getByRole("textbox", { name: "Nama Pengguna" }), "raka01");
    await user.type(screen.getByLabelText("Kata Sandi"), "short");
    await user.type(screen.getByRole("textbox", { name: "Kode Undangan" }), "TESTCODE");
    await user.click(screen.getByRole("button", { name: "Daftar Akun" }));
    expect(api.post).not.toHaveBeenCalled();

    await user.type(screen.getByLabelText("Kata Sandi"), "-valid");
    await user.click(screen.getByRole("button", { name: "Daftar Akun" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/auth/register", {
      username: "raka01",
      password: "short-valid",
      name: "Raka Purnama",
      invite_code: "TESTCODE",
      language: "id",
      region: "ID",
    }));
    expect(screen.getByRole("alert")).toHaveTextContent("Registration unavailable");
  });
});

describe("sign-in and sign-up in English (add-language-system)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    document.cookie = "cft_language=; Path=/; Max-Age=0";
    vi.mocked(api.get).mockRejectedValue(new Error("No session"));
    vi.mocked(api.post).mockRejectedValue(new Error("Registration unavailable"));
  });

  it("signs in in English, with both languages named in their own words", async () => {
    const { container } = renderWithIntl(<LoginPage />, { language: "en" });
    expect(screen.getByText("Sign in", { selector: "p" })).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "Username" })).toBeInTheDocument();
    expect(screen.getByLabelText("Password")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Sign in" })).toHaveAttribute("type", "submit");
    const languages = screen.getByRole("navigation", { name: "Language" });
    expect(within(languages).getByRole("button", { name: "English" })).toHaveAttribute("lang", "en");
    expect(within(languages).getByRole("button", { name: "Bahasa Indonesia" })).toHaveAttribute("lang", "id");
    expect(within(languages).getByRole("button", { name: "English" })).toHaveAttribute("aria-current", "true");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("switches to Indonesian before sign-in with the cookie and a refresh, no reload", async () => {
    const user = userEvent.setup();
    renderWithIntl(<LoginPage />, { language: "en" });
    await user.click(screen.getByRole("button", { name: "Bahasa Indonesia" }));
    expect(readLanguageCookie()).toBe("id");
    expect(router.refresh).toHaveBeenCalledTimes(1);
  });

  it("sends the interface language and the chosen region at sign-up, and words the password minimum", async () => {
    const user = userEvent.setup();
    renderWithIntl(<LoginPage />, { language: "en" });
    await user.click(screen.getByRole("button", { name: "Create one" }));
    expect(screen.getByRole("combobox", { name: "Region" })).toHaveAccessibleDescription("Sets number and date style. Language changes words only.");
    await user.type(screen.getByRole("textbox", { name: "Username" }), "dana01");
    await user.type(screen.getByLabelText("Password"), "short");
    await user.type(screen.getByRole("textbox", { name: "Invite code" }), "TESTCODE");
    await user.click(screen.getByRole("button", { name: "Create account" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Password must have at least 8 characters");
    expect(api.post).not.toHaveBeenCalled();
    await user.type(screen.getByLabelText("Password"), "-valid");
    await user.selectOptions(screen.getByRole("combobox", { name: "Region" }), "US");
    await user.click(screen.getByRole("button", { name: "Create account" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/auth/register", {
      username: "dana01",
      password: "short-valid",
      name: undefined,
      invite_code: "TESTCODE",
      language: "en",
      region: "US",
    }));
  });

  it("adopts the account's language from the sign-in response", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true, user: { language: "id", region: "ID" } });
    const replace = vi.fn();
    vi.stubGlobal("location", { ...window.location, replace, search: "" });
    try {
      const user = userEvent.setup();
      renderWithIntl(<LoginPage />, { language: "en" });
      await user.type(screen.getByRole("textbox", { name: "Username" }), "raka01");
      await user.type(screen.getByLabelText("Password"), "contoh-sandi");
      await user.click(screen.getByRole("button", { name: "Sign in" }));
      await waitFor(() => expect(replace).toHaveBeenCalledWith("/"));
      expect(readLanguageCookie()).toBe("id");
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
