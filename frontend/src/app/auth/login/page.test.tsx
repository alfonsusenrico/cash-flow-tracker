import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import LoginPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn() } }));

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

    await user.type(screen.getByRole("textbox", { name: "Nama Tampilan" }), "Enrico");
    await user.type(screen.getByRole("textbox", { name: "Nama Pengguna" }), "enrico01");
    await user.type(screen.getByLabelText("Kata Sandi"), "short");
    await user.type(screen.getByRole("textbox", { name: "Kode Undangan" }), "TESTCODE");
    await user.click(screen.getByRole("button", { name: "Daftar Akun" }));
    expect(api.post).not.toHaveBeenCalled();

    await user.type(screen.getByLabelText("Kata Sandi"), "-valid");
    await user.click(screen.getByRole("button", { name: "Daftar Akun" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/auth/register", {
      username: "enrico01",
      password: "short-valid",
      name: "Enrico",
      invite_code: "TESTCODE",
    }));
    expect(screen.getByRole("alert")).toHaveTextContent("Registration unavailable");
  });
});
