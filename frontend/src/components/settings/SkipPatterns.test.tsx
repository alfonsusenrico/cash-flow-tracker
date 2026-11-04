import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, createSkipPattern, getSkipPatterns, stopSkipPattern, type SkipPattern } from "@/lib/api";
import { SkipPatterns } from "./SkipPatterns";

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ApiError: actual.ApiError, getSkipPatterns: vi.fn(), createSkipPattern: vi.fn(), stopSkipPattern: vi.fn() };
});

const INSTITUTIONS = [
  { institution: "bca" as const, label: "BCA" },
  { institution: "gopay" as const, label: "GoPay" },
  { institution: "jago" as const, label: "Bank Jago" },
];

const JAGO: SkipPattern = {
  id: "pattern-1", redundant_institution: "jago", redundant_label: "Bank Jago", direction: "income",
  kept_institution: "gopay", kept_label: "GoPay", source: "confirmation", created_at: "2026-11-04T05:00:00Z",
  skipped_count: 3, last_skipped_at: "2026-11-04T06:00:00Z",
};

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  render(<QueryClientProvider client={client}><SkipPatterns /></QueryClientProvider>);
  return userEvent.setup();
}

const section = () => screen.findByRole("region", { name: "Notifikasi ganda" });

describe("Notifikasi ganda", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getSkipPatterns).mockResolvedValue({ ok: true, patterns: [JAGO], institutions: INSTITUTIONS });
    vi.mocked(stopSkipPattern).mockResolvedValue({ ok: true });
  });

  it("lists a pattern with where it came from and how often it skipped", async () => {
    show();
    const card = await section();
    expect(await within(card).findByText("Bank Jago, uang masuk")).toBeInTheDocument();
    expect(within(card).getByText("Dilewati bila GoPay sudah melaporkan nominal yang sama · dari Tinjau, 4 Nov · 3 kali dilewati")).toBeInTheDocument();
    expect((await axe(card)).violations).toHaveLength(0);
  });

  it("stops a pattern only after confirmation", async () => {
    const user = show();
    const card = await section();
    await user.click(await within(card).findByRole("button", { name: "Berhenti melewati Bank Jago, uang masuk" }));
    expect(stopSkipPattern).not.toHaveBeenCalled();
    expect(within(card).getByText("Berhenti melewati pola Bank Jago, uang masuk?")).toBeInTheDocument();
    await user.click(within(card).getByRole("button", { name: "Ya, berhenti" }));
    await waitFor(() => expect(stopSkipPattern).toHaveBeenCalledWith("pattern-1"));
    expect(await within(card).findByText("Pola dihentikan. Laporan serupa akan ditanyakan lagi di Tinjau.")).toBeInTheDocument();
  });

  it("adds a pattern by hand, offering only apps that sent notifications and never the same app twice", async () => {
    vi.mocked(getSkipPatterns).mockResolvedValue({ ok: true, patterns: [], institutions: INSTITUTIONS });
    vi.mocked(createSkipPattern).mockResolvedValue({ ok: true, pattern: { ...JAGO, source: "manual", skipped_count: 0 } });
    const user = show();
    const card = await section();
    expect(await within(card).findByText(/Belum ada pola\. Pola ditambahkan lewat .Benar, satu transaksi. di Tinjau, atau di sini\./)).toBeInTheDocument();
    const save = within(card).getByRole("button", { name: "Simpan pola" });
    expect(save).toBeDisabled();
    await user.selectOptions(within(card).getByRole("combobox", { name: "Aplikasi yang dilewati" }), "jago");
    const kept = within(card).getByRole("combobox", { name: "Aplikasi yang dipakai" });
    expect(within(kept).getAllByRole("option").map((option) => option.textContent)).toEqual(["Pilih aplikasi", "BCA", "GoPay"]);
    await user.selectOptions(kept, "gopay");
    expect(within(card).getByRole("combobox", { name: "Jenis" })).toHaveValue("income");
    await user.click(save);
    await waitFor(() => expect(createSkipPattern).toHaveBeenCalledWith({
      redundant_institution: "jago", direction: "income", kept_institution: "gopay",
    }));
    expect(await within(card).findByText("Pola Bank Jago, uang masuk disimpan.")).toBeInTheDocument();
    expect((await axe(card)).violations).toHaveLength(0);
  });

  it.each([
    [409, "skip_pattern_exists", "Pola ini sudah ada."],
    [422, "invalid_skip_pattern", "Pilih aplikasi yang sudah pernah mengirim notifikasi."],
  ])("shows the server's %s message in the card", async (status, code, message) => {
    vi.mocked(createSkipPattern).mockRejectedValue(new ApiError(status, { code, message }));
    const user = show();
    const card = await section();
    await user.selectOptions(await within(card).findByRole("combobox", { name: "Aplikasi yang dilewati" }), "jago");
    await user.selectOptions(within(card).getByRole("combobox", { name: "Aplikasi yang dipakai" }), "gopay");
    await user.click(within(card).getByRole("button", { name: "Simpan pola" }));
    expect(await within(card).findByRole("alert")).toHaveTextContent(message);
  });

  it("explains that a pattern needs two apps before any notification has come in", async () => {
    vi.mocked(getSkipPatterns).mockResolvedValue({ ok: true, patterns: [], institutions: [INSTITUTIONS[2]] });
    show();
    const card = await section();
    expect(await within(card).findByText("Pola butuh dua aplikasi yang sudah pernah mengirim notifikasi.")).toBeInTheDocument();
    expect(within(card).queryByRole("button", { name: "Simpan pola" })).not.toBeInTheDocument();
  });

  it("says when the list cannot be loaded", async () => {
    vi.mocked(getSkipPatterns).mockRejectedValue(new Error("offline"));
    show();
    expect(await within(await section()).findByRole("alert")).toHaveTextContent("Daftar pola gagal dimuat.");
  });
});
