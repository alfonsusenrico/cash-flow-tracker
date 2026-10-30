import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { SalarySettingsSection } from "./SalarySettingsSection";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), del: vi.fn() } }));

const income = { id: "income-1", amount: 100000, date: "2026-01-01T12:00:00Z", merchant: "Synthetic employer", notes: "Bonus" };

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.get).mockImplementation(async (path) => {
    if (path === "/salary/source") return { source: { field: "merchant", value: "Synthetic employer" } };
    if (path === "/salary/candidates") return { candidates: [{ ...income, confirmed: false, needs_review: true }] };
    if (path === "/salary/invalidated-detections") return { detections: [] };
    if (path === "/salary/receipts") return { receipts: [] };
    if (path.startsWith("/transactions?")) return { transactions: [income], total: 1 };
    throw new Error(`Unexpected request ${path}`);
  });
  vi.mocked(api.post).mockResolvedValue({ ok: true });
  vi.mocked(api.put).mockResolvedValue({ ok: true });
});

it("allows source selection and explicitly rejects ambiguous income", async () => {
  const user = userEvent.setup();
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const { container } = render(<QueryClientProvider client={client}><SalarySettingsSection open /></QueryClientProvider>);
  expect(await screen.findByText(/sumber: synthetic employer/i)).toBeInTheDocument();
  expect((await axe(container)).violations).toHaveLength(0);
  await user.selectOptions(screen.getByRole("combobox", { name: /pilih pemasukan/i }), income.id);
  await user.click(screen.getByRole("button", { name: /gunakan sebagai sumber/i }));
  await waitFor(() => expect(api.put).toHaveBeenCalledWith("/salary/source", { transaction_id: income.id }));
  await user.click(screen.getByRole("button", { name: /^Bukan gaji: / }));
  await waitFor(() => expect(api.post).toHaveBeenCalledWith("/salary/reject", { transaction_id: income.id }));
});

function renderSection() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={client}><SalarySettingsSection open /></QueryClientProvider>);
}

it("names each review action after its income and works from the keyboard", async () => {
  const second = { ...income, id: "income-2", amount: 250000, merchant: "Synthetic employer", notes: "Gaji" };
  vi.mocked(api.get).mockImplementation(async (path) => {
    if (path === "/salary/source") return { source: { field: "merchant", value: "Synthetic employer" } };
    if (path === "/salary/candidates") return { candidates: [
      { ...income, confirmed: false, needs_review: true },
      { ...second, confirmed: false, needs_review: true },
    ] };
    if (path === "/salary/invalidated-detections") return { detections: [] };
    if (path === "/salary/receipts") return { receipts: [] };
    if (path.startsWith("/transactions?")) return { transactions: [income, second], total: 2 };
    throw new Error(`Unexpected request ${path}`);
  });
  const user = userEvent.setup();
  renderSection();
  const confirmButtons = await screen.findAllByRole("button", { name: /^Konfirmasi gaji: / });
  expect(new Set(confirmButtons.map((button) => button.getAttribute("aria-label"))).size).toBe(2);
  confirmButtons[1].focus();
  await user.keyboard("{Enter}");
  await waitFor(() => expect(api.post).toHaveBeenCalledWith("/salary/receipts", { transaction_id: "income-2" }));
});

it("explains the empty state without inventing a payday", async () => {
  vi.mocked(api.get).mockImplementation(async (path) => {
    if (path === "/salary/source") return { source: null };
    if (path === "/salary/candidates") return { candidates: [] };
    if (path === "/salary/invalidated-detections") return { detections: [] };
    if (path === "/salary/receipts") return { receipts: [] };
    if (path.startsWith("/transactions?")) return { transactions: [], total: 0 };
    throw new Error(`Unexpected request ${path}`);
  });
  const { container } = renderSection();
  expect(await screen.findByText(/sumber gaji belum dipilih/i)).toHaveTextContent("bulan kalender");
  expect(screen.getByText("Belum ada pemasukan tercatat yang bisa dipilih.")).toBeInTheDocument();
  expect(screen.getByText("Tidak ada pemasukan yang perlu ditinjau.")).toBeInTheDocument();
  expect(screen.getByText("Belum ada gaji terkonfirmasi.")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: /periksa pemasukan/i })).not.toBeInTheDocument();
  expect((await axe(container)).violations).toHaveLength(0);
});

it("shows a same-day conflict refusal as an alert", async () => {
  vi.mocked(api.post).mockRejectedValue(new Error("Sudah ada gaji terkonfirmasi pada hari ini. Periksa transaksi tersebut."));
  const user = userEvent.setup();
  renderSection();
  await user.click(await screen.findByRole("button", { name: /^Konfirmasi gaji: / }));
  expect(await screen.findByRole("alert")).toHaveTextContent("Sudah ada gaji terkonfirmasi pada hari ini");
});
