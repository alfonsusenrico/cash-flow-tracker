import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { QuestionRate, type QuestionRateWeek } from "./QuestionRate";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn() } }));

function week(start: string, end: string, values: Partial<QuestionRateWeek> = {}): QuestionRateWeek {
  return {
    week_start: start, week_end: end, in_progress: false, recorded: 0, asked: 0, answered: 0, by_memory: 0,
    by_habit: 0, by_ai: 0, by_other: 0, corrections: 0, habit_corrections: 0, ...values,
  };
}

function renderPanel() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><QuestionRate /></QueryClientProvider>);
}

describe("question rate panel", () => {
  beforeEach(() => vi.clearAllMocks());

  it("shows the real weekly counts, newest week first, with the share asked", async () => {
    vi.mocked(api.get).mockResolvedValue({
      has_data: true,
      weeks: [
        week("2026-10-19", "2026-10-25", { recorded: 10, asked: 8, answered: 7, by_ai: 2 }),
        week("2026-10-26", "2026-11-01", {
          in_progress: true, recorded: 20, asked: 3, answered: 2, by_memory: 9, by_habit: 5, by_ai: 2, by_other: 1,
          corrections: 2, habit_corrections: 1,
        }),
      ],
    });
    const { container } = renderPanel();
    const panel = await screen.findByRole("region", { name: "Seberapa sering ditanya" });
    expect(api.get).toHaveBeenCalledWith("/dashboard/question-rate?weeks=8");
    const [current, previous] = within(panel).getAllByRole("listitem");
    expect(within(current).getByText("berjalan")).toBeInTheDocument();
    expect(current).toHaveTextContent("Ditanya 3 dari 20 (15%)");
    expect(current).toHaveTextContent("Memori9 (45%)");
    expect(current).toHaveTextContent("Kebiasaan5 (25%)");
    expect(current).toHaveTextContent("AI2 (10%)");
    expect(current).toHaveTextContent("Dijawab Anda2 (10%)");
    expect(current).toHaveTextContent("Koreksi: 2 (1 dari kebiasaan)");
    expect(previous).toHaveTextContent("Ditanya 8 dari 10 (80%)");
    expect(within(previous).queryByText("berjalan")).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("says a week had no recorded payments instead of showing zero shares", async () => {
    vi.mocked(api.get).mockResolvedValue({
      has_data: true,
      weeks: [week("2026-10-19", "2026-10-25"), week("2026-10-26", "2026-11-01", { in_progress: true, recorded: 1, by_habit: 1 })],
    });
    renderPanel();
    const panel = await screen.findByRole("region", { name: "Seberapa sering ditanya" });
    expect(within(panel).getByText("Tidak ada transaksi tercatat.")).toBeInTheDocument();
    expect(within(panel).queryByText(/NaN/)).not.toBeInTheDocument();
  });

  it("has an honest empty state", async () => {
    vi.mocked(api.get).mockResolvedValue({ has_data: false, weeks: [week("2026-10-26", "2026-11-01", { in_progress: true })] });
    const { container } = renderPanel();
    expect(await screen.findByText("Belum ada transaksi dari notifikasi atau ketikan dalam 8 minggu terakhir.")).toBeInTheDocument();
    expect(screen.queryByRole("listitem")).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("reports a failed load without figures", async () => {
    vi.mocked(api.get).mockRejectedValue(new Error("offline"));
    renderPanel();
    expect(await screen.findByText("Hitungan belum dapat dimuat.")).toBeInTheDocument();
    expect(screen.queryByText(/Ditanya/)).not.toBeInTheDocument();
  });
});
