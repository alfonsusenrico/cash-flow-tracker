import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactElement } from "react";
import { axe } from "vitest-axe";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  Card,
  CardHeader,
  ChipGroup,
  EmptyState,
  ErrorState,
  Kpi,
  KpiStrip,
  MoneyBar,
  Pill,
  Segmented,
  Skeleton,
  type MoneySegment,
} from "./primitives";

const SEGMENTS: MoneySegment[] = [
  { key: "need", label: "Kebutuhan", amount: 1_200_000, display: "Rp 1.200.000", className: "bg-need" },
  { key: "want", label: "Keinginan", amount: 450_000, display: "Rp 450.000", className: "bg-want" },
  { key: "unexpected", label: "Tak terduga", amount: 0, display: "Rp 0", className: "bg-unexpected" },
];

const PRIMITIVES: [string, () => ReactElement][] = [
  [
    "Card with CardHeader",
    () => (
      <Card labelledBy="card-title">
        <CardHeader
          id="card-title"
          title="Perlu ditinjau"
          icon="inbox"
          count={3}
          countLabel="3 transaksi"
          caption="Dari notifikasi bank"
          action={<button type="button">Buka</button>}
        />
        <p>Isi kartu</p>
      </Card>
    ),
  ],
  [
    "KpiStrip with Kpi",
    () => (
      <KpiStrip label="Ringkasan siklus">
        <Kpi icon="wallet" label="Aman dipakai hari ini" value="Rp 214.000" caption="12 hari lagi gajian" />
        <Kpi icon="receipt" label="Keluar siklus ini" value="belum tersedia" valueIsWords />
      </KpiStrip>
    ),
  ],
  [
    "Pill",
    () => (
      <p>
        <Pill tone="in" icon="check">
          Masuk
        </Pill>
        <Pill tone="warn">Hampir batas</Pill>
      </p>
    ),
  ],
  [
    "Segmented",
    () => (
      <Segmented
        label="Periode"
        value="cycle"
        onChange={() => {}}
        options={[
          { value: "cycle", label: "Siklus" },
          { value: "month", label: "Bulan" },
        ]}
      />
    ),
  ],
  [
    "ChipGroup",
    () => (
      <ChipGroup
        label="Pilar"
        selected={["need"]}
        onToggle={() => {}}
        options={[
          { value: "need", label: "Kebutuhan", count: 4 },
          { value: "want", label: "Keinginan" },
        ]}
      />
    ),
  ],
  ["MoneyBar", () => <MoneyBar label="Pengeluaran per pilar" segments={SEGMENTS} />],
  ["Skeleton", () => <Skeleton className="h-6 w-32" />],
  [
    "EmptyState",
    () => (
      <EmptyState title="Semua sudah ditinjau" action={<button type="button">Ke Transaksi</button>}>
        Tidak ada yang menunggu.
      </EmptyState>
    ),
  ],
  ["ErrorState", () => <ErrorState message="Ringkasan gagal dimuat." onRetry={() => {}} />],
];

describe.each([
  ["light", false],
  ["dark", true],
])("primitives in the %s theme", (_theme, dark) => {
  afterEach(() => document.documentElement.classList.remove("dark"));

  it.each(PRIMITIVES)("%s has no axe violations", async (_name, element) => {
    if (dark) document.documentElement.classList.add("dark");
    const { container } = render(element());
    expect((await axe(container)).violations).toHaveLength(0);
  });
});

describe("primitive behaviour", () => {
  it("CardHeader announces its count in words", () => {
    render(<CardHeader title="Perlu ditinjau" count={3} countLabel="3 transaksi" />);
    const heading = screen.getByRole("heading", { level: 2 });
    expect(heading).toHaveTextContent("3 transaksi");
    expect(within(heading).getByText("3")).toHaveAttribute("aria-hidden", "true");
  });

  it("MoneyBar hides the bar and keeps every segment as text", () => {
    const { container } = render(<MoneyBar label="Pengeluaran per pilar" segments={SEGMENTS} />);
    const bar = container.querySelector(".ws-moneybar")!;
    expect(bar).toHaveAttribute("aria-hidden", "true");
    expect(bar.children).toHaveLength(2);
    const legend = screen.getByRole("list", { name: "Pengeluaran per pilar" });
    const items = within(legend).getAllByRole("listitem");
    expect(items.map((item) => item.textContent)).toEqual(["KebutuhanRp 1.200.000", "KeinginanRp 450.000", "Tak terdugaRp 0"]);
  });

  it("MoneyBar leaves the text to the caller when its legend is off", () => {
    const { container } = render(<MoneyBar segments={SEGMENTS} legend={false} />);
    expect(screen.queryByRole("list")).not.toBeInTheDocument();
    expect(container.querySelector(".ws-moneybar")).toHaveAttribute("aria-hidden", "true");
  });

  it("Segmented marks the chosen option and reports a change", async () => {
    const onChange = vi.fn();
    render(
      <Segmented
        label="Periode"
        value="cycle"
        onChange={onChange}
        options={[
          { value: "cycle", label: "Siklus" },
          { value: "month", label: "Bulan" },
        ]}
      />,
    );
    expect(screen.getByRole("button", { name: "Siklus" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button", { name: "Bulan" })).toHaveAttribute("aria-pressed", "false");
    await userEvent.click(screen.getByRole("button", { name: "Bulan" }));
    expect(onChange).toHaveBeenCalledWith("month");
  });

  it("ChipGroup marks selected chips and toggles one", async () => {
    const onToggle = vi.fn();
    render(
      <ChipGroup
        label="Pilar"
        selected={["need"]}
        onToggle={onToggle}
        options={[
          { value: "need", label: "Kebutuhan" },
          { value: "want", label: "Keinginan" },
        ]}
      />,
    );
    expect(screen.getByRole("group", { name: "Pilar" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Kebutuhan" })).toHaveAttribute("aria-pressed", "true");
    await userEvent.click(screen.getByRole("button", { name: "Keinginan" }));
    expect(onToggle).toHaveBeenCalledWith("want");
  });

  it("Skeleton is hidden from assistive technology", () => {
    const { container } = render(<Skeleton />);
    expect(container.firstChild).toHaveAttribute("aria-hidden", "true");
  });

  it("ErrorState is an alert and its retry calls back", async () => {
    const onRetry = vi.fn();
    render(<ErrorState message="Ringkasan gagal dimuat." onRetry={onRetry} />);
    expect(screen.getByRole("alert")).toHaveTextContent("Ringkasan gagal dimuat.");
    await userEvent.click(screen.getByRole("button", { name: "Coba lagi" }));
    expect(onRetry).toHaveBeenCalledOnce();
  });

  it("ErrorState without a retry shows no button", () => {
    render(<ErrorState message="Ringkasan gagal dimuat." />);
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
});
