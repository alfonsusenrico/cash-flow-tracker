import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { readFileSync } from "node:fs";
import path from "node:path";
import type { ReactElement } from "react";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FigureBlock, HeroFigure, SplitBar, type SplitSegment } from "./figures";
import { DayHeading, ListRow, Panel, PanelBlock, SectionHeader } from "./lists";
import { MaskedMoney } from "./money";
import { Card, CardHeader, ChipGroup, EmptyState, ErrorState, Kpi, KpiStrip, Pill, Segmented, Skeleton } from "./primitives";

const app = vi.hoisted(() => ({ hideBalances: false, currency: "IDR" as "IDR" | "USD" }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    hideBalances: app.hideBalances,
    bal: (amount: number) => {
      if (app.hideBalances) return app.currency === "USD" ? "$ ••••••" : "Rp ••••••";
      return `Rp ${amount.toLocaleString("id-ID")}`;
    },
  }),
}));
vi.mock("next/navigation", () => ({ usePathname: () => "/", useRouter: () => ({ push: vi.fn() }) }));

const SPLIT: SplitSegment[] = [
  { key: "spending", label: "Pengeluaran", amount: 3_502_990, color: "var(--out-solid)" },
  { key: "saving", label: "Ditabung & investasi", amount: 2_750_000, color: "var(--inv-solid)" },
  { key: "scheduled", label: "Terjadwal", amount: 989_000, color: "var(--warn-solid)" },
  { key: "safe", label: "Aman dipakai", amount: 2_276_410, color: "var(--in-solid)" },
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
  [
    "HeroFigure with SplitBar and FigureBlock",
    () => (
      <HeroFigure id="hero" label="Aman dipakai sampai gajian" mark="var(--in-solid)" figure="Rp 2.276.410" line="20 hari lagi sampai gajian 25 Nov"
        action={<button type="button" aria-label="Rincian aman dipakai sampai gajian">Rincian</button>}>
        <SplitBar label="Uang siklus ini dari Rp 9.518.400 masuk" emptyLabel="Uang siklus ini: belum ada uang masuk" total={9_518_400} segments={SPLIT} />
        <div className="legend">
          <FigureBlock label="Masuk" figure="Rp 9.518.400" caption="Gaji 23 Okt" phoneHidden />
          <FigureBlock label="Pengeluaran" mark="var(--out-solid)" figure="Rp 3.502.990" caption="sejak gaji 23 Okt" />
        </div>
      </HeroFigure>
    ),
  ],
  [
    "Panel with blocks, a section header, list rows and a day heading",
    () => (
      <>
        <section aria-labelledby="sec">
          <SectionHeader id="sec" label="Aktivitas terbaru" aside={<span className="aside">hari ini dan kemarin · 2</span>} />
          <DayHeading label="Kamis, 5 November · hari ini" />
          <ul className="list">
            <li><ListRow onClick={() => {}} time="12.37" title="Warung Bu Tini" meta="Jago · Makan & Jajan" phoneMeta="12.37 · Kebutuhan" status="Kebutuhan" amount={<span className="t-amt">−Rp 30.000</span>} /></li>
            <li><ListRow href="/review" icon="help" title="TRF BIFAST" meta="Belum dikenali · 4 Nov 09.12" amount={<span className="t-amt">−Rp 120.000</span>} chevron selected /></li>
          </ul>
        </section>
        <Panel label="Ringkasan simpanan">
          <PanelBlock heading="Kekayaan bersih" href="/accounts"><p className="t-fig">Rp 45.828.280</p></PanelBlock>
          <PanelBlock><SectionHeader label="Kakeibo siklus ini" /></PanelBlock>
        </Panel>
      </>
    ),
  ],
  ["MaskedMoney", () => <p>Saldo <MaskedMoney text="Rp 30.000" /></p>],
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

beforeEach(() => {
  app.hideBalances = false;
  app.currency = "IDR";
});

describe.each([
  ["light", false],
  ["dark", true],
])("primitives in the %s theme", (_theme, dark) => {
  afterEach(() => document.documentElement.classList.remove("dark"));

  it("MaskedMoney with hidden balances has no axe violations", async () => {
    if (dark) document.documentElement.classList.add("dark");
    app.hideBalances = true;
    const { container } = render(<p>Saldo <MaskedMoney text="Rp 30.000" /></p>);
    expect((await axe(container)).violations).toHaveLength(0);
  });

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

describe("MaskedMoney", () => {
  it("shows the formatted amount as text while balances are shown", () => {
    render(<p data-testid="figure"><MaskedMoney text="−Rp 30.000" className="t-amt" /></p>);
    expect(screen.getByTestId("figure")).toHaveTextContent("−Rp 30.000");
    expect(screen.queryByRole("img")).not.toBeInTheDocument();
  });

  it.each([
    ["IDR", "Rp ••••••"],
    ["USD", "$ ••••••"],
  ] as const)("hides a %s amount behind its mask, named Disembunyikan, with no digit anywhere", (currency, mask) => {
    app.hideBalances = true;
    app.currency = currency;
    render(
      <button type="button">
        Warung Bu Tini <MaskedMoney text="−Rp 30.000" className="t-amt" />
      </button>,
    );
    const figure = screen.getByRole("img", { name: "Disembunyikan" });
    expect(figure).toHaveTextContent(mask);
    expect(figure).toHaveClass("mask", "t-amt");
    const row = screen.getByRole("button");
    expect(row.textContent).not.toMatch(/\d/);
    expect(row).toHaveAccessibleName("Warung Bu Tini Disembunyikan");
  });
});

describe("SplitBar", () => {
  const bar = () => screen.getByRole("img");

  it("is one image named with each segment's rounded share of the total", () => {
    render(<SplitBar label="Uang siklus ini dari Rp 9.518.400 masuk" emptyLabel="Uang siklus ini: belum ada uang masuk" total={9_518_400} segments={SPLIT} />);
    expect(bar()).toHaveAccessibleName(
      "Uang siklus ini dari Rp 9.518.400 masuk: Pengeluaran 37%, Ditabung & investasi 29%, Terjadwal 10%, Aman dipakai 24%",
    );
    const segments = [...bar().children] as HTMLElement[];
    expect(segments.map((segment) => segment.style.flexGrow)).toEqual(["3502990", "2750000", "989000", "2276410"]);
    expect(segments.map((segment) => segment.style.background)).toEqual(["var(--out-solid)", "var(--inv-solid)", "var(--warn-solid)", "var(--in-solid)"]);
  });

  it("gives a savings decrease no width and still names its share", () => {
    const decrease = SPLIT.map((segment) => (segment.key === "saving" ? { ...segment, amount: 0 } : segment));
    render(<SplitBar label="Uang siklus ini dari Rp 9.518.400 masuk" emptyLabel="" total={9_518_400} segments={decrease} />);
    expect([...bar().children].map((segment) => segment.getAttribute("data-segment"))).toEqual(["spending", "scheduled", "safe"]);
    expect(bar()).toHaveAccessibleName(/Ditabung & investasi 0%/);
  });

  it("says there is no money in yet without shares when the total is zero", () => {
    render(<SplitBar label="Uang siklus ini dari Rp 0 masuk" emptyLabel="Uang siklus ini: belum ada uang masuk" total={0} segments={[{ ...SPLIT[0], amount: 45_000 }]} />);
    expect(bar()).toHaveAccessibleName("Uang siklus ini: belum ada uang masuk");
    expect(bar().getAttribute("aria-label")).not.toMatch(/%/);
  });

  it("is not drawn when every segment is zero", () => {
    const { container } = render(<SplitBar label="x" emptyLabel="y" total={9_518_400} segments={SPLIT.map((segment) => ({ ...segment, amount: 0 }))} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe("figure blocks", () => {
  it("renders the hero as a labelled section with the 44 px figure and its line, and supporting figures as text", () => {
    render(
      <HeroFigure id="hero" label="Aman dipakai sampai gajian" mark="var(--in-solid)" figure="Rp 2.276.410" line="20 hari lagi sampai gajian 25 Nov">
        <FigureBlock label="Terjadwal" mark="var(--warn-solid)" figure="Rp 989.000" caption="tagihan sampai 24 Nov" phoneHidden />
      </HeroFigure>,
    );
    const hero = screen.getByRole("region", { name: "Aman dipakai sampai gajian" });
    expect(hero.querySelector(".t-hero")).toHaveTextContent("Rp 2.276.410");
    expect(hero.querySelector(".hero-sub")).toHaveTextContent("20 hari lagi sampai gajian 25 Nov");
    const block = hero.querySelector(".fig-block")!;
    expect(block).toHaveClass("is-phone-hide");
    expect(block).toHaveTextContent("TerjadwalRp 989.000tagihan sampai 24 Nov");
    expect(within(hero).queryByRole("button")).not.toBeInTheDocument();
  });
});

describe("list pieces", () => {
  it("makes a row one button named by its title, meta and amount, or one link", () => {
    const onClick = vi.fn();
    render(
      <ul className="list">
        <li><ListRow onClick={onClick} time="12.37" title="Warung Bu Tini" meta="Jago · Makan & Jajan" status="Kebutuhan" amount={<span className="t-amt">−Rp 30.000</span>} /></li>
        <li><ListRow href="/review?item=q1#review-questions" icon="help" title="TRF BIFAST" meta="Belum dikenali · 4 Nov 09.12" amount={<span className="t-amt">−Rp 120.000</span>} chevron /></li>
      </ul>,
    );
    const row = screen.getByRole("button");
    expect(row).toHaveAccessibleName("12.37 Warung Bu Tini Jago · Makan & Jajan Kebutuhan −Rp 30.000");
    expect(row).toHaveClass("row");
    row.click();
    expect(onClick).toHaveBeenCalledOnce();
    const link = screen.getByRole("link");
    expect(link).toHaveAttribute("href", "/review?item=q1#review-questions");
    expect(link).toHaveAccessibleName("TRF BIFAST Belum dikenali · 4 Nov 09.12 −Rp 120.000");
    expect(link.querySelector(".r-ico svg")).toHaveAttribute("aria-hidden", "true");
    expect(within(link).queryByRole("button")).not.toBeInTheDocument();
  });

  it("puts the phone's second line apart from the desktop meta and marks a selected row", () => {
    render(<ListRow onClick={() => {}} time="18.23" title="Kedai Awan" meta="Jago · Makan" phoneMeta="18.23 · Kebutuhan" selected />);
    const row = screen.getByRole("button");
    expect(row.querySelector(".r-meta.dt-only")).toHaveTextContent("Jago · Makan");
    expect(row.querySelector(".r-meta.ph-only")).toHaveTextContent("18.23 · Kebutuhan");
    expect(row).toHaveAttribute("data-selected", "");
    expect(row).toHaveClass("ws-select-row");
  });

  it("titles a day with a level 3 heading and makes a linked panel block one link", () => {
    render(
      <Panel label="Ringkasan simpanan">
        <DayHeading label="Rabu, 4 November · kemarin" totals="Pengeluaran Rp 50.500" />
        <PanelBlock heading="Ketahanan Dana" href="/goals"><p>5,4 bulan biaya pokok</p></PanelBlock>
      </Panel>,
    );
    expect(screen.getByRole("complementary", { name: "Ringkasan simpanan" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 3, name: "Rabu, 4 November · kemarin" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Ketahanan Dana 5,4 bulan biaya pokok" })).toHaveAttribute("href", "/goals");
  });
});

describe("page piece geometry in the stylesheet", () => {
  const css = readFileSync(path.join(__dirname, "../../app/globals.css"), "utf8").replace(/\/\*[\s\S]*?\*\//g, "");
  const rule = (selector: string, from = 0) => {
    const start = css.indexOf(`${selector} {`, from);
    expect(start, selector).toBeGreaterThanOrEqual(0);
    return css.slice(start, css.indexOf("}", start));
  };
  const phone = css.indexOf("@media (max-width: 767px) {\n  .stack");

  it("sticks the day heading under the top bar, and under the 56 px phone header", () => {
    expect(rule(".day-h")).toMatch(/position: sticky;[\s\S]*top: var\(--topbar-h\);[\s\S]*height: 40px;/);
    expect(rule(".day-h", phone)).toContain("top: 56px");
  });

  it("draws the hero 44/52 and 32/40 below 360 px, the split bar 8 px with 2 px gaps and segments of at least 2 px", () => {
    expect(rule(".t-hero")).toMatch(/font-size: var\(--fs-44\); line-height: 52px/);
    expect(css).toMatch(/@media \(max-width: 359px\) \{\s*\.t-hero \{ font-size: var\(--fs-32\); line-height: 40px; \}/);
    expect(rule(".split")).toMatch(/gap: 2px; height: 8px;/);
    expect(rule(".split > span")).toContain("min-width: 2px");
  });

  it("makes rows 56 px, 64 px and two lines on the phone, with the time and status columns hidden there", () => {
    expect(rule(".row")).toMatch(/min-height: 56px;/);
    expect(rule(".row", phone)).toMatch(/min-height: 64px;/);
    expect(rule(".row .r-main", phone)).toContain("flex-direction: column");
    expect(rule(".row :is(.r-time, .r-cat, .r-pillar, .dt-only)", phone)).toContain("display: none");
  });
});

