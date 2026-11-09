"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import { Button } from "@/components/ui/Button";
import { MonetaryInput } from "@/components/ui/FormField";
import { Icon } from "@/components/ui/Icon";
import { Input, Select } from "@/components/ui/Input";
import { FigureBlock, GainText, HeroFigure, ProgressBar, SplitBar } from "@/components/workspace/figures";
import { InstitutionMark } from "@/components/accounts/Tile";
import { Answer, AnswerList } from "@/components/review/triage/Answers";
import { DayHeading, ListRow, Panel, PanelBlock, SectionHeader } from "@/components/workspace/lists";
import { Rail, RailItem, RailRule } from "@/components/workspace/rail";
import { EmptyState, ErrorState, Pill, Segmented, Skeleton } from "@/components/workspace/primitives";
import { KAKEIBO_BUCKETS } from "@/lib/kakeibo";
import styles from "./komponen.module.css";

// Every shared primitive and state, light and dark side by side, like the prototype's #/komponen
// (apply-markas-foundation D12). Development builds only; see page.tsx.

const TYPE_ROLES: [string, string, string, string][] = [
  ["Angka utama (hero)", "t-hero", "Rp 2.276.410", "Manrope 44 / 52 · 600 · −0,025em · tnum"],
  ["Angka dialog", "t-fig-l", "Rp 65.000", "Inter 32 / 40 · 600 · −0,02em · tnum"],
  ["Judul halaman", "t-title", "Transaksi", "Manrope 24 / 32 · 600 · −0,015em"],
  ["Angka pendukung", "t-fig", "Rp 9.518.400", "Inter 20 / 28 · 600 · −0,01em · tnum"],
  ["Judul dialog", "t-dtitle", "Ubah transaksi", "Manrope 20 / 28 · 600 · −0,01em"],
  ["Kalimat Catat", "t-lead", "Untuk apa transfer Rp 120.000 ke ANDI S?", "Inter 16 / 24 · 400"],
  ["Isi, judul baris", "t-body", "Warung Bu Tini", "Inter 14 / 20 · 400"],
  ["Tombol, menu, tab", "t-strong", "Semua transaksi", "Inter 14 / 20 · 500"],
  ["Jumlah di baris", "t-amt", "−Rp 30.000", "Inter 14 / 20 · 600 · tnum"],
  ["Label bagian", "t-label", "Aktivitas terbaru", "Inter 12 / 16 · 500 · +0,06em · kapital"],
  ["Keterangan", "t-meta", "Jago · Makan & Jajan · 12.37", "Inter 12 / 16 · 400 · muted"],
];

// App token (prototype name): the swatches print their contrast against the three surfaces.
const TOKENS: [string, string][] = [
  ["--window", "s0"], ["--surface", "s1"], ["--surface-2", "s2"], ["--dialog", "dialog"], ["--line", "line"],
  ["--line-strong", "line-strong"], ["--text", "ink"], ["--muted", "muted"], ["--accent", "accent"],
  ["--accent-hover", "accent-hover"], ["--accent-pressed", "accent-pressed"], ["--accent-fg", "accent-text"],
  ["--focus", "focus"], ["--field-focus", "field-focus"], ["--in-fg", "income"], ["--tr-fg", "transfer"],
  ["--inv-fg", "invest"], ["--warn-fg", "warning"], ["--out-fg", "expense"], ["--in-solid", "m-income"],
  ["--tr-solid", "m-transfer"], ["--inv-solid", "m-invest"], ["--warn-solid", "m-warning"], ["--out-solid", "m-expense"],
  ["--k-need", "k-need"], ["--k-want", "k-want"], ["--k-culture", "k-culture"], ["--k-unexpected", "k-unexpected"],
  ["--k-saving", "k-saving"], ["--k-none", "k-none"], ["--danger-solid", "danger"],
];

function luminance(hex: string): number {
  const channels = [1, 3, 5].map((index) => parseInt(hex.slice(index, index + 2), 16) / 255);
  const [r, g, b] = channels.map((value) => (value <= 0.03928 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4));
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrast(first: string, second: string): string {
  const [light, dark] = [luminance(first), luminance(second)].sort((a, b) => b - a);
  return ((light + 0.05) / (dark + 0.05)).toFixed(2).replace(".", ",");
}

function Swatches() {
  const ref = useRef<HTMLDivElement>(null);
  const [values, setValues] = useState<Record<string, string>>({});
  useEffect(() => {
    if (!ref.current) return;
    const style = getComputedStyle(ref.current);
    setValues(Object.fromEntries(TOKENS.map(([name]) => [name, style.getPropertyValue(name).trim().toUpperCase()])));
  }, []);
  const hex = (name: string) => (/^#[0-9A-F]{6}$/.test(values[name] ?? "") ? values[name] : null);
  return (
    <div ref={ref} className={styles.swatches}>
      {TOKENS.map(([name, prototypeName]) => {
        const value = hex(name);
        const surfaces = ["--window", "--surface", "--surface-2"].map(hex);
        const ratios = value && surfaces.every(Boolean)
          ? surfaces.map((surface, index) => `${contrast(value, surface!)} s${index}`).join(" · ")
          : "";
        return (
          <div key={name} className={styles.swatch}>
            <span className={styles.swatchBox} style={{ background: `var(${name})` }} aria-hidden="true" />
            <div className="min-w-0">
              <p className="t-strong">{name}</p>
              <p className="t-meta tabular">{prototypeName} · {values[name] ?? ""}</p>
              {ratios && <p className="t-meta tabular">{ratios}</p>}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function Both({ title, children }: { title: string; children: (theme: "light" | "dark") => ReactNode }) {
  return (
    <section className={styles.section} aria-label={title}>
      <h2 className="t-label">{title}</h2>
      <div className={styles.grid}>
        <div className={`theme-light ${styles.cell}`}>
          <span className="t-label">Terang</span>
          {children("light")}
        </div>
        <div className={`dark ${styles.cell}`}>
          <span className="t-label">Gelap</span>
          {children("dark")}
        </div>
      </div>
    </section>
  );
}

const STATES = ["", styles.isHover, styles.isFocus, styles.isPressed];

function ButtonRow({ className, label }: { className: string; label: string }) {
  return (
    <div className={styles.row}>
      {STATES.map((state) => (
        <button key={state || "rest"} type="button" tabIndex={-1} className={`${className} ${state}`}>{label}</button>
      ))}
      <button type="button" className={className} disabled>{label}</button>
    </div>
  );
}

const SPLIT = [
  { key: "spending", label: "Pengeluaran", amount: 3_502_990, color: "var(--out-solid)" },
  { key: "saving", label: "Ditabung & investasi", amount: 2_750_000, color: "var(--inv-solid)" },
  { key: "scheduled", label: "Terjadwal", amount: 989_000, color: "var(--warn-solid)" },
  { key: "safe", label: "Aman dipakai", amount: 2_276_410, color: "var(--in-solid)" },
];

const ROW_STATES: [string, string][] = [["bawaan", ""], ["hover", styles.rowHover], ["ditekan", styles.rowPressed], ["fokus", styles.isFocusRow]];

function Pillar({ color, label }: { color: string; label: string }) {
  return (
    <>
      <span className="dot" style={{ background: color }} aria-hidden="true" />
      <span>{label}</span>
    </>
  );
}

function PageRows() {
  return (
    <div className={styles.wide}>
      <DayHeading label="Kamis, 5 November · hari ini" totals="Pengeluaran Rp 52.500" />
      <ul className="list">
        {ROW_STATES.map(([state, className]) => (
          <li key={state} className={className}>
            <ListRow time="12.37" title={`Warung Bu Tini (${state})`} meta="Jago · Makan & Jajan" phoneMeta="12.37 · Kebutuhan"
              status={<Pillar color="var(--k-need)" label="Kebutuhan" />} amount={<span className="t-amt">−Rp 30.000</span>} />
          </li>
        ))}
        <li>
          <ListRow time="11.19" title="Teh Tarik Corner (terpilih)" meta="ShopeePay" status={<Pillar color="var(--k-want)" label="Keinginan" />}
            amount={<span className="t-amt">−Rp 22.500</span>} selected />
        </li>
        <li>
          <ListRow time="08.02" title="Gaji Oktober" meta="BCA · Utama" status="Uang masuk" amount={<span className="t-amt c-income">+Rp 9.518.400</span>} />
        </li>
        <li>
          <ListRow time="07.55" title="Isi GoPay" meta="Jago · Kantong Utama → GoPay" status="—" amount={<span className="t-amt c-transfer">Rp 200.000</span>} />
        </li>
        <li>
          <ListRow time="07.40" title="Top up Investasi" meta="Stockbit · RDN" status={<Pillar color="var(--k-saving)" label="Tabungan" />}
            amount={<span className="t-amt c-invest">Rp 250.000</span>} />
        </li>
        <li>
          <ListRow time="07.31" title="Warung Bu Tini (lengkap)" meta="Jago · Makan & Jajan" category="Makan & Jajan"
            status={<Pillar color="var(--k-need)" label="Kebutuhan" />} amount={<span className="t-amt">−Rp 18.000</span>} />
        </li>
        <li>
          <ListRow icon="help" title="TRF BIFAST 88213 ANDI S" meta="Belum dikenali · 4 Nov 09.12" amount={<span className="t-amt">−Rp 120.000</span>} chevron />
        </li>
      </ul>
    </div>
  );
}

/** Tinjau's pieces (apply-markas-tinjau D19): the rail (chips under 768 px), stacked rows and numbered answers. */
function TinjauPieces({ theme }: { theme: "light" | "dark" }) {
  return (
    <div className={styles.tinjau}>
      <Rail label={`Folder ${theme}`} chips>
        <RailItem label="Semua" count="6" countName="6 item" current onSelect={() => undefined} />
        <RailItem label="Belum dikenali" count="2" countName="2 item" onSelect={() => undefined} />
        <RailItem label="Pindah saldo belum lengkap" count="1" countName="1 item" onSelect={() => undefined} />
        <RailRule />
        <RailItem label="Dicatat otomatis" count="4" countName="4 item" onSelect={() => undefined} />
      </Rail>
      <ul className="list" aria-label={`Baris bertumpuk ${theme}`}>
        {ROW_STATES.map(([state, className]) => (
          <li key={state} className={className}>
            <ListRow layout="stacked" icon="help" title={`TRF BIFAST 88213 (${state})`} meta="Belum dikenali · 4 Nov 09.12"
              amount={<span className="t-amt c-expense">−Rp 120.000</span>} onClick={() => undefined} />
          </li>
        ))}
        <li>
          <ListRow layout="stacked" icon="alert" title="Kedai Awan (terpilih)" meta="Perlu jawaban · 4 Nov 12.10" selected current
            amount={<span className="t-amt c-expense">−Rp 54.000</span>} onClick={() => undefined} />
        </li>
      </ul>
      <div>
        <p id={`ks-question-${theme}`} className="t-lead">Kategori untuk Kedai Awan: Makan & Minum?</p>
        <AnswerList labelledBy={`ks-question-${theme}`}>
          <Answer n={1} primary label="Terima semua (3)" detail="Kategorinya sudah benar" onClick={() => undefined} />
          <Answer n={2} label="Ubah kategori" detail="Kategori dan pilar Kakeibo" onClick={() => undefined} />
          <Answer n={3} label="Ganti nama" detail="Nama toko dan catatan" disabled onClick={() => undefined} />
        </AnswerList>
      </div>
    </div>
  );
}

export function ComponentSheet() {
  const [segment, setSegment] = useState("cycle");
  const [amount, setAmount] = useState("35.000");
  return (
    <div className="pb-16">
      <p className={`t-body ${styles.intro}`}>
        Setiap komponen dan keadaannya, terang dan gelap berdampingan. Tujuh ukuran huruf
        (12, 14, 16, 20, 24, 32, 44), tiga ketebalan (400, 500, 600), tiga radius (6, 12, 16) dan tepi 1 px netral.
      </p>

      <Both title="Skala huruf">
        {() => TYPE_ROLES.map(([role, className, sample, spec]) => (
          <div key={className} className={styles.type}>
            <div>
              <p className="t-body">{role}</p>
              <p className="t-meta">{spec}</p>
            </div>
            <p className={className}>{sample}</p>
          </div>
        ))}
      </Both>

      <Both title="Token warna">{() => <Swatches />}</Both>

      <Both title="Tombol">
        {() => (
          <>
            <p className="t-meta">bawaan · hover · fokus · ditekan · nonaktif</p>
            <ButtonRow className="ws-btn ws-btn-accent" label="Catat" />
            <ButtonRow className="ws-btn" label="Ubah" />
            <ButtonRow className="ws-btn ws-btn-quiet" label="Rincian" />
            <ButtonRow className="ws-btn ws-btn-danger" label="Hapus" />
            <div className={styles.row}>
              <Button variant="primary">Simpan</Button>
              <Button variant="secondary">Batal</Button>
              <Button variant="ghost">Lainnya</Button>
              <Button variant="danger">Ya, hapus</Button>
              <Button variant="ghost" size="sm">Kelola</Button>
            </div>
            <div className={styles.row}>
              <button type="button" tabIndex={-1} className="ws-icon-btn" aria-label="Contoh tombol ikon"><Icon name="bell" /></button>
              <button type="button" tabIndex={-1} className={`ws-icon-btn ${styles.isHover}`} aria-label="Contoh hover"><Icon name="eye" /></button>
              <button type="button" tabIndex={-1} className={`ws-icon-btn ${styles.isFocus}`} aria-label="Contoh fokus"><Icon name="sun" /></button>
              <button type="button" tabIndex={-1} className="ws-linkbtn">Semua transaksi</button>
              <span className="ws-kbd">1</span>
              <span className="ws-kbd">⌘K</span>
              <button type="button" tabIndex={-1} className="ws-act">Jawab</button>
            </div>
          </>
        )}
      </Both>

      <Both title="Isian dan pilihan">
        {(theme) => (
          <>
            <div className={styles.formGrid}>
              <Input id={`ks-a-${theme}`} label="Kosong" placeholder="Rp" />
              <Input id={`ks-b-${theme}`} label="Terisi" defaultValue="65.000" />
              <Input id={`ks-c-${theme}`} label="Fokus" defaultValue="65.000" className={styles.fieldFocus} />
              <Input id={`ks-d-${theme}`} label="Galat" error="Isi nominalnya dulu." />
              <Select id={`ks-e-${theme}`} label="Pilihan" defaultValue="utama">
                <option value="utama">Jago · Kantong Utama · bawaan</option>
              </Select>
              <MonetaryInput id={`ks-f-${theme}`} name={`ks-f-${theme}`} label="Nominal besar" value={amount} onChange={setAmount} />
            </div>
            <div className={styles.row}>
              <label className="inline-flex items-center gap-2">
                <input type="checkbox" defaultChecked className="h-4 w-4" />
                <span className="t-body">Kotak centang</span>
              </label>
              <span className="switch" role="switch" aria-checked="true" aria-label="Sakelar aktif" />
              <span className="switch" role="switch" aria-checked="false" aria-label="Sakelar mati" />
              <span className="chip">
                <span className="lbl">BCA</span>
                <button type="button" tabIndex={-1} aria-label="Hapus filter BCA"><Icon name="x" className="h-3 w-3" /></button>
              </span>
              <button type="button" tabIndex={-1} className="ws-chip" aria-pressed="true">Pengeluaran</button>
            </div>
            <div className="ws-search">
              <Icon name="search" className="h-4 w-4" />
              <input aria-label={`Contoh cari ${theme}`} placeholder="Cari transaksi" />
              <span className="ws-kbd">⌘K</span>
            </div>
          </>
        )}
      </Both>

      <Both title="Kontrol segmen, tab, garis pilihan">
        {() => (
          <>
            <Segmented
              label="Contoh segmen"
              value={segment}
              onChange={setSegment}
              options={[{ value: "cycle", label: "Siklus gaji" }, { value: "month", label: "Bulan kalender" }, { value: "all", label: "Semua waktu" }]}
            />
            <div className={styles.nav}>
              <span className="t-label block px-6 py-2">Harian</span>
              <span className={styles.navItem}><Icon name="home" className="h-4 w-4" /><span className={styles.grow}>Bawaan</span></span>
              <span className={`ws-select-bar ${styles.navItem}`} aria-current="page"><Icon name="inbox" className="h-4 w-4" /><span className={styles.grow}>Aktif</span><span className={styles.count}>6</span></span>
            </div>
            <ul className="ws-group-rail" style={{ ["--rail" as string]: "var(--k-saving)" }}>
              <li className="t-body">Rel kelompok: 2 px lurus, warna kegunaan</li>
              <li className="t-meta">Tabungan · 2 kantong</li>
            </ul>
            <div>
              <button type="button" tabIndex={-1} className="ws-select-row flex w-full items-center justify-between border-b border-[var(--line)] px-3 py-4 text-left" aria-pressed="true">
                <span className="t-body">Baris terpilih</span>
                <span className="t-amt">−Rp 30.000</span>
              </button>
              <button type="button" tabIndex={-1} className="ws-select-row flex w-full items-center justify-between border-b border-[var(--line)] px-3 py-4 text-left" aria-pressed="false">
                <span className="t-body">Baris biasa</span>
                <span className="t-amt text-[var(--in-fg)]">+Rp 9.518.400</span>
              </button>
            </div>
          </>
        )}
      </Both>

      <Both title="Lencana, progres, pilar Kakeibo">
        {() => (
          <>
            <div className={styles.row}>
              <Pill>Selesai</Pill>
              <Pill tone="warn">Perlu jawaban</Pill>
              <Pill tone="in">Uang Masuk</Pill>
              <Pill tone="out">Terlewat</Pill>
              <Pill tone="inv">Investasi</Pill>
              <Pill tone="tr">Pindah Saldo</Pill>
              <span className="ws-badge">Akun Induk</span>
            </div>
            <div className="ws-bar" aria-hidden="true"><i style={{ width: "62%", ["--bc" as string]: "var(--text)" }} /></div>
            <div className={styles.row}>
              {KAKEIBO_BUCKETS.map((bucket) => (
                <span key={bucket.value} className="t-meta inline-flex items-center gap-2">
                  <span className={bucket.value === "unclassified" ? "dot is-unclassified" : "dot"} style={bucket.value === "unclassified" ? undefined : { background: bucket.color }} aria-hidden="true" />
                  {bucket.label}
                </span>
              ))}
            </div>
          </>
        )}
      </Both>

      <Both title="Angka utama, batang, angka pendukung">
        {(theme) => (
          <div className={styles.scroll} tabIndex={0} role="region" aria-label={`Contoh angka utama ${theme}`}>
            <HeroFigure id={`ks-hero-${theme}`} label="Aman dipakai sampai gajian" mark="var(--in-solid)" figure="Rp 2.276.410" line="20 hari lagi sampai gajian 25 Nov"
              action={<button type="button" tabIndex={-1} className="ws-btn ws-btn-quiet ws-btn-sm">Rincian</button>}>
              <SplitBar label="Uang siklus ini dari Rp 9.518.400 masuk" emptyLabel="Uang siklus ini: belum ada uang masuk" total={9_518_400} segments={SPLIT} />
              <div className="legend">
                <FigureBlock label="Masuk" figure="Rp 9.518.400" caption="Gaji 23 Okt" phoneHidden />
                <FigureBlock label="Pengeluaran" mark="var(--out-solid)" figure="Rp 3.502.990" caption="sejak gaji 23 Okt" />
                <FigureBlock label="Ditabung & investasi" mark="var(--inv-solid)" figure="Rp 2.750.000" caption="Tabungan bertambah" />
                <FigureBlock label="Terjadwal" mark="var(--warn-solid)" figure="Rp 989.000" caption="tagihan sampai 24 Nov" phoneHidden />
              </div>
            </HeroFigure>
          </div>
        )}
      </Both>

      <Both title="Rekening: grup, kartu, batang">
        {(theme) => (
          <div className={styles.scroll} tabIndex={0} role="region" aria-label={`Contoh Rekening ${theme}`}>
            <section className="grp" style={{ "--rail": "var(--use-investasi)" } as React.CSSProperties} aria-label={`Grup contoh ${theme}`}>
              <div className="grp-h">
                <h3 className="t-label">Investasi</h3>
                <span className="count">5 produk</span>
                <GainText gain={{ amount: 396_000, cost: 4_950_000 }} />
                <span className="total t-amt">Rp 30.219.700</span>
              </div>
              <ul className="tiles">
                <li>
                  <div className="tile">
                    <span className="tl-top">
                      <InstitutionMark name="Stockbit" colour="#2E7DF6" />
                      <span className="names"><span className="tl-name">BBCA</span><span className="t-meta">Stockbit · 12 lot</span></span>
                    </span>
                    <span className="tl-bottom"><span className="tl-amt t-amt">Rp 1.000.000</span><GainText gain={{ amount: -164_000, cost: 1_532_710 }} /></span>
                  </div>
                </li>
                <li>
                  <div className="tile">
                    <span className="tl-top">
                      <InstitutionMark name="SPayLater" colour="#F08A24" />
                      <span className="names"><span className="tl-name">SPayLater</span><span className="t-meta">Sisa limit Rp 2.107.000</span></span>
                    </span>
                    <ProgressBar value={893_000} max={3_000_000} color="var(--out-solid)" />
                    <span className="tl-bottom"><span className="tl-amt t-amt c-expense">−Rp 893.000</span><span className="tl-due t-meta">jatuh tempo 11 Des · Rp 434.000</span></span>
                  </div>
                </li>
              </ul>
            </section>
            <div className="hbar">
              <span className="lbl">Reksa dana</span>
              <span className="bar" aria-hidden="true"><span style={{ width: "62%", background: "var(--inv-solid)" }} /></span>
              <span className="amt t-amt">Rp 18.400.000</span>
              <span className="side"><GainText gain={{ amount: 65_800, cost: 18_334_200 }} /></span>
            </div>
          </div>
        )}
      </Both>

      <Both title="Kepala bagian, baris, judul hari">
        {(theme) => (
          <>
            <SectionHeader label="Aktivitas terbaru" aside={<span className="aside">hari ini dan kemarin · 7</span>} />
            <div className={styles.scroll} tabIndex={0} role="region" aria-label={`Contoh baris ${theme}`}>
              <PageRows />
            </div>
          </>
        )}
      </Both>

      <Both title="Rail Tinjau, baris bertumpuk, jawaban bernomor">
        {(theme) => <TinjauPieces theme={theme} />}
      </Both>

      <Both title="Panel konteks dan Kakeibo">
        {() => (
          <Panel label="Contoh panel">
            <PanelBlock heading="Kekayaan bersih" href="/komponen">
              <p className="t-fig">Rp 45.828.280</p>
              <p className="t-meta">aset Rp 58.971.280 · utang Rp 13.143.000</p>
            </PanelBlock>
            <PanelBlock heading="Ketahanan Dana" href="/komponen">
              <p className="t-strong">5,4 bulan biaya pokok</p>
              <p className="t-meta">dana darurat Rp 19.412.400 · target 6 bulan</p>
            </PanelBlock>
            <PanelBlock>
              <SectionHeader label="Kakeibo siklus ini" aside={<span className="ws-linkbtn">Analisis</span>} />
              <div className="kk-row">
                <span className="dot" style={{ background: "var(--k-need)" }} aria-hidden="true" />
                <span className="t-body">Kebutuhan</span>
                <span className="t-amt">Rp 2.341.000</span>
                <span className="prog" aria-hidden="true"><span style={{ width: "100%", background: "var(--k-need)" }} /></span>
              </div>
              <div className="kk-row">
                <span className="dot is-unclassified" aria-hidden="true" />
                <span className="t-body">Belum diklasifikasi</span>
                <span className="t-amt">Rp 190.500</span>
                <span className="t-meta c-accent kk-note">Perlu dipilih di Tinjau</span>
              </div>
            </PanelBlock>
          </Panel>
        )}
      </Both>

      <Both title="Menu, toast, dialog, kosong, memuat, galat">
        {() => (
          <>
            <div className={`ws-menu ${styles.static}`} role="presentation">
              <p className="ws-menu-h">Tema</p>
              <button type="button" tabIndex={-1} className="ws-menu-item" aria-pressed="true">Terang</button>
              <button type="button" tabIndex={-1} className="ws-menu-item" aria-pressed="false">Gelap</button>
              <div className="ws-menu-sep" />
              <button type="button" tabIndex={-1} className="ws-menu-item" aria-pressed="false">Ikuti sistem</button>
            </div>
            <div className={`ws-toast ${styles.static}`}>
              <span>Dijawab · Pembayaran QRIS</span>
              <button type="button" tabIndex={-1}>Urungkan</button>
            </div>
            <div className={`ws-drawer border border-[var(--line)] rounded-2xl shadow-float ${styles.static}`}>
              <div className="ws-drawer-h">
                <h2>Judul dialog</h2>
                <button type="button" tabIndex={-1} className="ws-icon-btn" aria-label="Tutup contoh"><Icon name="x" /></button>
              </div>
              <p className="t-body">Isi dialog. Radius 16, tepi 1 px netral, bayangan hanya di lapisan mengambang.</p>
              <div className={styles.dialogFoot}>
                <button type="button" tabIndex={-1} className="ws-btn">Batal</button>
                <button type="button" tabIndex={-1} className="ws-btn ws-btn-accent">Simpan</button>
              </div>
            </div>
            <div className={`ws-drawer border border-[var(--line)] shadow-float ${styles.static} ${styles.sheet}`}>
              <div className="ws-grab" aria-hidden="true" />
              <p className="t-dtitle">Lembar ponsel</p>
              <p className="t-body">Dialog di ponsel muncul dari bawah, tinggi maksimal 92%.</p>
            </div>
            <EmptyState title="Semua sudah ditinjau">Tidak ada yang perlu dijawab.</EmptyState>
            <div aria-busy="false">
              {[0, 1].map((row) => (
                <div key={row} className={styles.skelRow}>
                  <Skeleton style={{ width: 40, height: 12 }} />
                  <Skeleton style={{ width: "40%", height: 12 }} />
                  <span className="flex-1" />
                  <Skeleton style={{ width: 96, height: 12 }} />
                </div>
              ))}
            </div>
            <ErrorState message="Transaksi belum bisa dimuat." onRetry={() => undefined} />
          </>
        )}
      </Both>
    </div>
  );
}
