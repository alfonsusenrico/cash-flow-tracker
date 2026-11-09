// Target & Tagihan figure parity (apply-markas-target task 6.5): reads the hero, the bills, every goal and debt, and
// Beranda's Terjadwal and Pengeluaran from a running app, and writes them as one JSON so two runs can be compared:
// before against after on the same seed, and English against Indonesian on the same data.
//
// Usage:
//   node target-parity.mjs --side before|after --origin <url> --lang id|en --out <file.json> [--rev <hash>] [--port 9773]
//   node target-parity.mjs --compare <a.json> <b.json>    lists every value that differs
// Environment: MARKAS_USER, MARKAS_PASSWORD (the seed owner), MARKAS_AXE (not needed here).
//
// `before` reads the pre-redesign page (three cards: #goals-runway, #goals-debts, #goals-bills, #goals-list). That
// page shows only three bill rows and no per-debt amount, so the bill rows are the visible ones and each debt's
// remaining amount, total and paid amount are read from its drawer (?utang=). `barPct` is the old bar's inline
// width (the browser's division), `target` the old client target figure. `after` reads the rebuilt page; its
// extractor is filled in with the new DOM.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const argv = process.argv.slice(2);

if (argv[0] === "--compare") {
  const [a, b] = argv.slice(1).map((file) => JSON.parse(readFileSync(file, "utf8")));
  const differences = [];
  const walk = (left, right, at) => {
    if (left && right && typeof left === "object" && typeof right === "object") {
      for (const key of new Set([...Object.keys(left), ...Object.keys(right)])) walk(left[key], right[key], `${at}.${key}`);
      return;
    }
    if (JSON.stringify(left) !== JSON.stringify(right)) differences.push({ at, a: left ?? null, b: right ?? null });
  };
  for (const part of ["hero", "bills", "goals", "debts", "credit", "beranda"]) walk(a[part], b[part], part);
  // Between languages only the words may differ: every amount, percent, count and date token must be identical
  // (row keys hold each database's own ids and are left out).
  const figures = (value) => String(value ?? "").match(/[−+-]?(?:Rp|\$)\s?[\d.,]+|\d[\d.,]*%?|\b\d{1,2} [A-Z][a-z]{2}\b/g) ?? [];
  const figureDifferences = differences.filter((item) => !/\.key$/.test(item.at) && JSON.stringify(figures(item.a)) !== JSON.stringify(figures(item.b)));
  console.log(JSON.stringify({ differences: differences.length, figureDifferences: figureDifferences.length, figureList: figureDifferences, list: differences }, null, 1));
  process.exit(0);
}

// Before (old page) against after (rebuilt page) on the same seed: every figure the old page showed is looked up on the
// new one. Each difference is listed; the named presentation changes of the design are marked `named`.
if (argv[0] === "--compare-sides") {
  const [before, after] = argv.slice(1).map((file) => JSON.parse(readFileSync(file, "utf8")));
  const rows = [];
  const check = (at, a, b, named = null) => rows.push({ at, before: a ?? null, after: b ?? null, same: JSON.stringify(a ?? null) === JSON.stringify(b ?? null), named });
  for (const key of ["months", "fund", "target", "essential", "days"]) check(`hero.${key}`, before.hero?.[key], after.hero?.[key]);
  // D3: the bar reads the server's coverage_pct (one decimal) instead of the browser's division; equal within 0.1.
  const barGap = Math.abs((before.hero?.barPct ?? 0) - (after.hero?.barPct ?? 0));
  check("hero.barPct", before.hero?.barPct, after.hero?.barPct, barGap <= 0.1 ? `D3: server coverage_pct, ${barGap.toFixed(4)} apart (≤ 0.1)` : null);
  check("bills.count", before.bills?.count, after.bills?.count);
  check("bills.total", before.bills?.total, after.bills?.total);
  for (const row of before.bills?.rows ?? []) {
    const match = (after.bills?.rows ?? []).find((candidate) => candidate.label === row.label && candidate.date === row.date);
    const creditPlan = match && /sudah dihitung di Pengeluaran|already counted in Spending/.test(match.meta ?? "");
    check(`bills.row ${row.date} ${row.label}`, row.amount, match?.amount, creditPlan ? "D14: a credit-bill plan shows plan_amount; the counted part is the header total's" : null);
  }
  for (const goal of before.goals ?? []) {
    const match = (after.goals ?? []).find((candidate) => candidate.name === goal.name);
    if (!match) {
      check(`goals.${goal.name}`, goal, null, after.hero?.fund ? "D7: the one flagged goal leaves the list; its balance is the hero's Dana darurat" : null);
      continue;
    }
    for (const key of ["current", "target", "pct"]) check(`goals.${goal.name}.${key}`, goal[key], match[key]);
  }
  for (const debt of before.debts ?? []) {
    const match = (after.debts ?? []).find((candidate) => candidate.name === debt.name);
    check(`debts.${debt.name}.remaining`, debt.remaining, match?.remaining);
    check(`debts.${debt.name}.dialogRemaining`, debt.remaining, match?.dialogRemaining);
    check(`debts.${debt.name}.total`, debt.total, match?.total);
    const parse = (value) => Number(String(value ?? "").replace(/[^\d]/g, ""));
    const paidPct = debt.total ? String(Math.round((100 * parse(debt.paid)) / parse(debt.total))) : null;
    check(`debts.${debt.name}.pct (old paid ÷ total)`, paidPct, match?.pct, "D19: the old drawer's paid amount is now the % paid");
  }
  check("beranda.scheduled", before.beranda?.scheduled, after.beranda?.scheduled);
  check("beranda.spending", before.beranda?.spending, after.beranda?.spending);
  const changed = rows.filter((row) => !row.same);
  console.log(JSON.stringify({
    compared: rows.length,
    changed: changed.length,
    unnamed: changed.filter((row) => !row.named).length,
    requests: { before: before.requests?.goals, after: after.requests?.goals, added: (after.requests?.goalsList ?? []).filter((item) => !(before.requests?.goalsList ?? []).includes(item)), removed: (before.requests?.goalsList ?? []).filter((item) => !(after.requests?.goalsList ?? []).includes(item)) },
    changedList: changed,
  }, null, 1));
  process.exit(0);
}

const args = Object.fromEntries(argv.reduce((pairs, value, index, all) => (index % 2 ? pairs : [...pairs, [all[index].replace(/^--/, ""), all[index + 1]]]), []));
if (!args.side || !args.origin || !args.out) {
  console.error("Usage: node target-parity.mjs --side before|after --origin <url> --lang id|en --out <file.json>");
  process.exit(2);
}
const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "target-contoh-123" };

// Shared helpers, injected into every page read.
const HELPERS = String.raw`
  const text = (element) => (element ? element.innerText.replace(/\s+/g, " ").trim() : "");
  const money = /[−+-]?(?:Rp|\$)\s?[\d.,]+/g;
  const amounts = (value) => value.match(money) ?? [];
  const visible = (element) => Boolean(element && element.getBoundingClientRect().width > 0);
`;

const BEFORE_GOALS = String.raw`(() => {${HELPERS}
  const runway = document.querySelector("[aria-labelledby=goals-runway]");
  const facts = Object.fromEntries([...(runway?.querySelectorAll("dl > div") ?? [])].map((row) => [text(row.querySelector("dt")), text(row.querySelector("dd"))]));
  const big = text(runway?.querySelector(".ws-dana-big"));
  const foot = text(runway?.querySelector(".ws-dana-foot"));
  const bar = runway?.querySelector(".ws-bar > i");
  const hero = {
    months: (big.match(/^([\d.,]+)\s/) ?? [null, big])[1],
    fund: amounts(foot)[0] ?? null,
    target: (foot.match(/target\s+((?:Rp|\$)\s?[\d.,]+)/) ?? [null, null])[1],
    essential: facts["Biaya pokok / bulan"] ?? null,
    days: (facts["Jika pemasukan berhenti"] ?? "").replace(/\s*hari$/, "") || null,
    barPct: bar ? Number.parseFloat(bar.style.width) : null,
  };
  const billsCard = document.querySelector("[aria-labelledby=goals-bills]");
  const aside = text(billsCard?.querySelector("#goals-bills")?.parentElement?.parentElement?.querySelector(".ws-caption"));
  const head = text(billsCard?.firstElementChild);
  const headCount = head.match(/(\d+)\s·\s((?:Rp|\$)\s?[\d.,]+)/);
  const rows = [...(billsCard?.querySelectorAll("ul li") ?? [])].map((row) => {
    const parts = [...row.querySelectorAll("b")].map(text);
    const meta = text(row.querySelector("span > span"));
    return { date: meta.split(" · ")[0], label: parts[0], amount: parts[parts.length - 1] };
  });
  const more = text(billsCard?.querySelector("p.ws-caption"));
  const bills = {
    title: text(billsCard?.querySelector("h2, h3, #goals-bills")),
    count: headCount ? Number(headCount[1]) : rows.length,
    total: headCount ? headCount[2] : null,
    rowsShown: rows.length,
    more,
    rows,
  };
  const goals = [...document.querySelectorAll("[aria-labelledby=goals-list] ul li button")].map((tile) => {
    const value = text(tile);
    const pair = value.match(/((?:Rp|\$)\s?[\d.,]+) dari ((?:Rp|\$)\s?[\d.,]+)/);
    return { name: tile.getAttribute("aria-label"), current: pair?.[1] ?? null, target: pair?.[2] ?? null, pct: (value.match(/(\d+)%/) ?? [null, null])[1] };
  });
  const debtsCard = document.querySelector("[aria-labelledby=goals-debts]");
  const debtNames = [...(debtsCard?.querySelectorAll("ul li button") ?? [])].map(text);
  const paidRow = [...(debtsCard?.querySelectorAll("dl > div") ?? [])].find((row) => /Terbayar/.test(text(row)));
  return {
    hero,
    bills,
    goals,
    debtNames,
    debtsAggregate: { remaining: amounts(text(debtsCard?.querySelector(".ws-dana-big")))[0] ?? null, paidPct: text(paidRow?.querySelector("dd")) || null },
  };
})()`;

// One debt drawer of the old page: its remaining amount, total and paid amount.
const BEFORE_DEBT_DRAWER = String.raw`(() => {${HELPERS}
  const dialog = [...document.querySelectorAll("[role=dialog], dialog[open]")].filter(visible).pop();
  if (!dialog) return null;
  const facts = Object.fromEntries([...dialog.querySelectorAll("dt")].map((term) => [text(term), text(term.nextElementSibling)]));
  const balance = [...dialog.querySelectorAll("div")].find((element) => /^Sisa\s/.test(text(element)) && element.querySelector("b"));
  return { remaining: text(balance?.querySelector("b")) || null, total: facts.Total ?? null, paid: facts.Terbayar ?? null };
})()`;

// The rebuilt page (apply-markas-target D3 to D15): the hero's figure and four supporting figures, the bar's drawn
// width, the bills header and every row (the list is expanded first), each goal row, and each debt and credit row of
// the panel. Amounts keep their visible text; a debt's leading minus is dropped so it compares with the old drawer.
const AFTER_GOALS = String.raw`(() => {${HELPERS}
  const hero = document.querySelector(".tg-page section.hero");
  const figures = Object.fromEntries([...(hero?.querySelectorAll(".fig-block") ?? [])].filter(visible)
    .map((block) => [text(block.querySelector(".t-meta")), text(block.querySelector(".t-fig"))]));
  const pick = (...names) => names.map((name) => figures[name]).find((value) => value !== undefined) ?? null;
  const figure = text(hero?.querySelector(".t-hero"));
  const bar = hero?.querySelector(".tg-bar .prog > span");
  const days = pick("Jika pemasukan berhenti", "If income stops");
  const heroOut = {
    months: (figure.match(/^([\d.,]+)\s/) ?? [null, figure])[1],
    fund: pick("Dana darurat", "Emergency fund", "Saldo kas", "Cash balance"),
    target: pick("Target"),
    essential: pick("Biaya pokok / bulan", "Essential costs a month"),
    days: days ? days.replace(/\s*(hari|days?)$/, "") : null,
    barPct: bar ? Number.parseFloat(bar.style.width) : null,
    barLabel: hero?.querySelector(".tg-bar")?.getAttribute("aria-label") ?? null,
  };
  const billsSection = document.querySelector("[aria-labelledby=tg-bills]");
  const aside = text(billsSection?.querySelector(".sec-h .aside"));
  const headCount = aside.match(/^(\d+)\s·\s((?:Rp|\$)\s?[\d.,]+)/);
  const rows = [...(billsSection?.querySelectorAll(".list > li > .row") ?? [])].map((row) => ({
    date: text(row.querySelector(".r-time")),
    label: text(row.querySelector(".r-title")),
    amount: text(row.querySelector(".r-amt")),
    meta: text(row.querySelector(".r-meta:not(.ph-only)")),
    key: row.getAttribute("data-row"),
  }));
  const bills = {
    title: text(billsSection?.querySelector(".sec-h h2")),
    aside,
    count: headCount ? Number(headCount[1]) : null,
    total: headCount ? headCount[2] : null,
    rowsShown: rows.length,
    rows,
  };
  const goals = [...document.querySelectorAll(".goal-row")].map((row) => {
    const meta = text(row.querySelector(".r-amt .t-meta"));
    return {
      name: text(row.querySelector(".r-title")),
      current: text(row.querySelector(".r-amt .t-amt")),
      target: amounts(meta)[0] ?? null,
      pct: (meta.match(/(\d+)%/) ?? [null, null])[1],
    };
  });
  const stacked = [...document.querySelectorAll(".panel .row.stacked")];
  const owed = (row) => amounts(text(row.querySelector(".line .t-amt")))[0]?.replace(/^[−-]/, "") ?? null;
  const debtRows = stacked.filter((row) => (row.getAttribute("data-row") ?? "").startsWith("debt-")).map((row) => ({
    id: row.getAttribute("data-row").slice(5),
    name: text(row.querySelector(".line .t-body")),
    remaining: owed(row),
    pct: (text(row.querySelectorAll(".line")[1]).match(/(\d+)%/) ?? [null, null])[1],
  }));
  const credit = stacked.filter((row) => (row.getAttribute("data-row") ?? "").startsWith("credit-")).map((row) => ({
    name: text(row.querySelector(".line .t-body")),
    owed: owed(row) ?? text(row.querySelector(".line .t-amt")),
    line: text(row.querySelectorAll(".line")[1]),
  }));
  return { hero: heroOut, bills, goals, debtRows, credit, panelAside: text(document.querySelector("[aria-labelledby=tg-debts] .aside")) };
})()`;

// One debt dialog of the rebuilt page: what is left and "dari {total} · terbayar N%".
const AFTER_DEBT_DIALOG = String.raw`(() => {${HELPERS}
  const dialog = [...document.querySelectorAll("dialog[open]")].filter(visible).pop();
  if (!dialog) return null;
  const figure = dialog.querySelector(".dlg-fig");
  const line = text(figure?.querySelector("p .t-meta"));
  return { remaining: text(figure?.querySelector(".t-fig-l")) || null, total: amounts(line)[0] ?? null, pct: (line.match(/(\d+)%/) ?? [null, null])[1] };
})()`;

const BERANDA = String.raw`(() => {${HELPERS}
  const figures = Object.fromEntries([...document.querySelectorAll(".hero .fig-block, .legend .fig-block")].filter(visible)
    .map((block) => [text(block.querySelector(".t-meta")), text(block.querySelector(".t-fig"))]));
  return {
    scheduled: figures.Terjadwal ?? figures.Scheduled ?? null,
    spending: figures.Pengeluaran ?? figures.Spending ?? null,
  };
})()`;

const API_REQUESTS = `performance.getEntriesByType("resource").map((entry) => new URL(entry.name).pathname + new URL(entry.name).search).filter((name) => name.startsWith("/api/"))`;

const browser = await launch({ port: Number(args.port ?? 9773), profileDir: path.join(path.dirname(args.out), `.profile-parity-${args.port ?? 9773}`) });
try {
  await browser.setCell({ width: 1440, height: 900, theme: "light" });
  await browser.navigate(`${args.origin}/auth/login`);
  const status = await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) }).then((r) => r.status)`);
  if (status !== 200) throw new Error(`login ${status}`);
  await browser.evaluate(`localStorage.removeItem("hideBalances")`);

  await browser.navigate(`${args.origin}/goals`);
  await browser.settle();
  await sleep(1500);
  const requests = await browser.evaluate(API_REQUESTS);
  let page;
  let debts = [];
  if (args.side === "before") {
    page = await browser.evaluate(BEFORE_GOALS);
    const ids = await browser.evaluate(`fetch("/api/obligations").then((r) => r.json()).then((body) => body.obligations.filter((o) => !o.is_archived && o.remaining_amount > 0).map((o) => [o.id, o.name]))`);
    for (const [id, name] of ids) {
      await browser.navigate(`${args.origin}/goals?utang=${id}`);
      await browser.settle();
      await sleep(800);
      debts.push({ name, ...(await browser.evaluate(BEFORE_DEBT_DRAWER)) });
    }
  } else {
    // Every bill row: the list shows five until its toggle.
    await browser.evaluate(`[...document.querySelectorAll("[aria-labelledby=tg-bills] button[aria-expanded=false]")].forEach((button) => button.click())`);
    await sleep(500);
    page = await browser.evaluate(AFTER_GOALS);
    for (const row of page.debtRows) {
      await browser.navigate(`${args.origin}/goals?utang=${row.id}`);
      await browser.settle();
      await sleep(800);
      const dialog = await browser.evaluate(AFTER_DEBT_DIALOG);
      debts.push({ name: row.name, remaining: row.remaining, rowPct: row.pct, dialogRemaining: dialog?.remaining ?? null, total: dialog?.total ?? null, pct: dialog?.pct ?? null });
    }
  }

  await browser.navigate(`${args.origin}/`);
  await browser.settle();
  await sleep(1500);
  const beranda = await browser.evaluate(BERANDA);

  const result = {
    side: args.side,
    rev: args.rev ?? null,
    lang: args.lang ?? null,
    origin: args.origin,
    at: new Date().toISOString(),
    hero: page.hero,
    bills: page.bills,
    goals: page.goals,
    debts,
    debtsAggregate: page.debtsAggregate ?? null,
    credit: page.credit ?? null,
    panelAside: page.panelAside ?? null,
    beranda,
    requests: { goals: requests.length, goalsList: [...new Set(requests)].sort() },
  };
  mkdirSync(path.dirname(args.out), { recursive: true });
  writeFileSync(args.out, JSON.stringify(result, null, 1));
  console.log(JSON.stringify({ hero: result.hero, bills: { count: result.bills?.count, total: result.bills?.total }, goals: result.goals.length, debts: result.debts.length, beranda, requests: result.requests.goals }));
} finally {
  browser.close();
}
