// Request recorder for Catat (apply-markas-catat D24, task 5.4): the same inputs through the capture forms of one
// build, recording the method, path and JSON body of every write. Run on the build before the change (the former
// CatatDialog and QuickCaptureModal) and after it, on the same disposable owner, then compare with --compare.
// The flows drive the forms by their labels in both languages, so the same script records both builds.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node catat-requests.mjs --origin <url> --out <file.json> [--port 9812]
//        node catat-requests.mjs --compare <before.json> <after.json>
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const argv = process.argv.slice(2);
// Fields that differ by design between two runs: the capture's own id and the device clock.
const VOLATILE = new Set(["client_capture_id", "device_time", "reply_id"]);

if (argv[0] === "--compare") {
  const [before, after] = argv.slice(1, 3).map((file) => JSON.parse(readFileSync(file, "utf8")));
  const differences = [];
  for (const flow of Object.keys({ ...before, ...after })) {
    const a = before[flow] ?? [];
    const b = after[flow] ?? [];
    if (a.length !== b.length) differences.push({ flow, problem: `requests ${a.length} before, ${b.length} after` });
    for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
      const x = a[index] ?? {};
      const y = b[index] ?? {};
      if (x.method !== y.method || x.path !== y.path) differences.push({ flow, index, before: `${x.method} ${x.path}`, after: `${y.method} ${y.path}` });
      const keys = new Set([...Object.keys(x.body ?? {}), ...Object.keys(y.body ?? {})]);
      for (const key of keys) {
        if (VOLATILE.has(key)) continue;
        const left = JSON.stringify(x.body?.[key]);
        const right = JSON.stringify(y.body?.[key]);
        if (left !== right) differences.push({ flow, index, field: key, before: left ?? "(absent)", after: right ?? "(absent)" });
      }
    }
  }
  console.log(JSON.stringify(differences, null, 1));
  process.exit(0);
}

const args = Object.fromEntries(argv.reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
const browser = await launch({ port: Number(args.port ?? 9812), profileDir: path.join(path.dirname(args.out), ".profile-requests") });
const TIME = "2026-11-08T12:30:00";

// In-page driver: fields by label (either build, either language), clicks by text, writes recorded from fetch.
const DRIVER = `(() => {
  const visible = (e) => e && e.getBoundingClientRect().width > 0;
  const top = () => [...document.querySelectorAll('dialog[open], [role=dialog]')].filter(visible).pop() ?? document;
  const norm = (text) => (text || '').replace(/\\s+/g, ' ').trim();
  const control = (pattern) => {
    const re = new RegExp(pattern, 'i');
    const scope = top();
    for (const label of scope.querySelectorAll('label')) {
      if (re.test(norm(label.textContent)) && label.htmlFor) { const e = document.getElementById(label.htmlFor); if (e) return e; }
    }
    return [...scope.querySelectorAll('[aria-label]')].find((e) => re.test(e.getAttribute('aria-label')) && /INPUT|SELECT/.test(e.tagName)) ?? null;
  };
  const set = (el, value) => {
    const proto = el instanceof HTMLSelectElement ? HTMLSelectElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(proto, 'value').set.call(el, value);
    el.dispatchEvent(new Event(el instanceof HTMLSelectElement ? 'change' : 'input', { bubbles: true }));
  };
  window.__d = {
    openDetails: () => { top().querySelectorAll('details').forEach((d) => { d.open = true; d.dispatchEvent(new Event('toggle')); }); return 'details'; },
    fill: (pattern, value) => { const el = control(pattern); if (!el) return 'NOT FOUND ' + pattern; set(el, value); return 'filled ' + pattern; },
    pick: (pattern, option) => {
      const el = control(pattern); if (!el) return 'NOT FOUND ' + pattern;
      const o = [...el.options].find((item) => new RegExp(option).test(norm(item.textContent))); if (!o) return 'NO OPTION ' + option;
      set(el, o.value); return 'picked ' + norm(o.textContent).slice(0, 30);
    },
    click: (pattern) => {
      const re = new RegExp(pattern, 'i');
      const b = [...top().querySelectorAll('button, [role=radio]')].filter(visible).find((e) => re.test(norm(e.getAttribute('aria-label') || e.textContent)));
      if (!b) return 'NOT FOUND ' + pattern; b.click(); return 'clicked ' + norm(b.textContent).slice(0, 30);
    },
    record: () => { if (window.__writes) return; window.__writes = []; const original = window.fetch; window.fetch = (input, init) => { const method = (init?.method ?? 'GET').toUpperCase(); if (method !== 'GET') { let body = init?.body ?? null; try { body = body ? JSON.parse(body) : null; } catch { body = '(form data)'; } window.__writes.push({ method, path: String(input).replace(location.origin, ''), body }); } return original(input, init); }; },
  };
  window.__d.record();
  return 'driver';
})()`;

const step = async (expression, after = 500) => {
  const result = await browser.evaluate(expression);
  await sleep(after);
  if (String(result).startsWith("NOT FOUND") || String(result).startsWith("NO OPTION")) throw new Error(result);
  return result;
};
const N = ["n", "KeyN", 78, 0, "n"];
const MANUAL = "^(Isi manual|Fill in manually)$";
const SAVE = "^(Simpan transaksi|Save transaction)$";
const MOVE = "^(Pindahkan saldo|Move balance)$";

async function home() {
  await browser.navigate(`${args.origin}/`);
  await browser.settle();
  await sleep(800);
  await step(DRIVER, 100);
}
async function manual(mode) {
  await home();
  await browser.key(...N);
  await sleep(1200);
  await step(`__d.click(${JSON.stringify(MANUAL)})`, 1500);
  await step(DRIVER, 100);
  if (mode) await step(`__d.click(${JSON.stringify(mode)})`, 800);
  await step("__d.openDetails()", 300);
}

const FLOWS = {
  "sentence capture": async () => {
    await home();
    await browser.key(...N);
    await sleep(1200);
    await step(`__d.fill("^(Tulis transaksi|Kalimat transaksi|Transaction sentence)$", document.documentElement.lang === "en" ? "parking 5k" : "parkir 5rb")`, 1500);
    await step(`__d.click("^(Catat|Record)$")`, 2500);
    // A follow-up question is answered with its first option.
    await browser.evaluate(`(() => { const g = [...document.querySelectorAll('dialog[open] [role=group], [role=dialog] [role=group]')].find((e) => e.querySelector('button')); g?.querySelector('button')?.click(); })()`);
    await sleep(2000);
  },
  "expense with a category": async () => {
    await manual();
    await step(`__d.fill("^(Nominal|Amount) \\\\(IDR\\\\)$", "35000")`);
    await step(`__d.pick("^(Bayar dari|Pay from)$", "^GoPay")`);
    await step(`__d.pick("^(Kategori|Category)$", "^Makanan")`);
    await step(`__d.fill("^(Waktu transaksi|Time)$", "${TIME}")`);
    await step(`__d.fill("^(Catatan|Note)", "uji catat")`);
    await step(`__d.click(${JSON.stringify(SAVE)})`, 2500);
  },
  "expense with a pillar override and a goal": async () => {
    await manual();
    await step(`__d.fill("^(Nominal|Amount) \\\\(IDR\\\\)$", "25000+15000")`);
    await step(`__d.pick("^(Bayar dari|Pay from)$", "^GoPay")`);
    await step(`__d.pick("^(Kategori|Category)$", "^Makanan")`);
    await step(`__d.click("^(Keinginan|Wants)$")`);
    await step(`__d.fill("^(Waktu transaksi|Time)$", "${TIME}")`);
    await step(`__d.pick("^(Kaitkan ke target|Link to a goal)", "Dana Darurat")`);
    await step(`__d.click(${JSON.stringify(SAVE)})`, 2500);
  },
  "expense split across three debts": async () => {
    await manual();
    await step(`__d.pick("^(Bayar dari|Pay from)$", "^BCA")`);
    await step(`__d.fill("^(Waktu transaksi|Time)$", "${TIME}")`);
    for (const [index, debt] of [[1, "Cicilan Motor"], [2, "Pinjaman"], [3, "Tagihan Internet"]]) {
      await step(`__d.click("^(\\\\+ Tambah tagihan|\\\\+ Add a bill)$")`, 600);
      await step(`__d.pick("^(Tagihan|Bill) ${index}$", "^${debt}")`, 600);
    }
    await step(`__d.click("^(Ubah jumlah|Change amounts)$")`, 600);
    for (const [index, amount] of [[1, "100000"], [2, "50000"], [3, "25000"]]) await step(`__d.fill("^(Dibayar untuk tagihan|Paid for bill) ${index}", "${amount}")`, 300);
    await step(`__d.click(${JSON.stringify(SAVE)})`, 2500);
  },
  "income with a reimbursement": async () => {
    await manual("^(Pemasukan|Income)$");
    await step(`__d.fill("^(Nominal|Amount) \\\\(IDR\\\\)$", "150000")`);
    await step(`__d.pick("^(Masuk ke|Paid into)$", "^BCA")`);
    await step(`__d.pick("^(Kategori|Kategori pemasukan|Income category)$", "^Patungan")`);
    await step(`__d.pick("^(Mengurangi pengeluaran|Reduces spending in)$", "^Makanan")`);
    await step(`__d.fill("^(Waktu transaksi|Time)$", "${TIME}")`);
    await step(`__d.click(${JSON.stringify(SAVE)})`, 2500);
  },
  "movement into credit with a fee": async () => {
    await manual("^(Perpindahan|Transfer)$");
    await step(`__d.fill("^(Nominal|Amount) \\\\(IDR\\\\)$", "434000")`);
    await step(`__d.pick("^(Dari rekening|From account)$", "^Transport")`);
    await step(`__d.pick("^(Ke rekening|To account)$", "^SPayLater")`, 800);
    await step(`__d.fill("^(Biaya / bunga|Fee / interest)", "2500")`);
    await step(`__d.fill("^(Waktu pemindahan|Transfer time)$", "${TIME}")`);
    await step(`__d.click(${JSON.stringify(MOVE)})`, 2500);
  },
  "credit purchase paid in full": async () => {
    await manual();
    await step(`__d.fill("^(Nominal|Amount) \\\\(IDR\\\\)$", "120000")`);
    await step(`__d.pick("^(Bayar dari|Pay from)$", "^SPayLater")`, 800);
    await step(`__d.pick("^(Kategori|Category)$", "^Makanan")`);
    await step(`__d.fill("^(Waktu transaksi|Time)$", "${TIME}")`);
    await step(`__d.click(${JSON.stringify(SAVE)})`, 2500);
  },
  "credit purchase in 3 with an edited date": async () => {
    await manual();
    await step(`__d.fill("^(Nominal|Amount) \\\\(IDR\\\\)$", "1000000")`);
    await step(`__d.pick("^(Bayar dari|Pay from)$", "^SPayLater")`, 800);
    await step(`__d.fill("^(Waktu transaksi|Time)$", "${TIME}")`);
    // The former form has no instalment choice; only the build with H11 can set these.
    await browser.evaluate(`__d.click("^3x$")`);
    await browser.evaluate(`__d.fill("^(Jatuh tempo pertama|First due date)$", "2026-12-20")`);
    await sleep(400);
    await step(`__d.click(${JSON.stringify(SAVE)})`, 2500);
  },
  "bill from the bell": async () => {
    await home();
    await step(`__d.click("^(Notifikasi|Notifications)")`, 900);
    await step(`(() => { const item = [...document.querySelectorAll('.notif')].find((e) => /SPayLater/.test(e.textContent)); if (!item) return 'NOT FOUND bill'; item.click(); return 'clicked bill'; })()`, 1800);
    await step(DRIVER, 100);
    await step(`__d.pick("^(Dari rekening|From account)$", "^Transport")`);
    await step(`__d.fill("^(Waktu pemindahan|Transfer time)$", "${TIME}")`);
    await step(`__d.click(${JSON.stringify(MOVE)})`, 2500);
  },
};

const output = {};
try {
  await browser.setCell({ width: 1440, height: 900, theme: "light", mobile: false });
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
  await sleep(500);
  for (const [name, run] of Object.entries(FLOWS)) {
    try {
      await run();
      output[name] = (await browser.evaluate("window.__writes ?? []")).filter((write) => !/\/api\/(capture\/parse)$/.test(write.path));
      console.log("flow", name, output[name].length, "writes");
    } catch (error) {
      output[name] = [{ error: String(error).slice(0, 160) }];
      console.log("flow", name, "ERROR", String(error).slice(0, 160));
    }
  }
} finally {
  writeFileSync(args.out, JSON.stringify(output, null, 1));
  browser.close();
}
