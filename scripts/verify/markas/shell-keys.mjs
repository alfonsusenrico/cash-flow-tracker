// Keyboard flows of the shell (apply-markas-shell D16) on a running app: skip link, ⌘K, N, ?, the menus with
// ↑/↓ and Esc, the Lainnya focus trap, focus never under the phone header or the tab bar, sticky page parts
// below the top bar, and the toast position. Writes shell-keys.json.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node shell-keys.mjs --origin http://127.0.0.1:3491 --out <dir>
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 2) args[argv[index].replace(/^--/, "")] = argv[index + 1];
  return args;
}

const args = parseArgs(process.argv.slice(2));
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9647), profileDir: path.join(args.out, ".profile-keys") });
const results = [];
const record = (check, cell, pass, detail) => {
  results.push({ check, cell, pass, detail });
  console.log(pass ? "PASS" : "FAIL", cell, check, detail ?? "");
};
const active = () => browser.evaluate(`(() => { const a = document.activeElement; return a ? (a.getAttribute("aria-label") || a.id || a.textContent.trim().slice(0, 40)) : null; })()`);
const TAB = ["Tab", "Tab", 9];
const ESC = ["Escape", "Escape", 27];
const DOWN = ["ArrowDown", "ArrowDown", 40];

async function open(url) {
  await browser.navigate(args.origin + url);
  await browser.settle();
  await sleep(400);
}

try {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "raka_contoh", password: process.env.MARKAS_PASSWORD ?? "contoh-sandi-123" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);

  for (const theme of ["light", "dark"]) {
    const cell = `1440-${theme}`;
    await browser.setCell({ width: 1440, height: 900, theme });
    await browser.evaluate(`localStorage.setItem("theme", "${theme}")`);

    await open("/");
    await browser.key(...TAB);
    const skip = await active();
    await browser.key("Enter", "Enter", 13);
    await sleep(200);
    record("skip link is the first stop and moves focus to main", cell, skip === "Lewati ke isi" && (await active()) === "main", `${skip} → ${await active()}`);
    await browser.key(...TAB);
    record("focus after the skip link starts inside main", cell, await browser.evaluate(`document.getElementById("main").contains(document.activeElement)`), await active());

    await open("/");
    await browser.key("k", "KeyK", 75, 4);
    record("⌘K focuses the top-bar search on Beranda", cell, (await active()) === "Cari transaksi");
    await open("/ledger?period=cycle");
    await browser.key("k", "KeyK", 75, 4);
    record("⌘K focuses Transaksi's own search", cell, (await active()) === "ledger-search");

    await open("/");
    await browser.evaluate("document.activeElement && document.activeElement.blur()");
    await browser.key("n", "KeyN", 78, 0, "n");
    await sleep(600);
    record("N opens Catat", cell, await browser.evaluate(`Boolean([...document.querySelectorAll('[role=dialog]')].find((d) => /Catat/.test(d.getAttribute("aria-label") || d.textContent)))`));
    await browser.key(...ESC);
    await sleep(300);

    await browser.evaluate("document.activeElement && document.activeElement.blur()");
    await browser.key("?", "Slash", 191, 8, "?");
    await sleep(500);
    const help = await browser.evaluate(`Boolean(document.querySelector("dialog.shell-dialog[open]"))`);
    await browser.key(...ESC);
    await sleep(300);
    record("? opens Bantuan & pintasan and Esc closes it", cell, help && !(await browser.evaluate(`Boolean(document.querySelector("dialog.shell-dialog[open]"))`)));

    await browser.evaluate(`document.querySelector('.topbar [aria-label="Tema"]').focus()`);
    await browser.key("Enter", "Enter", 13);
    await sleep(300);
    const firstItem = await active();
    await browser.key(...DOWN);
    const second = await active();
    await browser.key(...ESC);
    await sleep(200);
    record("Tema menu: focus on the checked item, ↓ moves, Esc returns to Tema", cell, Boolean(firstItem) && second !== firstItem && (await active()) === "Tema", `${firstItem} → ${second} → ${await active()}`);

    await browser.evaluate(`document.querySelector('.user-btn').focus()`);
    await browser.key("Enter", "Enter", 13);
    await sleep(300);
    const profil = await active();
    await browser.key(...DOWN);
    const keluar = await active();
    await browser.key(...ESC);
    await sleep(200);
    record("account menu: Profil, ↓ Keluar, Esc back to the user button", cell, profil === "Profil" && keluar === "Keluar" && /^Akun/.test(await active()), `${profil} → ${keluar} → ${await active()}`);

    await browser.evaluate(`document.querySelector('.topbar .bell').focus()`);
    await browser.key("Enter", "Enter", 13);
    await sleep(300);
    const inside = await browser.evaluate(`Boolean(document.querySelector('.popover[role=dialog]')?.contains(document.activeElement))`);
    await browser.key(...ESC);
    await sleep(200);
    record("bell: focus inside the popover, Esc back to the bell", cell, inside && /^Notifikasi/.test(await active()));

    // Sticky page parts sit below the 56 px top bar after scrolling.
    for (const [url, selector] of [["/accounts", "[class*=summaryFirst]"], ["/review", ".ws-tri-list"], ["/settings/tampilan", "[class*=settingsSide]"]]) {
      await open(url);
      const top = await browser.evaluate(`(() => { const el = document.querySelector(${JSON.stringify(selector)}); if (!el) return null; window.scrollTo(0, 400); return new Promise((r) => setTimeout(() => r(getComputedStyle(el).position === "sticky" ? Math.round(el.getBoundingClientRect().top) : "static"), 300)); })()`);
      record(`sticky part below the top bar on ${url}`, cell, top === null || top === "static" || top >= 56, String(top));
    }
  }

  for (const width of [390, 320]) {
    for (const theme of ["light", "dark"]) {
      const cell = `${width}-${theme}`;
      await browser.setCell({ width, height: 844, theme, mobile: true });
      await browser.evaluate(`localStorage.setItem("theme", "${theme}")`);

      await open("/");
      await browser.evaluate(`[...document.querySelectorAll('.tabbar button')].find((b) => b.textContent.includes("Lainnya")).focus()`);
      await browser.key("Enter", "Enter", 13);
      await sleep(400);
      let trapped = true;
      for (let step = 0; step < 14; step += 1) {
        await browser.key(...TAB);
        trapped = trapped && (await browser.evaluate(`Boolean(document.querySelector("dialog.shell-dialog[open]")?.contains(document.activeElement))`));
      }
      await browser.key(...ESC);
      await sleep(300);
      record("Lainnya traps Tab and Esc returns to Lainnya", cell, trapped && (await active()) === "Lainnya");

      // Tab through a long page: every focused element is fully between the phone header and the tab bar.
      for (const url of ["/ledger?period=cycle", "/goals", "/accounts"]) {
        await open(url);
        const hidden = await browser.evaluate(`(async () => {
          const header = document.querySelector(".ph-header").getBoundingClientRect().bottom;
          const bar = document.querySelector(".tabbar").getBoundingClientRect().top;
          const items = [...document.querySelectorAll("main a[href], main button:not([disabled]), main input, main [tabindex='0']")].filter((el) => el.getBoundingClientRect().width > 0);
          const bad = [];
          for (const el of items.slice(-12)) {
            el.focus();
            await new Promise((r) => setTimeout(r, 30));
            const box = el.getBoundingClientRect();
            if (box.height < innerHeight / 2 && (box.top < header - 1 || box.bottom > bar + 1)) bad.push((el.getAttribute("aria-label") || el.textContent).trim().slice(0, 30) + " " + Math.round(box.top) + "–" + Math.round(box.bottom));
          }
          return bad;
        })()`);
        record(`focus not obscured on ${url}`, cell, hidden.length === 0, hidden.join("; "));
      }

      await open("/");
      await browser.evaluate(`document.querySelector('.ph-header [aria-label="Cari transaksi"]').click()`);
      await sleep(1500);
      const focused = await active();
      const search = await browser.evaluate("location.search");
      record("phone search opens Transaksi with its search focused and drops fokus", cell, focused === "ledger-search" && !search.includes("fokus"), `${focused} ${search}`);
    }
  }

  // Toast placement (D11): read from an element with the app's toast class once its 200 ms rise has ended.
  for (const [width, mobile, expected] of [[1440, false, 32], [390, true, 64 + 16]]) {
    await browser.setCell({ width, height: 844, theme: "light", mobile });
    await open("/");
    const bottom = await browser.evaluate(`(() => { const t = document.createElement("div"); t.className = "ws-toast"; t.textContent = "Contoh"; document.body.appendChild(t); return new Promise((resolve) => setTimeout(() => { const gap = innerHeight - t.getBoundingClientRect().bottom; t.remove(); resolve(Math.round(gap)); }, 500)); })()`);
    record(`toast bottom edge ${expected} px above the viewport edge`, `${width}`, bottom === expected, String(bottom));
  }
} finally {
  browser.close();
}
writeFileSync(path.join(args.out, "shell-keys.json"), JSON.stringify({ passed: results.filter((r) => r.pass).length, failed: results.filter((r) => !r.pass).length, results }, null, 1));
