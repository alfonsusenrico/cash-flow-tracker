// In-page Target & Tagihan checklist (apply-markas-target D26), from D1 to D20 of the change's design.md and option-6
// SPEC §2, §5 to §7 and §13. Evaluated by the Target capture runs on /goals, its dialogs and Beranda's H9 parts.
// Every check is { check, value, expected, pass }.
(() => {
  const checks = [];
  const add = (check, value, expected, tolerance = 0) => {
    const pass = typeof expected === "number" ? Math.abs(Number(value) - expected) <= tolerance : value === expected;
    checks.push({ check, value, expected, pass });
  };
  const within = (check, value, allowed) => checks.push({ check, value, expected: allowed, pass: value.every((item) => allowed.includes(item)) });
  const shown = (element) => Boolean(element) && element.getBoundingClientRect().width > 0 && element.getBoundingClientRect().height > 0;
  const all = (selector, scope = document) => [...scope.querySelectorAll(selector)].filter(shown);
  const first = (selector, scope = document) => all(selector, scope)[0] ?? null;
  const style = (element, pseudo) => getComputedStyle(element, pseudo);
  const box = (element) => element.getBoundingClientRect();
  const px = (value) => Math.round(parseFloat(value) * 100) / 100;
  const phone = innerWidth < 768;
  const tokens = getComputedStyle(document.documentElement);
  const probe = document.createElement("span");
  document.body.appendChild(probe);
  const rgb = (name) => {
    probe.style.color = tokens.getPropertyValue(name).trim();
    return getComputedStyle(probe).color;
  };

  // Every short date in a time column ("10 Nov", "30 Sept") sits on one 16 px line and fits its column.
  const dates = all(".row .r-time.is-date");
  if (dates.length) {
    add("date cells on one line", dates.filter((cell) => Math.round(box(cell).height) > 16 || cell.scrollWidth > cell.clientWidth + 1).map((cell) => cell.textContent).join(", "), "");
    add("date cells tabular", dates.every((cell) => /tabular-nums/.test(style(cell).fontVariantNumeric)), true);
  }

  const dialogs = all("dialog.shell-dialog[open]");
  const dialog = dialogs[dialogs.length - 1] ?? null;
  const page = first(".tg-page");
  const scope = dialog ?? document.querySelector("main") ?? document.body;

  add("no horizontal page overflow", document.documentElement.scrollWidth > innerWidth, false);

  const hero = page ? first("section.hero", page) : null;
  if (hero && !dialog) {
    if (innerWidth === 1440) add("hero starts at y = 192", Math.round(box(hero).top + scrollY), 192, 1);
    const top = first(".hero-top", hero);
    if (top) add("hero label row min-height 32", px(style(top).minHeight), 32);
    const figure = first(".t-hero", hero);
    if (figure && !first(".is-words", figure)) {
      add("hero figure 44 px (32 below 360)", px(style(figure).fontSize), innerWidth < 360 ? 32 : 44);
      add("hero figure weight 600", style(figure).fontWeight, "600");
      add("hero figure Manrope", /manrope/i.test(style(figure).fontFamily), true);
    }
    const line = first(".hero-sub", hero);
    if (line) add("hero line 14/20", `${px(style(line).fontSize)}/${px(style(line).lineHeight)}`, "14/20");
    const bar = first(".tg-bar", hero);
    if (bar) {
      add("thick bar 8 px", px(style(first(".prog", bar)).height), 8);
      add("thick bar margin-top 24", px(style(bar).marginTop), 24);
      add("thick bar square", px(style(first(".prog", bar)).borderTopLeftRadius), 0);
    }
    const supports = first(".tg-supports", hero);
    if (supports) {
      add("supporting figures columns (4, 2 on a phone)", style(supports).gridTemplateColumns.split(" ").length, phone ? 2 : 4);
      add("supporting figures shown (4, 2 on a phone)", all(".fig-block", supports).length, phone ? 2 : 4);
      add("supporting figures gap (24, 16 on a phone)", px(style(supports).columnGap), phone ? 16 : 24);
    }
    add("one 44 px figure", all(".t-hero").length, 1);
    const big = all("main *").filter((element) => element.childElementCount === 0 && px(style(element).fontSize) >= 33 && !element.closest(".t-hero"));
    add("no other text of 33 px or more", big.length, 0);
    add("one Rincian control", all("main button").filter((button) => /^(Rincian|Breakdown)$/.test(button.textContent.trim())).length, 1);
  }
  const cols = page ? first(".tg-cols", page) : null;
  if (cols && !dialog) {
    const columns = style(cols).gridTemplateColumns.split(" ");
    if (innerWidth >= 1200) {
      add("reading column and 352 px panel", `${columns.length} ${columns[1]}`, "2 352px");
      add("columns gap 24", px(style(cols).columnGap), 24);
    } else add("one column below 1200", columns.length, 1);
    add("sections 48 apart (32 on a phone)", Math.round(box(cols).top - box(hero ?? cols).bottom), phone ? 32 : 48, 1);
  }
  const header = page ? first(".sec-h", page) : null;
  if (header && !dialog) {
    add("section header min-height 32", px(style(header).minHeight), 32);
    add("section header margin-bottom 12", px(style(header).marginBottom), 12);
  }
  const billRow = page ? first("[aria-labelledby=tg-bills] .list .row") : null;
  if (billRow && !dialog) {
    add("bill row min-height 56 (64 phone)", px(style(billRow).minHeight), phone ? 64 : 56);
    add("bill row padding 0 12 (left)", px(style(billRow).paddingLeft), 12);
    const time = first(".r-time", billRow);
    if (time && !phone) add("bill date column 48", Math.round(box(time).width), 48);
    if (phone) add("bill date in the phone meta", Boolean(first(".r-meta.ph-only", billRow)), true);
  }
  const goalRow = page ? first(".row.goal-row") : null;
  if (goalRow && !dialog) {
    add("goal row min-height 72", px(style(goalRow).minHeight), 72);
    add("goal row at least 72 tall", Math.round(box(goalRow).height) >= 72, true);
    const prog = first(".prog", goalRow);
    if (prog) {
      add("goal bar 4 px", px(style(prog).height), 4);
      add("goal bar max-width 320", px(style(prog).maxWidth), 320);
      add("goal bar margin-top 8", px(style(prog).marginTop), 8);
    }
  }
  const stacked = page ? first(".panel .row.stacked") : null;
  if (stacked && !dialog) {
    add("stacked row padding 12/12", `${px(style(stacked).paddingTop)}/${px(style(stacked).paddingBottom)}`, "12/12");
    const prog = first(".prog", stacked);
    if (prog) {
      add("stacked bar 4 px", px(style(prog).height), 4);
      add("stacked bar margin 8 0 4", `${px(style(prog).marginTop)} ${px(style(prog).marginBottom)}`, "8 4");
    }
  }
  const panel = page ? first(".panel", page) : null;
  if (panel && !dialog) {
    add("panel padding 24 (16 phone)", px(style(panel).paddingTop), phone ? 16 : 24);
    add("panel radius 12", px(style(panel).borderTopLeftRadius), 12);
    add("panel surface", style(panel).backgroundColor, rgb("--surface"));
  }
  const expense = first("main .r-meta .c-expense, main .line .c-expense");
  if (expense && !dialog) add("overdue word in the expense colour", style(expense).color, rgb("--out-fg"));
  const warning = first("main .r-meta .c-warning, main .line .c-warning");
  if (warning && !dialog) add("due-today word in the warning colour", style(warning).color, rgb("--warn-fg"));

  if (dialog) {
    const width = Math.round(box(dialog).width);
    if (!phone) checks.push({ check: "dialog width 480/560/880", value: width, expected: [480, 560, 880], pass: [480, 560, 880].includes(width) });
    if (!phone) add("dialog radius 16", px(style(dialog).borderTopLeftRadius), 16);
    if (phone) add("sheet spans the screen", width >= innerWidth - 1, true);
    const head = first(".dlg-h", dialog);
    if (head) add("dialog header padding 24 24 16 (sheet 16 16 12)", `${px(style(head).paddingTop)} ${px(style(head).paddingLeft)} ${px(style(head).paddingBottom)}`, phone ? "16 16 12" : "24 24 16");
    const footer = first(".dlg-f", dialog);
    if (footer) add("dialog footer padding 16 24 (sheet 12 16)", `${px(style(footer).paddingTop)} ${px(style(footer).paddingLeft)}`, phone ? "12 16" : "16 24");
    const figures = all("*", dialog).filter((element) => element.childElementCount === 0 && px(style(element).fontSize) >= 33);
    add("no dialog text of 33 px or more", figures.length, 0);
    const grid = first(".rc-grid", dialog);
    if (grid) add("plans list and detail (one column on a phone)", style(grid).gridTemplateColumns.split(" ").length, phone ? 1 : 2);
  }

  // Type scale, weights, text styles and borders over the visible text and boxes of the page or dialog.
  const texts = all("*", scope).filter((element) => [...element.childNodes].some((node) => node.nodeType === 3 && node.textContent.trim()));
  const sizes = [...new Set(texts.map((element) => px(style(element).fontSize)))].sort((a, b) => a - b);
  const weights = [...new Set(texts.map((element) => style(element).fontWeight))].sort();
  within("font sizes", sizes, [12, 14, 16, 20, 24, 32, 44]);
  within("font weights", weights, ["400", "500", "600"]);
  const styles = new Set(texts.filter((element) => !element.closest("aside.ws-side, nav, header")).map((element) => `${px(style(element).fontSize)}/${style(element).fontWeight}`));
  checks.push({ check: "text styles (size/weight) in view", value: styles.size, expected: "≤ 8", pass: styles.size <= 8 });
  const thick = all("*", scope).filter((element) => {
    const s = style(element);
    return ["Top", "Right", "Bottom", "Left"].some((side) => px(s[`border${side}Width`]) >= 2 && s[`border${side}Style`] !== "none");
  }).map((element) => element.tagName.toLowerCase() + "." + [...element.classList].slice(0, 2).join("."));
  add("no border of 2 px or more", thick.length, 0);
  const neutral = ["--line", "--line-strong", "--field-focus", "--muted"].map(rgb);
  // Legend dots are colour marks (SPEC §3), not borders of a box.
  const coloured = all("*", scope).filter((element) => {
    const s = style(element);
    return ["Top", "Right", "Bottom", "Left"].some((side) => s[`border${side}Style`] !== "none" && px(s[`border${side}Width`]) > 0 &&
      !/rgba\(0, 0, 0, 0\)|, 0\)$/.test(s[`border${side}Color`]) && !neutral.includes(s[`border${side}Color`]) && element !== document.activeElement && !element.classList.contains("dot"));
  }).map((element) => element.tagName.toLowerCase() + "." + [...element.classList].slice(0, 2).join("."));
  checks.push({ check: "no coloured border (selection bar and focus aside)", value: coloured.slice(0, 6), expected: [], pass: coloured.length === 0 });
  probe.remove();
  return { pass: checks.filter((item) => item.pass).length, fail: checks.filter((item) => !item.pass).length, checks };
})()
