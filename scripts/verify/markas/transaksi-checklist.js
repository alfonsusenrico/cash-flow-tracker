// In-page Transaksi checklist (apply-markas-transaksi D24), from D1, D3 to D14 and D29 of the change's design.md and
// option-6 SPEC §2, §5 to §7 and §13. Evaluated by capture.mjs for scenarios marked `transaksi`. Every check is
// { check, value, expected, pass }.
(() => {
  const checks = [];
  const add = (check, value, expected, tolerance = 0) => {
    const pass = typeof expected === "number" ? Math.abs(Number(value) - expected) <= tolerance : value === expected;
    checks.push({ check, value, expected, pass });
  };
  const atLeast = (check, value, minimum) => checks.push({ check, value, expected: `>= ${minimum}`, pass: Number(value) >= minimum });
  const shown = (element) => Boolean(element) && element.getBoundingClientRect().width > 0 && element.getBoundingClientRect().height > 0;
  const all = (selector, scope = document) => [...scope.querySelectorAll(selector)].filter(shown);
  const first = (selector, scope = document) => all(selector, scope)[0] ?? null;
  const style = (element, pseudo) => getComputedStyle(element, pseudo);
  const box = (element) => element.getBoundingClientRect();
  const px = (value) => Math.round(parseFloat(value) * 100) / 100;
  const font = (element) => `${px(style(element).fontSize)}/${px(style(element).lineHeight)} ${style(element).fontWeight}`;
  const phone = innerWidth < 768;
  const wide = innerWidth >= 1200;
  // The expense ink as the page resolves it, to compare every "−Rp" amount with (D29).
  const probe = document.createElement("span");
  probe.style.color = "var(--out-fg)";
  document.body.appendChild(probe);
  const expenseInk = style(probe).color;
  probe.remove();

  const dialog = first("dialog.shell-dialog[open]");
  const tx = first(".tx");
  const hero = first(".hero");

  if (hero && !dialog) {
    if (innerWidth === 1440) add("hero starts at y = 192", Math.round(box(hero).top + scrollY), 192, 1);
    add("one 44 px figure", all(".t-hero").length, 1);
    const figure = first(".t-hero");
    const words = figure && first(".is-words, .skel", figure);
    if (figure && !words) add("hero figure 44/52 600 (32/40 below 360 px)", font(figure), innerWidth < 360 ? "32/40 600" : "44/52 600");
    if (hero.classList.contains("is-split")) {
      add("split hero gap", `${px(style(hero).rowGap)} ${px(style(hero).columnGap)}`, phone ? "16 16" : "24 48");
      add("split hero margin-bottom", px(style(hero).marginBottom), phone ? 24 : 32);
    }
  }

  if (tx && !dialog) {
    const columns = style(tx).gridTemplateColumns.split(" ").map((value) => Math.round(parseFloat(value)));
    const rail = first(".tx-rail");
    const types = first(".tx-types");
    if (wide) {
      add(".tx rail column", columns[0], 200);
      add(".tx gap", px(style(tx).columnGap), 32);
      if (innerWidth === 1440) add(".tx list column", columns[1], 808, 1);
      add("rail is sticky", rail ? style(rail).position : "missing", "sticky");
      if (rail) add("rail sticky top", px(style(rail).top), 80);
      add("type control hidden from 1200 px", types, null);
      const item = first(".tx-rail .rail-item");
      if (item) {
        add("rail item height", Math.round(box(item).height), 36);
        add("rail item padding", `${px(style(item).paddingLeft)} ${px(style(item).paddingRight)}`, "12 12");
        add("rail item radius", px(style(item).borderTopLeftRadius), 6);
      }
      const pressed = first(".tx-rail .rail-item[aria-pressed='true']");
      if (pressed) {
        const bar = style(pressed, "::before");
        add("pressed bar 2 × 20", `${px(bar.width)} × ${px(bar.height)}`, "2 × 20");
        add("pressed bar left", px(bar.left), -8);
      }
      const rule = first(".tx-rail .rail-sep");
      if (rule) add("rail rule 1 px", Math.round(box(rule).height), 1);
    } else {
      add(".tx is one column", columns.length, 1);
      add("rail hidden below 1200 px", rail, null);
      add("type control shown below 1200 px", Boolean(types), true);
      if (types) {
        add("type control has seven types", all("button", types).length, 7);
        add("type control scrolls inside itself", style(types).overflowX, "auto");
        add("type item height", Math.round(box(first("button", types)).height), phone ? 40 : 30, phone ? 0 : 1);
      }
    }
    const search = first(".toolbar .search input");
    if (search) {
      add("search height", Math.round(box(search).height), phone ? 40 : 36, phone ? 8 : 0);
      atLeast("search min-width", Math.round(box(first(".toolbar .search")).width), phone ? 0 : 200);
    }
    const chip = first(".chip");
    if (chip) {
      add("chip height", Math.round(box(chip).height), 28);
      add("chip padding", `${px(style(chip).paddingLeft)} ${px(style(chip).paddingRight)}`, "12 4");
      add("chip radius", px(style(chip).borderTopLeftRadius), 6);
      add("chip remove button", Math.round(box(first("button", chip)).width), 24);
    }
    const bar = first(".sel-bar");
    if (bar) {
      atLeast("selection bar height", Math.round(box(bar).height), 48);
      add("selection bar radius", px(style(bar).borderTopLeftRadius), 6);
      add("selection bar sticky", style(bar).position, "sticky");
    }
    const heading = first(".day-h");
    if (heading) {
      add("day heading height", Math.round(box(heading).height), 40);
      // While merging, headings stick below the 48 px selection bar (D9).
      add("day heading sticky top", px(style(heading).top), first(".tx-list.is-selecting") ? 104 : 56);
      const totals = first(".tot", heading);
      if (totals) add("day totals 12 muted", `${px(style(totals).fontSize)} ${style(totals).color}`, `12 ${style(first(".muted") ?? totals).color}`);
    }
    const rows = all(".tx-list .row");
    if (rows[0]) {
      atLeast("row min-height", Math.round(box(rows[0]).height), phone ? 64 : 56);
      add("row padding x", `${px(style(rows[0]).paddingLeft)} ${px(style(rows[0]).paddingRight)}`, "12 12");
      if (!phone) {
        add("row gap", px(style(rows[0]).columnGap), 16);
        add("time column", Math.round(box(first(".r-time", rows[0])).width), 40);
        add("category column", Math.round(box(first(".r-cat", rows[0])).width), 168);
        add("pillar column", Math.round(box(first(".r-pillar", rows[0])).width), 136);
        atLeast("amount column", Math.round(box(first(".r-amt", rows[0])).width), 120);
      }
      const clipped = rows.filter((row) => row.scrollWidth > row.clientWidth + 1);
      add("no row overflows its width", clipped.length, 0);
    }
  }

  // Every spending amount on the page or in the open dialog is the expense ink (D29), and every amount is tabular.
  const amounts = all(".t-amt, .t-fig-l");
  const expenses = amounts.filter((element) => /^−/.test(element.textContent.trim()));
  const offInk = expenses.filter((element) => style(element).color !== expenseInk);
  if (expenses.length) add(`spending amounts in --out-fg (${expenses.length})`, offInk.length, 0);
  const notTabular = amounts.filter((element) => !style(element).fontVariantNumeric.includes("tabular-nums"));
  if (amounts.length) add("amounts tabular", notTabular.length, 0);

  if (dialog) {
    const width = Math.round(box(dialog).width);
    if (phone) add("dialog is a bottom sheet at full width", width, innerWidth, 1);
    else {
      atLeast("dialog centred: left gap equals right gap", -Math.abs(box(dialog).left - (innerWidth - box(dialog).right)), -1);
      add("dialog width matches its --dw", width, Math.min(parseFloat(dialog.style.getPropertyValue("--dw")), innerWidth - 32), 1);
      add("dialog radius", px(style(dialog).borderTopLeftRadius), 16);
    }
    const header = first(".dlg-h", dialog);
    if (header && !phone) add("dialog header padding", `${px(style(header).paddingTop)} ${px(style(header).paddingLeft)} ${px(style(header).paddingBottom)}`, "24 24 16");
    const footer = first(".dlg-f", dialog);
    if (footer && !phone) add("dialog footer padding", `${px(style(footer).paddingTop)} ${px(style(footer).paddingLeft)}`, "16 24");
    const list = first(".dl", dialog);
    if (list && !phone) add(".dl label column", Math.round(parseFloat(style(list).gridTemplateColumns.split(" ")[0])), 160);
  }

  const stepper = first(".stepper");
  if (stepper) {
    const button = first(".ws-icon-btn", stepper);
    if (button) add("stepper button", Math.round(box(button).width), phone ? 44 : 36);
    const label = first(".cur", stepper);
    if (label && !phone) atLeast("stepper label min-width", Math.round(box(label).width), 168);
  }

  // SPEC §13: sizes, weights, no other text of 33 px or more, no border of 2 px or more, no horizontal overflow.
  const texts = all("main *, dialog *").filter((element) => [...element.childNodes].some((node) => node.nodeType === 3 && node.textContent.trim()));
  const sizes = [...new Set(texts.map((element) => px(style(element).fontSize)))].sort((a, b) => a - b);
  add("sizes within 12 14 16 20 24 32 44", sizes.filter((size) => ![12, 14, 16, 20, 24, 32, 44].includes(size)).join(" "), "");
  const weights = [...new Set(texts.map((element) => style(element).fontWeight))];
  add("weights within 400 500 600", weights.filter((weight) => !["400", "500", "600"].includes(weight)).join(" "), "");
  const big = texts.filter((element) => parseFloat(style(element).fontSize) >= 33 && !element.closest(".t-hero"));
  add("no other text >= 33 px", big.length, 0);
  const thick = all("main *, dialog *").filter((element) => ["Top", "Right", "Bottom", "Left"].some((side) => parseFloat(style(element)[`border${side}Width`]) >= 2));
  add("no border >= 2 px", thick.length, 0);
  add("no horizontal overflow", document.documentElement.scrollWidth > innerWidth, false);
  return { checks, failed: checks.filter((entry) => !entry.pass) };
})();
