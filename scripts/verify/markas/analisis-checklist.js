// In-page Analisis checklist (apply-markas-analisis D26), from D1 to D16 and D22 of the change's design.md and option-6
// SPEC §2, §6 to §9 and §13. Evaluated by capture.mjs for scenarios marked `analisis`. Every check is
// { check, value, expected, pass }.
(() => {
  const checks = [];
  const add = (check, value, expected, tolerance = 0) => {
    const pass = typeof expected === "number" ? Math.abs(Number(value) - expected) <= tolerance : value === expected;
    checks.push({ check, value, expected, pass });
  };
  const atLeast = (check, value, minimum) => checks.push({ check, value, expected: `>= ${minimum}`, pass: Number(value) >= minimum });
  const atMost = (check, value, maximum) => checks.push({ check, value, expected: `<= ${maximum}`, pass: Number(value) <= maximum });
  const shown = (element) => Boolean(element) && element.getBoundingClientRect().width > 0 && element.getBoundingClientRect().height > 0;
  const all = (selector, scope = document) => [...scope.querySelectorAll(selector)].filter(shown);
  const first = (selector, scope = document) => all(selector, scope)[0] ?? null;
  const style = (element, pseudo) => getComputedStyle(element, pseudo);
  const box = (element) => element.getBoundingClientRect();
  const px = (value) => Math.round(parseFloat(value) * 100) / 100;
  const font = (element) => `${px(style(element).fontSize)}/${px(style(element).lineHeight)} ${style(element).fontWeight}`;
  const phone = innerWidth < 768;
  const wide = innerWidth >= 1200;
  const probe = document.createElement("span");
  probe.style.color = "var(--out-fg)";
  document.body.appendChild(probe);
  const expenseInk = style(probe).color;
  probe.remove();
  const dialog = first("dialog.shell-dialog[open]");

  const tabs = first(".tabs");
  if (tabs && !dialog) {
    const tab = first(".tab", tabs);
    add("tab height", Math.round(box(tab).height), phone ? 44 : 40);
    const selected = first(".tab[aria-selected='true']", tabs);
    if (selected) {
      const bar = style(selected, "::after");
      add("selected tab bar 2 px", px(bar.height), 2);
      add("selected tab bar is the full tab width", Math.round(parseFloat(bar.width)), Math.round(box(selected).width), 1);
    }
    add("tabs margin-bottom", px(style(tabs).marginBottom), 32);
    const link = first(".tabs .btn-link");
    add("report link beside the tabs from 768 px", Boolean(link), !phone);
    if (innerWidth === 1440) add("tabs start at y = 192", Math.round(box(tabs).top + scrollY), 192, 1);
  }

  const cols = first(".an-cols");
  if (cols && !dialog) {
    const columns = style(cols).gridTemplateColumns.split(" ").map((value) => Math.round(parseFloat(value)));
    if (wide) {
      add(".cols panel column", columns[1], 352);
      add(".cols gap", px(style(cols).columnGap), 24);
      if (innerWidth === 1440) add(".cols reading column", columns[0], 728, 1);
    } else add(".cols is one column below 1200 px", columns.length, 1);
    const panel = first(".panel", cols);
    if (panel) {
      add("panel padding", px(style(panel).paddingTop), phone ? 16 : 24);
      add("panel radius", px(style(panel).borderTopLeftRadius), 12);
    }
  }

  if (!dialog) {
    add("one 44 px figure", all(".t-hero").length, all(".hero").length ? 1 : 0);
    const figure = first(".t-hero");
    if (figure && !first(".is-words, .skel", figure)) add("hero figure 44/52 600 (32/40 below 360 px)", font(figure), innerWidth < 360 ? "32/40 600" : "44/52 600");
    const rows = all(".cmp-row");
    for (const row of rows.slice(0, 4)) atLeast("comparison row min-height", Math.round(box(row).height), 48);
    const desk = rows.find((row) => !row.classList.contains("is-compare"));
    if (desk) {
      const columns = style(desk).gridTemplateColumns.split(" ").map((value) => Math.round(parseFloat(value)));
      if (!phone) add("Kakeibo row columns 120 72 120", columns.slice(1).join(" "), "120 72 120");
      else add("Kakeibo row phone columns end in 40", columns[columns.length - 1], 40);
    }
    const dot = first(".kk-name .dot");
    if (dot) add("pillar dot 8 × 8 square", `${Math.round(box(dot).width)} × ${Math.round(box(dot).height)} ${px(style(dot).borderTopLeftRadius)}`, "8 × 8 0");
    const bars = all(".chart svg rect[data-bar]");
    const chart = first(".chart svg");
    if (chart && bars.length) {
      const width = Number(chart.getAttribute("width"));
      const expected = Math.max(4, Math.floor((width - 8) / bars.length) - 4);
      add("daily bar width max(4, floor((W − 8)/n) − 4)", Math.round(Number(bars[0].getAttribute("width"))), expected);
      add("chart drawn at the column width", width, Math.round(box(chart.parentElement).width), 1);
      const labels = all(".chart .ax");
      add("chart labels 12 px", [...new Set(labels.map((label) => px(style(label).fontSize)))].join(" "), "12");
      const boxes = labels.filter((label) => label.getAttribute("y") === String(200 - 8)).map((label) => label.getBoundingClientRect());
      const overlapping = boxes.filter((one, index) => boxes.some((other, otherIndex) => otherIndex !== index && one.left < other.right && other.left < one.right));
      add("no overlapping date labels", overlapping.length, 0);
      atMost("chart inside its column", Math.round(box(chart).right), Math.round(box(chart.parentElement).right) + 1);
    }
    const group = first(".grp");
    if (group) {
      add("group padding-left", px(style(group).paddingLeft), 16);
      add("group rail 2 px", px(style(group, "::before").width), 2);
    }
    const meter = first(".cat-row .prog");
    if (meter) add("category meter 160 × 4", `${Math.round(box(meter).width)} × ${Math.round(box(meter).height)}`, "160 × 4");
    const catRow = first(".cat-row");
    if (catRow) atLeast("category row min-height", Math.round(box(catRow).height), phone ? 64 : 56);
    const cell = first(".cal-cell[role=gridcell]");
    if (cell) {
      atLeast("calendar cell min-height", Math.round(box(cell).height), phone ? 56 : 80);
      const mark = first(".cal-cell .mark");
      if (mark) add("marker 6 × 6", `${Math.round(box(mark).width)} × ${Math.round(box(mark).height)}`, "6 × 6");
      const heat = first(".cal-cell .heat");
      if (heat) add("heat strip 4 px", Math.round(box(heat).height), 4);
      const selected = first(".cal-cell[aria-selected='true']");
      if (selected) add("selected day bar 2 px", px(style(selected, "::before").height), 2);
      const clipped = all(".cal-cell .cal-amt").filter((amount) => amount.scrollWidth > amount.clientWidth + 1);
      add("every cell amount fits its cell", clipped.length, 0);
    }
    const flow = first(".flow svg");
    if (flow) {
      const frame = box(flow);
      const outside = all(".flow-label").filter((label) => { const r = label.getBoundingClientRect(); return r.right > frame.right + 0.5 || r.left < frame.left - 0.5; });
      add("every flow label inside the frame", outside.length, 0);
      const bar = first(".flow .flow-bar");
      if (bar) add("flow node width 8", Math.round(box(bar).width), 8);
      const opacities = [...new Set(all(".flow [data-band] > path:first-child").map((path) => path.getAttribute("fill-opacity")))].sort().join(" ");
      add("band opacities within 0.06 0.3", opacities.split(" ").filter((value) => !["0.06", "0.3"].includes(value)).join(" "), "");
      atLeast("map at least 720 px", Math.round(frame.width), 720);
    }
  }

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
  }

  const stepper = first(".stepper");
  if (stepper) {
    const button = first(".ws-icon-btn", stepper);
    if (button) add("stepper button", Math.round(box(button).width), phone ? 44 : 36);
    const label = first(".cur", stepper);
    if (label && !phone) atLeast("stepper label min-width", Math.round(box(label).width), 168);
  }
  for (const segment of all(".ws-seg button").slice(0, 3)) if (phone) atLeast("phone segment height", Math.round(box(segment).height), 40);

  // SPEC §13: sizes, weights, no dotted underline, no other text of 33 px or more, no border of 2 px or more, no overflow.
  const texts = all("main *, dialog *").filter((element) => [...element.childNodes].some((node) => node.nodeType === 3 && node.textContent.trim()));
  const sizes = [...new Set(texts.map((element) => px(style(element).fontSize)))].sort((a, b) => a - b);
  add("sizes within 12 14 16 20 24 32 44", sizes.filter((size) => ![12, 14, 16, 20, 24, 32, 44].includes(size)).join(" "), "");
  const weights = [...new Set(texts.map((element) => style(element).fontWeight))];
  add("weights within 400 500 600", weights.filter((weight) => !["400", "500", "600"].includes(weight)).join(" "), "");
  const styles = new Set(texts.filter((element) => element.closest("main")).map((element) => `${px(style(element).fontSize)}/${style(element).fontWeight}/${style(element).textTransform}`));
  if (!dialog) atMost("text styles per tab", styles.size, 8);
  const big = texts.filter((element) => parseFloat(style(element).fontSize) >= 33 && !element.closest(".t-hero"));
  add("no other text >= 33 px", big.length, 0);
  const dotted = all("main *").filter((element) => style(element).textDecorationStyle === "dotted" && style(element).textDecorationLine.includes("underline"));
  add("no dotted underline", dotted.length, 0);
  const thick = all("main *, dialog *").filter((element) => ["Top", "Right", "Bottom", "Left"].some((side) => parseFloat(style(element)[`border${side}Width`]) >= 2));
  add("no border >= 2 px", thick.length, 0);
  add("Rincian controls on the page", all("main button").filter((button) => /^(Rincian|Breakdown)$/.test(button.textContent.trim())).length <= 1, true);
  add("no horizontal overflow", document.documentElement.scrollWidth > innerWidth, false);
  return { checks, failed: checks.filter((entry) => !entry.pass) };
})();
