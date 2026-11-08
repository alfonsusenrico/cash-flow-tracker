// In-page Tinjau checklist (apply-markas-tinjau D22), generated from D1, D5 to D8, D13 to D15 of the change's design.md
// and option-6 SPEC §2, §5, §7 and §13. It runs unchanged on the prototype (`#/tinjau`) and on the app, which uses the
// prototype's class names; the phone sheet is `.dialog.is-full` there and `dialog.shell-dialog.is-full` here.
// Evaluated by capture.mjs for scenarios marked `tinjau`. Every check is { check, value, expected, pass }.
(() => {
  const checks = [];
  const add = (check, value, expected, tolerance = 0) => {
    const pass = typeof expected === "number" ? Math.abs(Number(value) - expected) <= tolerance : value === expected;
    checks.push({ check, value, expected, pass });
  };
  const atLeast = (check, value, minimum) => checks.push({ check, value, expected: `>= ${minimum}`, pass: Number(value) >= minimum });
  const shown = (element) => Boolean(element) && element.getBoundingClientRect().width > 0 && element.getBoundingClientRect().height > 0;
  const all = (selector, scope = document) => [...scope.querySelectorAll(selector)].filter(shown);
  const first = (selectors, scope = document) => {
    for (const selector of selectors.split("|")) {
      const found = all(selector, scope)[0];
      if (found) return found;
    }
    return null;
  };
  const style = (element, pseudo) => getComputedStyle(element, pseudo);
  const box = (element) => element.getBoundingClientRect();
  const px = (value) => Math.round(parseFloat(value) * 100) / 100;
  const font = (element) => `${px(style(element).fontSize)}/${px(style(element).lineHeight)} ${style(element).fontWeight}`;
  const phone = innerWidth < 768;
  const wide = innerWidth >= 1200;

  const sheet = first("dialog.shell-dialog.is-full[open]|.dialog.is-full");
  const otherDialog = first("dialog.shell-dialog[open]:not(.is-full)|.layer .dialog:not(.is-full)");
  if (sheet) {
    add("sheet height = viewport", Math.round(box(sheet).height), innerHeight, 1);
    add("sheet radius", px(style(sheet).borderTopLeftRadius), 0);
  }
  if (otherDialog) {
    const width = Math.round(box(otherDialog).width);
    if (!phone) atLeast("dialog centred: left gap equals right gap", -Math.abs(box(otherDialog).left - (innerWidth - box(otherDialog).right)), -1);
    else add("dialog is a bottom sheet at full width", width, innerWidth, 1);
  }

  const region = first(".tj");
  const hero = first(".hero");
  if (hero && innerWidth === 1440 && !sheet && !otherDialog) add("hero starts at y = 192", Math.round(box(hero).top + scrollY), 192, 1);
  const heroFigures = all(".t-hero");
  if (hero) add("one 44 px figure", heroFigures.length, 1);
  if (heroFigures[0]) add("hero figure 44/52 600 (32/40 below 360 px)", font(heroFigures[0]), innerWidth < 360 ? "32/40 600" : "44/52 600");

  if (region && !otherDialog) {
    const rail = first(".tj .rail|.tj .tj-folders");
    const list = first(".tj .tj-list");
    const detail = first(".tj .tj-detail");
    const columns = style(region).gridTemplateColumns.split(" ").map((value) => Math.round(parseFloat(value)));
    if (wide) {
      add(".tj column 1", columns[0], 232);
      add(".tj column 2", columns[1], 336);
      if (innerWidth >= 1440) atLeast(".tj column 3", columns[2], 488);
      add(".tj gap", px(style(region).columnGap), 24);
      if (rail && list && detail) {
        add("rail and list share their top edge", Math.round(box(rail).top), Math.round(box(list).top), 1);
        add("list and item share their top edge", Math.round(box(list).top), Math.round(box(detail).top), 1);
      }
      if (list) add("list's right line", `${style(list).borderRightWidth} ${style(list).borderRightStyle}`, "1px solid");
    } else if (!phone) {
      add(".tj column 1", columns[0], 184);
      add(".tj gap", px(style(region).columnGap), 24);
      if (detail) add("item spans the full width below", Math.round(box(detail).width), Math.min(880, Math.round(box(region).width)), 1);
      if (rail && list) add("rail and list share their top edge", Math.round(box(rail).top), Math.round(box(list).top), 1);
    } else {
      add(".tj is one column", style(region).display, "block");
    }

    const items = all(".rail-item", rail ?? region);
    const current = items.find((item) => item.getAttribute("aria-current") === "true");
    if (items[0] && !phone) {
      add("rail item height", Math.round(box(items[0]).height), 36);
      add("rail item padding", `${px(style(items[0]).paddingLeft)} ${px(style(items[0]).paddingRight)}`, "12 12");
      add("rail item radius", px(style(items[0]).borderTopLeftRadius), 6);
      add("rail item 14/500", `${px(style(items[0]).fontSize)} ${style(items[0]).fontWeight}`, "14 500");
      const count = first(".count", items[0]);
      if (count) add("rail count 12/400", `${px(style(count).fontSize)} ${style(count).fontWeight}`, "12 400");
      const rule = first(".rail-sep", rail ?? region);
      if (rule) add("rail rule 1 px", Math.round(box(rule).height), 1);
    }
    if (current && !phone) {
      const bar = style(current, "::before");
      add("current bar 2 × 20", `${px(bar.width)} × ${px(bar.height)}`, "2 × 20");
      add("current bar left", px(bar.left), -8);
      add("current bar top", Math.round(box(current).height / 2 - parseFloat(bar.height) / 2), 8);
    }
    if (items[0] && phone) {
      add("chip height", Math.round(box(items[0]).height), 44);
      add("chip radius", px(style(items[0]).borderTopLeftRadius), 6);
      add("chip line", style(items[0]).borderTopWidth, "1px");
      if (current) add("current chip has no bar", style(current, "::before").content, "none");
      const line = current?.parentElement;
      if (line) add("chip line scrolls inside itself", style(line).overflowX, "auto");
    }

    const rows = all(".tj-list .row");
    if (rows[0]) {
      atLeast("row height", Math.round(box(rows[0]).height), 64);
      add("row padding", `${px(style(rows[0]).paddingTop)} ${px(style(rows[0]).paddingLeft)}`, "12 12");
      const icon = first(".r-ico", rows[0]);
      if (icon) add("row icon column", Math.round(box(icon).width), 40);
    }
  }

  const detailRoot = sheet ?? (otherDialog ? null : first(".tj-detail"));
  if (detailRoot) {
    const question = first(".tj-q", detailRoot);
    if (question) add("question 16/24 400", font(question), "16/24 400");
    const answer = first(".answer", detailRoot);
    if (answer) {
      atLeast("answer height", Math.round(box(answer).height), 56);
      add("answer padding", `${px(style(answer).paddingTop)} ${px(style(answer).paddingLeft)}`, "8 12");
      const cap = first(".kbd|.ws-kbd", answer);
      if (cap) add("key cap height", Math.round(box(cap).height), 20);
    }
    const raw = first(".raw", detailRoot);
    if (raw) {
      add("raw box padding", `${px(style(raw).paddingTop)} ${px(style(raw).paddingLeft)}`, "12 16");
      add("raw box radius", px(style(raw).borderTopLeftRadius), 6);
    }
  }

  // SPEC §13: no other text 33 px or larger, no border of 2 px or more, no horizontal overflow.
  const big = all("body *").filter((element) => element.childNodes.length && [...element.childNodes].some((node) => node.nodeType === 3 && node.textContent.trim())
    && parseFloat(style(element).fontSize) >= 33 && !element.classList.contains("t-hero"));
  add("no other text >= 33 px", big.length, 0);
  const thick = all("main *, dialog *").filter((element) => ["Top", "Right", "Bottom", "Left"].some((side) => parseFloat(style(element)[`border${side}Width`]) >= 2));
  add("no border >= 2 px", thick.length, 0);
  add("no horizontal overflow", document.documentElement.scrollWidth > innerWidth, false);
  return { checks, failed: checks.filter((entry) => !entry.pass) };
})();
