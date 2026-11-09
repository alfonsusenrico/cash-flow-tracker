// In-page Laporan checklist (apply-markas-laporan D23), from D1 to D16 of the change's design.md and option-6
// SPEC §2, §6 to §9 and §13. Evaluated by capture.mjs for scenarios marked `laporan`. Every check is
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
  const dialog = first("dialog.shell-dialog[open]");
  const colour = (value) => {
    const probe = document.createElement("span");
    probe.style.color = value;
    document.body.appendChild(probe);
    const result = style(probe).color;
    probe.remove();
    return result;
  };

  const cols = first(".lp-cols");
  if (cols && !dialog) {
    const columns = style(cols).gridTemplateColumns.split(" ").map((value) => Math.round(parseFloat(value)));
    const article = first(".lp-article", cols);
    if (wide) {
      add(".cols panel column 352", columns[1], 352);
      add(".cols gap 24", px(style(cols).columnGap), 24);
      if (innerWidth === 1440) add("article 728 at 1440", Math.round(box(article).width), 728, 1);
    } else {
      add(".cols is one column below 1200 px", columns.length, 1);
      if (article) atMost("article at most 880", Math.round(box(article).width), 880);
    }
    if (innerWidth === 1440) add("first content at y = 192", Math.round(box(article).top + scrollY), 192, 1);
    const panel = first(".panel", cols);
    if (panel) {
      add("panel padding", px(style(panel).paddingTop), phone ? 16 : 24);
      add("panel radius", px(style(panel).borderTopLeftRadius), 12);
      add("panel after the article", box(panel).left >= box(article).right - 1 || box(panel).top >= box(article).bottom - 1, true);
      const current = first(".lp-past-row[aria-current='page']", panel);
      if (current) {
        const bar = style(current, "::before");
        add("selection bar 2 px", px(bar.width), 2);
        add("selection bar full height", Math.round(parseFloat(bar.height)), Math.round(box(current).height), 1);
        add("selection bar left at x 0", px(bar.left), 0);
      }
      add("data quality line without a tinted fill", style(panel).backgroundColor, colour("var(--surface)"));
    }
  }

  if (!dialog && first(".lp-article")) {
    add("one 44 px figure", all(".t-hero").length, 1);
    const figure = first(".t-hero");
    if (figure && !first(".skel", figure)) add("hero figure 44/52 600 (32/40 below 360 px)", font(figure), innerWidth < 360 ? "32/40 600" : "44/52 600");
    const hero = first(".lp-article > .hero");
    if (hero) add("hero 48 px above question 1", px(style(hero).marginBottom), phone ? 32 : 48);
    const questions = all(".q:not(.lp-skel-q)");
    if (questions.length) {
      add("four questions", questions.length, 4);
      const columns = style(questions[0]).gridTemplateColumns.split(" ").map((value) => Math.round(parseFloat(value)));
      if (!phone) add("question number column 32", columns[0], 32);
      else add("question one column on the phone", columns.length, 1);
      add("question gap", px(style(questions[0]).columnGap), phone ? 8 : 16);
      if (questions[1]) {
        add("question spacing", `${px(style(questions[1]).marginTop)} ${px(style(questions[1]).paddingTop)}`, phone ? "32 32" : "48 48");
        add("question rule 1 px", px(style(questions[1]).borderTopWidth), 1);
      }
      add("question number 20/32", font(first(".q-num")), "20/32 600");
      add("question heading 24/32", font(first(".q-h")), "24/32 600");
      add("question headings are h2", all(".q-h").every((heading) => heading.tagName === "H2"), true);
    }
    const fig = first(".q-body .t-fig");
    if (fig) add("question figure 20/28", font(fig), "20/28 600");
    const table = first(".lp-tbl");
    if (table) {
      for (const row of all("tbody tr", table).slice(0, 4)) add("table row 48", Math.round(box(row).height), 48, 1);
      const head = first("thead th", table);
      add("table header padding 8", px(style(head).paddingTop), 8);
      const foot = first("tfoot td", table);
      if (foot) add("total rule line-strong", style(foot).borderTopColor, colour("var(--line-strong)"));
      add("share and change columns hidden on the phone", all("thead th", table).length, phone ? 2 : all("thead th", table).length);
      const link = first(".lp-rowlink", table);
      if (link) add("row link stretched over the row", Math.round(parseFloat(style(link, "::after").width) || box(link.closest("tr")).width) >= Math.round(box(link.closest("tr")).width) - 1, true);
    }
    for (const row of all(".lp-article .list .row").slice(0, 6)) atLeast("row min-height", Math.round(box(row).height), phone ? 64 : 56);
    const meter = first(".lp-budget-row .prog");
    if (meter) add("budget meter 160 × 4", `${Math.round(box(meter).width)} × ${Math.round(box(meter).height)}`, "160 × 4");
    const area = first(".bars-v .b-area");
    if (area) {
      add("bar area 112", Math.round(box(area).height), 112);
      const bars = all(".bars-v .bar").length ? [...document.querySelectorAll(".bars-v .bar")] : [];
      const heights = bars.map((bar) => box(bar).height);
      const max = Math.max(...heights);
      add("tallest bar fills the area", Math.round(max), 112, 1);
      const bases = new Set(bars.map((bar) => Math.round(box(bar).bottom)));
      add("bars share one base", bases.size, 1);
    }
    const summary = first(".lp-details > summary");
    if (summary) atLeast("disclosure summary 56", Math.round(box(summary).height), 56);
  }

  const stepper = first(".stepper");
  if (stepper) {
    const button = first(".ws-icon-btn", stepper);
    if (button) add("stepper button", Math.round(box(button).width), phone ? 44 : 36);
    const label = first(".cur", stepper);
    if (label && !phone) atLeast("stepper label min-width", Math.round(box(label).width), 168);
  }
  if (dialog) {
    atMost("dialog within the viewport width", Math.round(box(dialog).width), innerWidth);
    if (!phone) add("classification dialog 560", Math.round(box(dialog).width), dialog.querySelector(".lp-classify") ? 560 : Math.round(box(dialog).width), 1);
  }

  // SPEC §13: sizes, weights, at most eight text styles, no dotted underline, no other text of 33 px or more, no
  // border of 2 px or more, no horizontal overflow.
  const texts = all("main *, dialog *").filter((element) => [...element.childNodes].some((node) => node.nodeType === 3 && node.textContent.trim()));
  const sizes = [...new Set(texts.map((element) => px(style(element).fontSize)))].sort((a, b) => a - b);
  add("sizes within 12 14 16 20 24 32 44", sizes.filter((size) => ![12, 14, 16, 20, 24, 32, 44].includes(size)).join(" "), "");
  const weights = [...new Set(texts.map((element) => style(element).fontWeight))];
  add("weights within 400 500 600", weights.filter((weight) => !["400", "500", "600"].includes(weight)).join(" "), "");
  const styles = new Set(texts.filter((element) => element.closest("main")).map((element) => `${px(style(element).fontSize)}/${style(element).fontWeight}/${style(element).textTransform}`));
  if (!dialog) checks.push({ check: "text styles on the page", value: [...styles].join(" | "), expected: "<= 8", pass: styles.size <= 8 });
  const big = texts.filter((element) => parseFloat(style(element).fontSize) >= 33 && !element.closest(".t-hero"));
  add("no other text >= 33 px", big.length, 0);
  const dotted = all("main *").filter((element) => style(element).textDecorationStyle === "dotted" && style(element).textDecorationLine.includes("underline"));
  add("no dotted underline", dotted.length, 0);
  const thick = all("main *, dialog *").filter((element) => ["Top", "Right", "Bottom", "Left"].some((side) => parseFloat(style(element)[`border${side}Width`]) >= 2));
  add("no border >= 2 px", thick.length, 0);
  add("no KPI card or icon tile", all("main .ws-kpis, main .ws-tile").length, 0);
  add("no horizontal overflow", document.documentElement.scrollWidth > innerWidth, false);
  return { checks, failed: checks.filter((entry) => !entry.pass) };
})();
