// In-page Rekening checklist (apply-markas-rekening D22), from D1 to D14 of the change's design.md and option-6
// SPEC §2, §5 to §8 and §13. Evaluated by capture.mjs for scenarios marked `rekening`. Every check is
// { check, value, expected, pass }.
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

  const dialog = first("dialog.shell-dialog[open]");
  const hero = first(".hero");
  const scope = dialog ?? document.querySelector("main") ?? document.body;

  add("no horizontal page overflow", document.documentElement.scrollWidth > innerWidth, false);
  if (hero && !dialog) {
    if (innerWidth === 1440) add("hero starts at y = 192", Math.round(box(hero).top + scrollY), 192, 1);
    add("one 44 px figure", all(".t-hero").length, 1);
    const big = all("main *").filter((element) => element.childElementCount === 0 && px(style(element).fontSize) >= 33 && !element.closest(".t-hero"));
    add("no other text of 33 px or more", big.length, 0);
    const split = first(".hero .split");
    if (split) {
      add("split bar 8 px", px(style(split).height), 8);
      add("split bar gaps 2 px", px(style(split).columnGap), 2);
    }
    add("one Rincian control", all("main button").filter((button) => /^(Rincian|Breakdown)$/.test(button.textContent.trim())).length, 1);
  }
  const toolbar = first(".rk-toolbar");
  if (toolbar && !dialog) add("toolbar margins 32/24", `${px(style(toolbar).marginTop)}/${px(style(toolbar).marginBottom)}`, "32/24");
  const groups = all("main section.grp");
  if (groups.length && !dialog) {
    const grp = groups[0];
    add("group padding-left 16", px(style(grp).paddingLeft), 16);
    add("group rail 2 px, square", `${px(style(grp, "::before").width)} ${style(grp, "::before").borderRadius}`, "2 0px");
    if (groups[1]) add("groups 48 apart", Math.round(box(groups[1]).top - box(groups[0]).bottom), 48, 1);
    add("group header min-height 32", px(style(first(".grp-h", grp)).minHeight), 32);
  }
  const tiles = all("main .tiles .tile");
  if (tiles.length && !dialog) {
    const tile = tiles[0];
    add("tile min-height 112", px(style(tile).minHeight), 112);
    add("tile padding 16", px(style(tile).paddingTop), 16);
    add("tile gap 12", px(style(tile).rowGap), 12);
    add("tile radius 12", px(style(tile).borderTopLeftRadius), 12);
    add("tile border 1 px", px(style(tile).borderTopWidth), 1);
    const row = all(".tile", tile.closest(".tiles")).filter((other) => Math.abs(box(other).top - box(tile).top) < 2).length;
    const list = tile.closest(".tiles");
    const columns = style(list).gridTemplateColumns.split(" ").length;
    if (innerWidth === 1440) add("four tile columns at 1440", columns, 4);
    if (innerWidth === 1024) add("three tile columns at 1024", columns, 3);
    if (phone) add("one tile column on a phone", columns, 1);
    checks.push({ check: "tiles in the first row", value: row, expected: "info", pass: true });
    const inst = first(".inst", tile);
    if (inst) add("institution mark 24 × 24 radius 6", `${px(box(inst).width)}×${px(box(inst).height)} ${px(style(inst).borderTopLeftRadius)}`, "24×24 6");
    const amount = first(".tl-amt", tile);
    if (amount) add("tile balance 20/600", `${px(style(amount).fontSize)} ${style(amount).fontWeight}`, "20 600");
  }
  const gain = first("main .gain");
  if (gain && !dialog) add("gain 12/500", `${px(style(gain).fontSize)} ${style(gain).fontWeight}`, "12 500");
  const kind = first("main .rk-kinds li");
  if (kind && !dialog) add("per-kind line min-height 40", box(kind).height >= 40, true);
  const hbar = first("main .hbar");
  if (hbar && !dialog) {
    add("bar row min-height 48", px(style(hbar).minHeight), 48);
    if (!phone) {
      const cols = style(hbar).gridTemplateColumns.split(" ").map(px);
      add("bar columns 200/1fr/136/176 (160/1fr/120/168 below 1024)", `${cols[0]} ${cols[2]} ${cols[3]}`, innerWidth >= 1024 ? "200 136 176" : "160 120 168");
    }
    const bar = first(".bar", hbar);
    if (bar && !phone) add("bar 8 px", px(style(bar).height), 8);
  }
  const legendRow = first(".rk-legend .ws-select-row");
  if (legendRow && !dialog) add("legend rows 48", Math.round(box(legendRow).height) >= 48, true);
  const walletTile = first("main .wallet .tile");
  if (walletTile && !dialog) add("wallet tile 280 × ≥152", `${Math.round(box(walletTile).width)} ${box(walletTile).height >= 152}`, "280 true");
  const selBar = first(".rk-sel-bar");
  if (selBar && !dialog) add("reorder bar min-height 48", box(selBar).height >= 48, true);
  if (dialog) {
    const width = Math.round(box(dialog).width);
    if (!phone) checks.push({ check: "dialog width 480/520/560", value: width, expected: [480, 520, 560], pass: [480, 520, 560].includes(width) });
    if (!phone) add("dialog radius 16", px(style(dialog).borderTopLeftRadius), 16);
    if (phone) add("sheet spans the screen", width >= innerWidth - 1, true);
    const header = first(".dlg-h", dialog);
    if (header) add("dialog header padding 24 24 16 (sheet 16 16 12)", `${px(style(header).paddingTop)} ${px(style(header).paddingLeft)} ${px(style(header).paddingBottom)}`, phone ? "16 16 12" : "24 24 16");
    const footer = first(".dlg-f", dialog);
    if (footer) add("dialog footer padding 16 24 (sheet 12 16)", `${px(style(footer).paddingTop)} ${px(style(footer).paddingLeft)}`, phone ? "12 16" : "16 24");
    const dl = first(".dl", dialog);
    if (dl && !phone) add(".dl label column 160", px(style(dl).gridTemplateColumns.split(" ")[0]), 160);
    const figures = all("*", dialog).filter((element) => element.childElementCount === 0 && px(style(element).fontSize) >= 33);
    add("no dialog text of 33 px or more", figures.length, 0);
  }
  // Type scale and borders over the visible text and boxes of the page or dialog.
  const texts = all("*", scope).filter((element) => [...element.childNodes].some((node) => node.nodeType === 3 && node.textContent.trim()));
  const sizes = [...new Set(texts.map((element) => px(style(element).fontSize)))].sort((a, b) => a - b);
  const weights = [...new Set(texts.map((element) => style(element).fontWeight))].sort();
  within("font sizes", sizes, [12, 14, 16, 20, 24, 32, 44]);
  within("font weights", weights, ["400", "500", "600"]);
  const thick = all("*", scope).filter((element) => {
    const s = style(element);
    return ["Top", "Right", "Bottom", "Left"].some((side) => px(s[`border${side}Width`]) >= 2 && s[`border${side}Style`] !== "none");
  }).map((element) => element.tagName.toLowerCase() + "." + [...element.classList].slice(0, 2).join("."));
  add("no border of 2 px or more", thick.length, 0);
  return { pass: checks.filter((item) => item.pass).length, fail: checks.filter((item) => !item.pass).length, checks };
})()
