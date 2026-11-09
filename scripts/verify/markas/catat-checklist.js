// In-page Catat and global-surface checklist (apply-markas-catat D3 to D8, D19, D24), from option-6 styles.css
// `.dialog`, `.input-money`, `.quick-chips`, `.seg`, `.field`, `.box`, `.dl`, `.notif`, `.menu-item`, `.toast` and SPEC
// §7, §11 and §13. Evaluated by the Catat capture runs. Every check is { check, value, expected, pass }.
(() => {
  const checks = [];
  const add = (check, value, expected, tolerance = 0) => {
    const pass = typeof expected === "number" ? Math.abs(Number(value) - expected) <= tolerance : value === expected;
    checks.push({ check, value, expected, pass });
  };
  const atMost = (check, value, maximum) => checks.push({ check, value, expected: `≤ ${maximum}`, pass: Number(value) <= maximum + 0.5 });
  const shown = (element) => Boolean(element) && element.getBoundingClientRect().width > 0 && element.getBoundingClientRect().height > 0;
  const all = (selector, scope = document) => [...scope.querySelectorAll(selector)].filter(shown);
  const first = (selector, scope = document) => all(selector, scope)[0] ?? null;
  const style = (element) => getComputedStyle(element);
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
  const neutral = new Set([rgb("--line"), rgb("--line-strong"), "rgba(0, 0, 0, 0)", rgb("--field-focus")]);

  add("no horizontal page overflow", document.documentElement.scrollWidth > innerWidth, false);
  const dialog = all("dialog.shell-dialog[open]").pop() ?? null;

  if (dialog && dialog.classList.contains("capture-dialog")) {
    const header = first(".dlg-h", dialog);
    const body = first(".dlg-b", dialog);
    const footer = first(".dlg-f", dialog);
    add("dialog width (560, the screen below 768)", Math.round(box(dialog).width), phone ? innerWidth : 560, 1);
    if (phone) add("sheet sits on the bottom edge", Math.round(box(dialog).bottom), innerHeight, 1);
    if (!phone) add("header padding 24 24 16", [style(header).paddingTop, style(header).paddingLeft, style(header).paddingBottom].map(px).join(" "), "24 24 16");
    add("body side padding (24, 16 on a phone)", px(style(body).paddingLeft), phone ? 16 : 24);
    if (footer) {
      add("footer top line 1 px", px(style(footer).borderTopWidth), 1);
      if (!phone) add("footer padding 16 24", `${px(style(footer).paddingTop)} ${px(style(footer).paddingLeft)}`, "16 24");
    }
    const sentence = first(".capture-sentence", dialog);
    if (sentence) {
      add("sentence field height 56", Math.round(box(sentence).height), 56);
      add("sentence field 16 px", px(style(sentence).fontSize), 16);
    }
    const money = first(".input-money", dialog);
    if (money) {
      add("money input height 56", Math.round(box(money).height), 56);
      add("money input 20/600", `${px(style(money).fontSize)}/${style(money).fontWeight}`, "20/600");
      add("money input tabular figures", style(money).fontVariantNumeric.includes("tabular-nums"), true);
    }
    for (const chip of all(".quick-chips .ws-btn", dialog).slice(0, 1)) add("chip height (32, 44 on a phone)", Math.round(box(chip).height), phone ? 44 : 32);
    for (const segment of all(".capture-seg [role=radio]", dialog).slice(0, 1)) add("segment item height (30, 40 on a phone)", Math.round(box(segment).height), phone ? 40 : 30, 1);
    const fields = all("select.field-control, input.field-control:not(.input-money):not(.capture-sentence):not([type=file])", dialog);
    if (fields.length) add("fields 40 px", [...new Set(fields.map((field) => Math.round(box(field).height)))].join(","), "40");
    for (const line of all(".field .hint, .field .err", dialog).slice(0, 3)) add("hint and error 12/16", `${px(style(line).fontSize)}/${px(style(line).lineHeight)}`, "12/16");
    const read = first(".capture-read", dialog);
    if (read) {
      add("Terbaca box radius 12", px(style(read).borderTopLeftRadius), 12);
      add("Terbaca box padding (24, 16 on a phone)", px(style(read).paddingTop), phone ? 16 : 24);
      const dl = first(".dl", read);
      if (!phone) add("dl term column 160", px(style(dl).gridTemplateColumns.split(" ")[0]), 160);
    }
    // No border of 2 px or more and no coloured border, except the focused field's edge (SPEC §13).
    const borders = [...dialog.querySelectorAll("*")].filter(shown).filter((element) => element !== document.activeElement);
    const thick = borders.filter((element) => ["Top", "Right", "Bottom", "Left"].some((side) => px(style(element)[`border${side}Width`]) >= 2));
    add("no border of 2 px or more", thick.length, 0);
    const coloured = borders.filter((element) => ["Top", "Bottom"].some((side) => px(style(element)[`border${side}Width`]) > 0 && !neutral.has(style(element)[`border${side}Color`])));
    add("no coloured border", coloured.map((element) => element.className || element.tagName).slice(0, 3).join(" | "), "");
    const textStyles = new Set(borders.filter((element) => [...element.childNodes].some((node) => node.nodeType === 3 && node.textContent.trim()))
      .map((element) => `${px(style(element).fontSize)}/${style(element).fontWeight}`));
    atMost("text styles in the dialog", textStyles.size, 8);
    // Focus on the amount or the sentence: a 1 px --field-focus edge with a 3 px --ring, no outline (D3, D6).
    const focusable = money ?? sentence;
    if (focusable) {
      const before = document.activeElement;
      focusable.focus();
      const focused = style(focusable);
      add("focused field: 1 px edge", px(focused.borderTopWidth), 1);
      add("focused field: --field-focus edge", focused.borderTopColor, rgb("--field-focus"));
      add("focused field: 3 px ring", /0px 0px 0px 3px/.test(focused.boxShadow), true);
      add("focused field: no outline", focused.outlineStyle === "none" || px(focused.outlineWidth) === 0, true);
      if (before instanceof HTMLElement) before.focus();
    }
  }

  const popover = first(".notif-pop, [role=dialog].ws-popover, .ws-menu[role=dialog]");
  const notif = first(".notif");
  if (notif) {
    const panel = notif.closest("[role=dialog], dialog") ?? notif.parentElement;
    if (!phone) add("notifications width 400", Math.round(box(panel).width), 400, 1);
    const dot = first(".notif .unread");
    if (dot) add("unread dot 8 px", `${Math.round(box(dot).width)}×${Math.round(box(dot).height)}`, "8×8");
  }
  const menu = all("[role=menu]").pop();
  if (menu && !phone) {
    const radios = all("[role=menuitemradio]", menu);
    if (radios.length) add("theme menu width 220", Math.round(box(menu).width), 220, 1);
    else if (first("a[href*='profil']", menu)) add("account menu width 240", Math.round(box(menu).width), 240, 1);
  }
  if (dialog && !dialog.classList.contains("capture-dialog")) {
    const rows = all(".more-sheet .row, .list .row", dialog);
    if (rows.length && /Lainnya|More/.test(dialog.getAttribute("aria-label") ?? dialog.textContent.slice(0, 40))) {
      add("Lainnya rows 56", [...new Set(rows.map((row) => Math.round(box(row).height)))].join(","), "56");
      if (!phone) add("Lainnya width 480", Math.round(box(dialog).width), 480, 1);
    }
    if (first(".kbd", dialog) && !phone) add("Bantuan width 560", Math.round(box(dialog).width), 560, 1);
  }
  const toast = first(".ws-toast");
  if (toast) {
    add("toast min height 48", Math.round(box(toast).height) >= 48, true);
    add("toast radius 12", px(style(toast).borderTopLeftRadius), 12);
  }
  void popover;
  probe.remove();
  return { checks };
})()
