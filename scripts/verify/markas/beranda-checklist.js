// In-page Beranda checklist (apply-markas-beranda D14), generated from the D1 and D3 to D9 tables of the change's
// design.md and option-6 SPEC §2, §5 and §7. It runs unchanged on the prototype (option-6 `#/beranda`) and on the
// app, because the app's Beranda uses the prototype's class names; the Rincian dialog is `.dialog` in the
// prototype and `dialog.shell-dialog` in the app. Evaluated by capture.mjs for scenarios marked `beranda`.
// Every check is { check, value, expected, pass }; positions that depend on text metrics allow 1 px.
(() => {
  const PALETTE = {
    light: {
      s0: "#F8F7F4", s1: "#F1EFEB", s2: "#E6E3DD", line: "#E1DDD6", lineStrong: "#847F78", ink: "#1C1E22", muted: "#5B5E65",
      accentText: "#B8142D", dialog: "#F8F7F4", mIncome: "#047857", mExpense: "#E11D48", mInvest: "#7C3AED", mWarning: "#B45309",
      income: "#056548", transfer: "#075985", invest: "#5B21B6", warning: "#8F4F04",
    },
    dark: {
      s0: "#151517", s1: "#1C1D20", s2: "#26272B", line: "#2F3034", lineStrong: "#6E7076", ink: "#E8E7E4", muted: "#A3A3A7",
      accentText: "#FF6B78", dialog: "#1C1D20", mIncome: "#10B981", mExpense: "#FF4757", mInvest: "#8B5CF6", mWarning: "#F59E0B",
      income: "#34D399", transfer: "#38BDF8", invest: "#B39DFB", warning: "#FBBF24",
    },
  };
  const hexToRgb = (hex) => {
    const value = parseInt(hex.slice(1), 16);
    return `rgb(${(value >> 16) & 255}, ${(value >> 8) & 255}, ${value & 255})`;
  };
  const theme = getComputedStyle(document.body).backgroundColor === hexToRgb(PALETTE.dark.s0) ? "dark" : "light";
  const colour = (name) => hexToRgb(PALETTE[theme][name]);
  const checks = [];
  const add = (check, value, expected, tolerance = 0) => {
    const pass = typeof expected === "number" ? Math.abs(Number(value) - expected) <= tolerance : value === expected;
    checks.push({ check, value, expected, pass });
  };
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
  const font = (element) => {
    const computed = style(element);
    const family = computed.fontFamily.split(",")[0].replace(/["']/g, "").trim().toLowerCase().startsWith("manrope") ? "Manrope" : "Inter";
    return `${family} ${px(computed.fontSize)}/${px(computed.lineHeight)} ${computed.fontWeight}`;
  };
  const edge = (element, side) => `${style(element)[`border${side}Width`]} ${style(element)[`border${side}Color`]}`;
  const phone = innerWidth < 768;
  const twoColumns = innerWidth >= 1200;

  const dialog = first("dialog.shell-dialog[open]|.layer .dialog");
  if (!dialog) {
    // D1: reading column and context panel.
    const cols = first(".cols");
    if (cols) {
      const stack = first(".cols > .stack");
      const panel = first(".cols > .panel");
      add("page columns", twoColumns ? 2 : 1, style(cols).gridTemplateColumns.trim().split(/\s+/).length);
      add("column gap", px(style(cols).columnGap), 24);
      if (stack && panel) {
        if (twoColumns) {
          add("panel width", px(box(panel).width), 352);
          add("panel beside the reading column", px(box(panel).left - box(stack).right), 24, 1);
        } else {
          add("panel follows the reading column", box(panel).top > box(stack).bottom, true);
          add("panel full width", px(box(panel).width), px(box(stack).width), 1);
        }
        const sections = [...stack.children].filter(shown);
        add("reading column sections", sections.length, 3);
        const gaps = sections.slice(1).map((section, index) => px(box(section).top - box(sections[index]).bottom));
        add("section gap", gaps.every((gap) => Math.abs(gap - (phone ? 32 : 48)) <= 1), true);
      }
    }

    // D3: hero.
    const hero = first(".hero");
    if (hero) {
      const top = first(".hero-top", hero);
      add("hero label row min-height", px(style(top).minHeight), 32);
      add("hero label row height", px(box(top).height), 32, 1);
      add("hero label row gap", px(style(top).columnGap), 8);
      const label = first(".hero-top .t-label", hero);
      add("hero label type", `${font(label)} ${style(label).textTransform} ${style(label).letterSpacing}`, "Inter 12/16 500 uppercase 0.72px");
      add("hero label colour", style(label).color, colour("muted"));
      const mark = first(".hero-top .sw-bar", hero);
      if (mark) {
        add("hero mark size", `${px(box(mark).width)}x${px(box(mark).height)}`, "16x2");
        add("hero mark colour", style(mark).backgroundColor, colour("mIncome"));
        add("hero mark above the label", px(box(label).top - box(mark).bottom), 3, 1);
      }
      const rincian = all("button").filter((button) => button.textContent.trim() === "Rincian");
      add("Rincian controls on the page", rincian.length, 1);
      if (rincian[0]) {
        const button = rincian[0];
        add("Rincian type", font(button), "Inter 14/20 500");
        add("Rincian colour", style(button).color, colour("muted"));
        add("Rincian radius", px(style(button).borderTopLeftRadius), 6);
        add("Rincian padding", `${style(button).paddingLeft} ${style(button).paddingRight}`, "8px 8px");
        if (!phone) add("Rincian height", px(box(button).height), 28);
        add("Rincian target ≥ 24 × 24", box(button).width >= 24 && box(button).height >= 24, true);
        add("Rincian in the label row", top.contains(button), true);
      }
      const figure = first(".t-hero", hero);
      add("hero figure type", `${font(figure)} ${style(figure).letterSpacing}`, innerWidth < 360 ? "Manrope 32/40 600 -0.8px" : "Manrope 44/52 600 -1.1px");
      add("hero figure colour", style(figure).color, colour("ink"));
      add("hero figure tabular", /tabular-nums/.test(style(figure).fontVariantNumeric), true);
      add("hero figure gap", px(box(figure).top - box(top).bottom), 8, 1);
      const line = first(".hero-sub", hero);
      add("hero line type", font(line), "Inter 14/20 400");
      add("hero line colour", style(line).color, colour("muted"));
      add("hero line gap", px(box(line).top - box(figure).bottom), 8, 1);
      // Owner decision 1 (B): no per-day amount; the prototype shows one, so this check fails there by design.
      add("hero line without a per-day amount", /per hari/.test(line.textContent), false);

      // D4: split bar and legend.
      const split = first(".split", hero);
      if (split) {
        add("split bar height", px(box(split).height), 8);
        add("split bar gap", px(style(split).columnGap), 2);
        add("split bar after the line", px(box(split).top - box(line).bottom), 24, 1);
        add("split bar role", split.getAttribute("role"), "img");
        add("split bar name has percentages", /\d+ ?%/.test(split.getAttribute("aria-label") || ""), true);
        const segments = [...split.children].filter(shown);
        add("split segments ≥ 2 px", segments.every((segment) => box(segment).width >= 2 - 0.01), true);
        add("split segments square", segments.every((segment) => px(style(segment).borderTopLeftRadius) === 0), true);
        const expected = [colour("mExpense"), colour("mInvest"), colour("mWarning"), colour("mIncome")];
        add("split segment colours", segments.map((segment) => style(segment).backgroundColor).every((value) => expected.includes(value)), true);
      }
      const legend = first(".legend", hero);
      if (legend) {
        const blocks = all(".fig-block", legend);
        add("supporting figures shown", blocks.length, phone ? 2 : 4);
        add("legend columns", style(legend).gridTemplateColumns.trim().split(/\s+/).length, innerWidth < 1024 ? 2 : 4);
        add("legend gap", px(style(legend).columnGap), phone ? 16 : 24);
        if (split) add("legend after the bar", px(box(legend).top - box(split).bottom), 16, 1);
        const labels = blocks.map((block) => first(".fb-label .t-meta", block)?.textContent.trim());
        add("supporting labels", labels.join(" · "), phone ? "Pengeluaran · Ditabung & investasi" : "Masuk · Pengeluaran · Ditabung & investasi · Terjadwal");
        for (const block of blocks) {
          const name = first(".fb-label .t-meta", block)?.textContent.trim();
          const swatch = block.querySelector(".sw-bar");
          const fig = first(".t-fig", block);
          const caption = block.querySelector(":scope > .t-meta");
          add(`${name}: colour bar slot`, swatch ? `${px(box(swatch).width)}x${px(box(swatch).height)}` : "none", "16x2");
          add(`${name}: label type`, font(first(".fb-label .t-meta", block)), "Inter 12/16 400");
          add(`${name}: figure type`, `${font(fig)} ${style(fig).letterSpacing}`, "Inter 20/28 600 -0.2px");
          add(`${name}: figure colour`, style(fig).color, colour("ink"));
          add(`${name}: figure is not a button`, fig.closest("button, a") === null, true);
          if (caption) {
            add(`${name}: caption gap`, px(box(caption).top - box(fig).bottom), 4, 1);
            add(`${name}: caption on one line`, px(box(caption).height), 16);
          }
        }
      }
    }

    // D6, D7: section headers, list rows and day headings.
    for (const header of all(".stack .sec-h")) {
      const label = header.querySelector(".t-label");
      add(`section header "${label.textContent.trim()}" min-height`, px(style(header).minHeight), 32);
      add(`section header "${label.textContent.trim()}" margin`, px(style(header).marginBottom), 12);
    }
    const rows = all(".stack .list > li > .row");
    if (rows.length) {
      const row = rows[0];
      add("row min-height", px(style(row).minHeight), phone ? 64 : 56);
      add("row padding", `${style(row).paddingLeft} ${style(row).paddingRight}`, "12px 12px");
      add("row gap", px(style(row).columnGap), 16);
      add("row radius", px(style(row).borderTopLeftRadius), 0);
      add("rows are whole-row controls", rows.every((element) => ["A", "BUTTON"].includes(element.tagName)), true);
      const separated = rows.filter((element) => element.parentElement.nextElementSibling);
      add("row separators", separated.every((element) => edge(element, "Bottom") === `1px ${colour("line")}`), true);
      const lists = all(".stack .list");
      add("list bleed", lists.every((list) => px(style(list).marginLeft) === -12), true);
      const title = first(".r-title", row);
      add("row title type", font(title), "Inter 14/20 400");
      const meta = first(".r-meta", row);
      if (meta) add("row meta type", `${font(meta)} ${style(meta).color}`, `Inter 12/16 400 ${colour("muted")}`);
      const time = first(".r-time", row.closest("section"));
      if (time && !phone) add("time column", `${px(box(time).width)} ${font(time)}`, "40 Inter 12/16 400");
      const pillar = first(".r-pillar");
      if (pillar && !phone) add("status column width", px(box(pillar).width), 112);
      const amounts = all(".stack .row .r-amt");
      if (amounts.length && !phone) add("amount column ≥ 120", amounts.every((amount) => box(amount).width >= 120 - 0.5), true);
      const amountTexts = all(".stack .row .r-amt .t-amt, .stack .row .r-amt > .mask");
      add("row amounts 14/20 600", amountTexts.every((amount) => font(amount) === "Inter 14/20 600" || amount.classList.contains("mask")), true);
      const signColours = amountTexts.filter((amount) => /^[−+]/.test(amount.textContent.trim())).map((amount) => ({
        sign: amount.textContent.trim()[0], colour: style(amount).color,
      }));
      add("expense amounts in ink", signColours.filter((item) => item.sign === "−").every((item) => item.colour === colour("ink")), true);
      add("income amounts green", signColours.filter((item) => item.sign === "+").every((item) => item.colour === colour("income")), true);
      const icons = all(".stack .row .r-ico");
      if (icons.length) add("icon column", `${px(box(icons[0]).width)}x${px(box(icons[0]).height)}`, "40x20");
    }
    const days = all(".day-h");
    if (days.length) {
      const day = days[0];
      add("day heading height", px(box(day).height), 40);
      add("day heading sticky", `${style(day).position} ${style(day).top}`, "sticky 56px");
      add("day heading fill", style(day).backgroundColor, colour("s0"));
      add("day heading line", edge(day, "Bottom"), `1px ${colour("line")}`);
      const label = day.querySelector(".t-label");
      add("day heading label", `${font(label)} ${style(label).color} ${style(label).textTransform}`, `Inter 12/16 500 ${colour("ink")} uppercase`);
      const groups = all(".day-group");
      if (groups.length > 1) add("day group gap", px(box(groups[1]).top - box(groups[0]).bottom), 24, 1);
    }
    const more = all("a, button").find((element) => element.textContent.trim() === "Semua transaksi");
    if (more) {
      add("Semua transaksi height", px(box(more).height), phone ? 44 : 36);
      add("Semua transaksi edge", edge(more, "Top"), `1px ${colour("lineStrong")}`);
      const footer = more.parentElement;
      add("Semua transaksi row padding", `${style(footer).paddingTop} ${style(footer).paddingBottom}`, "16px 16px");
      add("Semua transaksi centred", Math.abs((box(more).left + box(more).right) / 2 - (box(footer).left + box(footer).right) / 2) <= 1, true);
    }

    // D8: context panel.
    const panel = first(".cols > .panel|aside.panel");
    if (panel) {
      add("panel fill", style(panel).backgroundColor, colour("s1"));
      add("panel radius", px(style(panel).borderTopLeftRadius), 12);
      add("panel padding", px(style(panel).paddingTop), phone ? 16 : 24);
      add("panel landmark", `${panel.tagName.toLowerCase()} ${panel.getAttribute("aria-label")}`, "aside Ringkasan simpanan");
      const blocks = all(".panel-block", panel);
      add("panel blocks", blocks.length, 3);
      for (const block of blocks.slice(1)) {
        add(`panel block "${block.querySelector(".t-label")?.textContent.trim()}" rule`, `${edge(block, "Top")} ${style(block).paddingTop} ${style(block).marginTop}`, `1px ${colour("line")} 24px 24px`);
      }
      const net = blocks[0];
      if (net) {
        add("Kekayaan bersih links to Rekening", net.tagName === "A" && /rekening|accounts/.test(net.getAttribute("href")), true);
        const fig = first(".t-fig", net);
        add("Kekayaan bersih figure", `${font(fig)} ${style(fig).color}`, `Inter 20/28 600 ${colour("ink")}`);
        add("Kekayaan bersih caption", /^aset .+ · utang .+/.test(first(".t-meta", net)?.textContent.trim() ?? ""), true);
      }
      const dana = blocks[1];
      if (dana) {
        add("Ketahanan Dana links to Target", dana.tagName === "A" && /target|goals/.test(dana.getAttribute("href")), true);
        const strong = first(".t-strong", dana);
        add("Ketahanan Dana line", strong ? font(strong) : "none", "Inter 14/20 500");
      }
      const kakeibo = all(".kk-row", panel);
      add("Kakeibo rows", kakeibo.length, 5);
      if (kakeibo[0]) {
        const row = kakeibo[0];
        add("Kakeibo row columns", style(row).gridTemplateColumns.split(" ")[0], "16px");
        add("Kakeibo row gaps", `${style(row).columnGap} ${style(row).rowGap}`, "8px 8px");
        add("Kakeibo row padding", `${style(row).paddingTop} ${style(row).paddingBottom}`, "12px 12px");
        const dot = first(".dot", row);
        add("Kakeibo dot", `${px(box(dot).width)}x${px(box(dot).height)}`, "8x8");
        const amount = first(".t-amt", row);
        add("Kakeibo amount type", font(amount), "Inter 14/20 600");
        const bar = first(".prog", row);
        if (bar) add("Kakeibo bar", `${px(box(bar).height)} ${style(bar).backgroundColor}`, `2 ${colour("s2")}`);
      }
      add("Kakeibo labels", kakeibo.map((row) => row.querySelector(".t-body").textContent.trim()).join(" · "), "Kebutuhan · Keinginan · Budaya · Tak Terduga · Belum diklasifikasi");
    }

    // Every figure stays inside its block (the phase 2 overflow, D1).
    const escaped = [];
    for (const element of all(".t-hero, .t-fig, .t-amt, .mask")) {
      const block = element.closest(".fig-block, .panel-block, .row, .kk-row, .hero, .dlg-b") ?? element.parentElement;
      const own = box(element);
      const outer = box(block);
      if (own.right > outer.right + 0.5 || own.left < outer.left - 0.5 || element.scrollWidth > element.clientWidth + 1) {
        escaped.push(`${element.textContent.trim().slice(0, 24)} +${Math.round(own.right - outer.right)}`);
      }
    }
    add("figures inside their blocks", escaped.length, 0);
    return { theme, width: innerWidth, checks, escaped: escaped.slice(0, 8), passed: checks.filter((item) => item.pass).length, failed: checks.filter((item) => !item.pass).length };
  }

  // D5: the Rincian dialog.
  add("dialog width", px(box(dialog).width), phone ? innerWidth : 480);
  add("dialog radius", phone ? `${style(dialog).borderTopLeftRadius} ${style(dialog).borderBottomLeftRadius}` : style(dialog).borderTopLeftRadius, phone ? "16px 0px" : "16px");
  add("dialog edge", edge(dialog, "Top"), `1px ${colour("line")}`);
  add("dialog fill", style(dialog).backgroundColor, colour("dialog"));
  if (!phone) add("dialog centred", Math.abs((box(dialog).left + box(dialog).right) / 2 - innerWidth / 2) <= 1, true);
  if (phone) {
    const grab = first(".sheet-grab", dialog);
    add("sheet grab", grab ? `${px(box(grab).width)}x${px(box(grab).height)}` : "none", "32x4");
    add("sheet at the bottom", Math.abs(box(dialog).bottom - innerHeight) <= 1, true);
  }
  const header = first(".dlg-h", dialog);
  add("dialog header padding", `${style(header).paddingTop} ${style(header).paddingRight} ${style(header).paddingBottom}`, phone ? "16px 16px 12px" : "24px 24px 16px");
  const title = first(".dlg-t > :first-child", dialog);
  add("dialog title", `${font(title)} ${title.textContent.trim()}`, "Manrope 20/28 600 Aman dipakai sampai gajian");
  const meta = first(".dlg-t .t-meta", dialog);
  add("dialog meta", meta ? `${font(meta)} ${meta.textContent.trim()}` : "none", "Inter 12/16 400 Rincian hitungan");
  if (meta) add("dialog meta gap", px(box(meta).top - box(title).bottom), 4, 1);
  const body = first(".dlg-b", dialog);
  add("dialog body padding", `${style(body).paddingLeft} ${style(body).paddingRight}`, phone ? "16px 16px" : "24px 24px");
  const rule = first(".dlg-b > .t-body", dialog);
  add("rule", rule?.textContent.trim().replace(/\s+/g, " ") ?? "none", "Masuk dikurangi Pengeluaran, Ditabung & investasi dan tagihan terjadwal sampai akhir siklus.");
  const calculation = first(".dlg-b .list", dialog);
  const lines = calculation ? all(":scope > li > .row", calculation) : [];
  const total = lines[lines.length - 1];
  const over = lines.find((line) => line.classList.contains("is-over"));
  const parts = lines.slice(0, -1).filter((line) => !line.closest(".parts") && line !== over);
  add("calculation lines", parts.length, 4);
  add("calculation line labels", parts.map((line) => first(".r-title", line).textContent.trim()).join(" · ").replace(/sampai \d+ \w+/, "sampai …"),
    "Masuk · Pengeluaran · Ditabung & investasi · Terjadwal sampai …");
  add("line min-height", parts.every((line) => px(style(line).minHeight) === 44), true);
  add("line padding", parts.every((line) => px(style(line).paddingLeft) === 0 && px(style(line).paddingRight) === 0), true);
  const amounts = parts.map((line) => first(".t-amt|.mask", line)).filter(Boolean);
  add("line amounts 14/20 600", amounts.every((amount) => font(amount) === "Inter 14/20 600"), true);
  if (total) {
    add("total row min-height", px(style(total).minHeight), 56);
    // An over period puts "Lebih dari uang masuk" under the strong rule, then the result (owner fix, 2026-11-06).
    add("total rule", edge(over ?? total, "Top"), `1px ${colour("lineStrong")}`);
    if (over) {
      add("over row", `${px(style(over).minHeight)} ${first(".r-title", over).textContent.trim()}`, "44 Lebih dari uang masuk");
      add("over amount positive", /^Rp|•/.test(first(".t-amt|.mask", over).textContent.trim()), true);
      add("total never negative", /^[−-]/.test(first(".t-fig|.mask", total).textContent.trim()), false);
    }
    add("total label", `${font(first(".r-title", total))} ${first(".r-title", total).textContent.trim()}`, "Inter 14/20 500 Aman dipakai");
    const figure = first(".t-fig|.mask", total);
    add("total figure type", font(figure), "Inter 20/28 600");
  }
  const footer = first(".dlg-f", dialog);
  if (footer) {
    add("footer padding", `${style(footer).paddingTop} ${style(footer).paddingLeft}`, phone ? "12px 16px" : "16px 24px");
    add("footer line", edge(footer, "Top"), `1px ${colour("line")}`);
    const link = first("a", footer);
    add("footer link", link?.textContent.trim(), "Lihat transaksi siklus ini");
  }
  add("dialog figures ≤ 20 px", all("*", dialog).every((element) => px(style(element).fontSize) <= 20), true);
  return { theme, width: innerWidth, dialog: true, checks, passed: checks.filter((item) => item.pass).length, failed: checks.filter((item) => !item.pass).length };
})();
