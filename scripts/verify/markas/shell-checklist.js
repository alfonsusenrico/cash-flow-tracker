// In-page shell checklist (apply-markas-shell D16), generated from the D1 to D9 tables of the change's design.md.
// It runs unchanged on the prototype (option-6) and on the app, because the app's shell uses the prototype's
// class names; where they differ, both selectors are listed. Evaluated by capture.mjs after measure.js.
// Every check is { check, value, expected, pass }; positions that depend on text metrics allow 1 px.
(() => {
  const PALETTE = {
    light: { s0: "#F8F7F4", s1: "#F1EFEB", s2: "#E6E3DD", line: "#E1DDD6", ink: "#1C1E22", muted: "#5B5E65", accent: "#FF334B" },
    dark: { s0: "#151517", s1: "#1C1D20", s2: "#26272B", line: "#2F3034", ink: "#E8E7E4", muted: "#A3A3A7", accent: "#FF4757" },
  };
  const hexToRgb = (hex) => {
    const value = parseInt(hex.slice(1), 16);
    return `rgb(${(value >> 16) & 255}, ${(value >> 8) & 255}, ${value & 255})`;
  };
  const bodyBg = getComputedStyle(document.body).backgroundColor;
  const theme = bodyBg === hexToRgb(PALETTE.dark.s0) ? "dark" : "light";
  const colour = (name) => hexToRgb(PALETTE[theme][name]);
  const checks = [];
  const add = (check, value, expected, tolerance = 0) => {
    const pass = typeof expected === "number" ? Math.abs(Number(value) - expected) <= tolerance : value === expected;
    checks.push({ check, value, expected, pass });
  };
  const first = (selectors) => {
    for (const selector of selectors.split("|")) {
      const found = [...document.querySelectorAll(selector)].find((element) => element.getBoundingClientRect().width > 0);
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
  const phone = innerWidth < 768;

  if (!phone) {
    const sidebar = first(".sb");
    if (sidebar) {
      const rail = innerWidth < 1024 || first(".app.is-collapsed|.app[data-collapsed=\"true\"]") !== null;
      add("sidebar width", px(box(sidebar).width), rail ? 64 : 240);
      add("sidebar fill", style(sidebar).backgroundColor, colour("s1"));
      add("sidebar right line", `${style(sidebar).borderRightWidth} ${style(sidebar).borderRightColor}`, `1px ${colour("line")}`);
      const brand = first(".sb-brand");
      add("brand row height", px(box(brand).height), 56);
      const mark = first(".sb-brand .mark");
      if (mark) {
        add("brand mark size", `${px(box(mark).width)}x${px(box(mark).height)}`, "24x24");
        add("brand mark fill", style(mark).backgroundColor, colour("accent"));
        add("brand mark radius", px(style(mark).borderTopLeftRadius), 6);
      }
      const name = first(".sb-brand .name");
      if (name) add("brand name", font(name), "Manrope 14/20 600");
      const labels = [...document.querySelectorAll(".sb-group > .t-label")];
      add("group labels", labels.map((label) => label.textContent.trim().toUpperCase()).join(" "), "HARIAN RINGKASAN SIMPANAN");
      if (!rail && labels[0]) {
        add("group label type", `${font(labels[0])} ${style(labels[0]).textTransform} ${style(labels[0]).letterSpacing}`, "Inter 12/16 500 uppercase 0.72px");
        add("group label colour", style(labels[0]).color, colour("muted"));
        add("group label padding", `${style(labels[0]).paddingTop} ${style(labels[0]).paddingLeft}`, "8px 24px");
      }
      const items = [...document.querySelectorAll(".sb .nav-item")].filter((item) => box(item).width > 0);
      add("nav items", items.length, 8);
      const labelOf = (item) => (item.querySelector(".lbl") || item).textContent.trim();
      add("nav item labels", items.map(labelOf).join(" · "), "Beranda · Tinjau · Transaksi · Analisis · Laporan · Rekening & Kantong · Target & Tagihan · Pengaturan");
      const resting = items.find((item) => item.getAttribute("aria-current") !== "page");
      if (resting) {
        add("nav item height", px(box(resting).height), 36);
        add("nav item radius", px(style(resting).borderTopLeftRadius), 6);
        add("nav item margin-left", px(style(resting).marginLeft), 8);
        if (!rail) add("nav item padding-left", px(style(resting).paddingLeft), 16);
        add("nav item type", font(resting), "Inter 14/20 500");
        add("nav item colour", style(resting).color, colour("muted"));
        const icon = resting.querySelector("svg");
        add("nav icon size", `${px(box(icon).width)}x${px(box(icon).height)}`, "16x16");
      }
      const current = items.find((item) => item.getAttribute("aria-current") === "page");
      if (current) {
        add("current item fill", style(current).backgroundColor, colour("s2"));
        add("current item text", style(current).color, colour("ink"));
        add("current item border", style(current).borderTopWidth, "0px");
        const bar = style(current, "::before");
        add("selection bar", `${bar.width} ${bar.height} ${bar.backgroundColor}`, `2px 20px ${colour("accent")}`);
        add("selection bar left", px(bar.left), -8);
      }
      const count = first(".sb .nav-item .count");
      if (count && !rail) {
        add("count type", font(count), "Inter 12/16 400");
        add("count colour", style(count).color, colour("muted"));
      }
      const foot = first(".sb-foot");
      add("foot line", `${style(foot).borderTopWidth} ${style(foot).borderTopColor}`, `1px ${colour("line")}`);
      const avatar = first(".user-btn .avatar");
      add("avatar", `${px(box(avatar).width)}x${px(box(avatar).height)} r${px(style(avatar).borderTopLeftRadius)}`, "32x32 r6");
    }

    const bar = first(".topbar");
    if (bar) {
      add("top bar height", px(box(bar).height), 56);
      add("top bar position", style(bar).position, "sticky");
      add("top bar fill", style(bar).backgroundColor, colour("s0"));
      add("top bar line", `${style(bar).borderBottomWidth} ${style(bar).borderBottomColor}`, `1px ${colour("line")}`);
      add("top bar side padding", px(style(bar).paddingLeft), innerWidth < 1024 ? 32 : 48);
      const search = first(".topbar .search input|.topbar input[type=search]");
      add("search size", `${px(box(search).width)}x${px(box(search).height)}`, "320x36");
      add("search fill", style(search).backgroundColor, colour("s1"));
      add("search edge", `${style(search).borderTopWidth} ${style(search).borderTopColor}`, `1px ${colour("line")}`);
      add("search key cap", Boolean(first(".topbar .search .kbd|.topbar .search .ws-kbd")), true);
      const names = [...bar.querySelectorAll("button")].filter((button) => box(button).width > 0)
        .map((button) => (button.getAttribute("aria-label") || button.textContent).trim().replace(/, .*$/, ""));
      add("top bar actions", names.join(" · "), "Sembunyikan saldo · Tema · Notifikasi · Catat");
      add("top bar has no heading", bar.querySelectorAll("h1, h2").length, 0);
      // The shell and the title block; page bodies keep their own buttons until their phase.
      const primary = [...document.querySelectorAll(".btn-primary, .ws-btn-accent, .ws-btn-ink")].filter(
        (button) => box(button).width > 0 && button.closest(".topbar, .sb, .title-block"),
      );
      add("primary buttons in the shell and title block", primary.length, 1);
      const catat = primary[0];
      if (catat) {
        add("Catat height", px(box(catat).height), 36);
        add("Catat fill", style(catat).backgroundColor, colour("accent"));
      }
      const icons = [...bar.querySelectorAll("svg")].filter((svg) => box(svg).width > 0);
      add("top bar icons 16 px", icons.every((svg) => px(box(svg).width) === 16), true);
    }
  } else {
    const header = first(".ph-header");
    if (header) {
      add("phone header height", px(box(header).height), 56);
      add("phone header position", style(header).position, "sticky");
      add("phone header fill", style(header).backgroundColor, colour("s0"));
      const targets = [...header.querySelectorAll("a, button")].filter((element) => !element.classList.contains("brand"));
      // The bell widens with its count; every target is at least 44 × 44.
      add("phone header targets ≥ 44 × 44", targets.every((element) => box(element).width >= 44 && box(element).height >= 44), true);
      add("phone header controls", targets.map((element) => (element.getAttribute("aria-label") || "").replace(/, .*$/, "")).join(" · "), "Cari transaksi · Sembunyikan saldo · Notifikasi");
    }
    const tabs = first(".tabbar");
    if (tabs) {
      add("tab bar height", px(box(tabs).height), 64);
      add("tab bar fill", style(tabs).backgroundColor, colour("s1"));
      add("tab bar line", `${style(tabs).borderTopWidth} ${style(tabs).borderTopColor}`, `1px ${colour("line")}`);
      const columns = [...tabs.children];
      add("tab labels", columns.map((tab) => tab.textContent.replace(/\d+/g, "").trim()).join(" · "), "Beranda · Tinjau · Catat · Transaksi · Lainnya");
      const current = columns.find((tab) => tab.getAttribute("aria-current") === "page");
      if (current) {
        const mark = style(current, "::before");
        add("current tab bar", `${mark.height} ${mark.backgroundColor}`, `2px ${colour("accent")}`);
        add("current tab bar width", px(parseFloat(mark.width)), px(box(current).width / 2), 1);
        add("current tab colour", style(current).color, colour("ink"));
      }
      const plus = first(".tabbar .catat .plus");
      add("Catat block", `${px(box(plus).width)}x${px(box(plus).height)} ${style(plus).backgroundColor}`, `44x32 ${colour("accent")}`);
      const label = columns[0].querySelector("span");
      add("tab label type", font(label), "Inter 12/16 500");
      add("tab icons 20 px", [...tabs.querySelectorAll("svg")].every((svg) => px(box(svg).width) === 20), true);
    }
  }

  const shellSvgs = [...document.querySelectorAll(".sb svg, .topbar svg, .ph-header svg, .tabbar svg")].filter((svg) => box(svg).width > 0);
  // Dotted icons ("more") are filled dots, not strokes.
  const stroked = shellSvgs.filter((svg) => !svg.querySelector("circle"));
  add("shell icons stroke 1.5", stroked.every((svg) => svg.getAttribute("stroke-width") === "1.5"), true);

  const title = first(".title-block");
  if (title) {
    add("title block padding", `${style(title).paddingTop} ${style(title).paddingBottom}`, phone ? "24px 24px" : "48px 32px");
    const heading = title.querySelector("h1");
    add("title type", `${font(heading)} ${style(heading).letterSpacing}`, "Manrope 24/32 600 -0.36px");
    const sub = title.querySelector(".sub");
    if (sub) {
      add("subtitle type", font(sub), "Inter 14/20 400");
      add("subtitle colour", style(sub).color, colour("muted"));
      add("subtitle gap", px(box(sub).top - box(heading).bottom), 4);
    }
    const next = title.nextElementSibling;
    // On the phone the actions wrap under the title, so the y is only fixed for a title block without actions.
    const hasActions = [...title.querySelectorAll(".tb-actions > *")].some((child) => box(child).width > 0);
    // 192 at 1440 with a one-line subtitle (168 without); a subtitle that wraps on a narrow screen adds its lines.
    const extraLines = sub ? Math.round((box(sub).height - 20) / 20) * 20 : 0;
    const expectedY = (phone ? (sub ? 56 + 24 + 32 + 4 + 20 + 24 : 56 + 24 + 32 + 24) : sub ? 192 : 168) + extraLines;
    if (next && !(phone && hasActions)) add("first content y", px(box(next).top + scrollY), expectedY, 1);
    const page = first("main");
    const inner = title.parentElement;
    const narrow = /^\/(ledger|settings)|^#\/(transaksi|pengaturan)/.test(location.hash || location.pathname);
    add("frame max width", style(inner).maxWidth, narrow ? "1040px" : "1200px");
    add("page side margin", px(style(page).paddingLeft), phone ? 16 : innerWidth < 1024 ? 32 : 48);
    add("h1 count", document.querySelectorAll("h1").length, 1);

    const actions = title.querySelector(".tb-actions");
    if (actions) {
      const visible = [...actions.children].filter((child) => box(child).width > 0);
      add("visible actions (≤ 2 + …)", visible.filter((child) => !/Tindakan lain/.test(child.getAttribute("aria-label") || "")).length <= 2 || visible.length <= 2, true);
    }
  }
  // Nothing in the page may reach past the frame's right edge, except inside its own horizontal scroller. The
  // frame is the page-inner element (or main on a build without it), so the same check runs before and after.
  const frame = first(".page-inner|main#main|main");
  const outsideFrame = [];
  if (frame) {
    const padding = frame.tagName === "MAIN" ? parseFloat(style(frame).paddingRight) : 0;
    const right = box(frame).right - padding + 1;
    const clipped = (element) => {
      for (let node = element.parentElement; node && node !== frame; node = node.parentElement) {
        if (["auto", "scroll", "hidden", "clip"].includes(style(node).overflowX)) return true;
      }
      return false;
    };
    for (const element of frame.querySelectorAll("*")) {
      const rect = box(element);
      if (rect.width > 0 && rect.right > right && !clipped(element) && style(element).position !== "fixed" && !element.closest(".topbar, .ph-header, .ws-topbar")) {
        outsideFrame.push(`${element.tagName.toLowerCase()}.${String(element.className).split(" ")[0]} "${(element.textContent || "").trim().slice(0, 30)}" +${Math.round(rect.right - right)}`);
      }
    }
    add("content inside the frame", outsideFrame.length, 0);
  }
  return { theme, width: innerWidth, checks, outsideFrame: outsideFrame.slice(0, 8), passed: checks.filter((item) => item.pass).length, failed: checks.filter((item) => !item.pass).length };
})();
