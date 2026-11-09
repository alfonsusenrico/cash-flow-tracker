// In-page Pengaturan checklist (apply-markas-pengaturan D24), from D1 to D15 of the change's design.md, option-6
// styles.css `.set`, `.rail`, `.toggle`, `.switch`, `.qr` and SPEC §7, §8 and §13. Evaluated by the Pengaturan capture
// runs on /settings, its sections and its dialogs. Every check is { check, value, expected, pass }.
(() => {
  const checks = [];
  const add = (check, value, expected, tolerance = 0) => {
    const pass = typeof expected === "number" ? Math.abs(Number(value) - expected) <= tolerance : value === expected;
    checks.push({ check, value, expected, pass });
  };
  const atLeast = (check, value, minimum) => checks.push({ check, value, expected: `≥ ${minimum}`, pass: Number(value) >= minimum });
  const atMost = (check, value, maximum) => checks.push({ check, value, expected: `≤ ${maximum}`, pass: Number(value) <= maximum + 0.5 });
  const within = (check, value, allowed) => checks.push({ check, value, expected: allowed, pass: value.every((item) => allowed.includes(item)) });
  const shown = (element) => Boolean(element) && element.getBoundingClientRect().width > 0 && element.getBoundingClientRect().height > 0;
  const all = (selector, scope = document) => [...scope.querySelectorAll(selector)].filter(shown);
  const first = (selector, scope = document) => all(selector, scope)[0] ?? null;
  const style = (element, pseudo) => getComputedStyle(element, pseudo);
  const box = (element) => element.getBoundingClientRect();
  const px = (value) => Math.round(parseFloat(value) * 100) / 100;
  const phone = innerWidth < 768;
  const wide = innerWidth >= 1200;
  const tokens = getComputedStyle(document.documentElement);
  const probe = document.createElement("span");
  document.body.appendChild(probe);
  const rgb = (name) => {
    probe.style.color = tokens.getPropertyValue(name).trim();
    return getComputedStyle(probe).color;
  };

  // The app's dialogs are `dialog.shell-dialog`; the prototype's are `[role=dialog]` panels.
  const dialogs = all("dialog.shell-dialog[open], .dialog[role=dialog]");
  const dialog = dialogs[dialogs.length - 1] ?? null;
  const set = first(".set");
  const scope = dialog ?? document.querySelector("main") ?? document.body;

  add("no horizontal page overflow", document.documentElement.scrollWidth > innerWidth, false);

  if (set && !dialog) {
    const rail = first(".set-rail", set);
    const form = first(".set-form", set) ?? rail.nextElementSibling;
    if (innerWidth === 1440) add("settings frame starts at y = 168 (prototype)", Math.round(box(set).top + scrollY), 168, 2);
    if (phone) {
      add("phone: one column", style(set).display, "block");
      add("phone: rail 24 px above the section", px(style(rail).marginBottom), 24);
    } else {
      add("rail width (200 from 1200, 184 below)", Math.round(box(rail).width), wide ? 200 : 184);
      add("rail to form gap (48 from 1200, 32 below)", Math.round(box(form).left - box(rail).right), wide ? 48 : 32);
      if (wide) atMost("form column ≤ 720", Math.round(box(form).width), 720);
      add("rail beside the form", Math.round(box(rail).top), Math.round(box(form).top), 1);
    }
    const items = all(".rail-item", rail);
    add("rail rows: nine sections and Keluar", items.length, 10);
    add("rail item height (36, 44 on a phone)", Math.round(box(items[0]).height), phone ? 44 : 36);
    add("rail group labels", all(".t-label", rail).length, 3);
    const current = all('.rail-item[aria-current="page"]', rail);
    add("one current section", current.length, 1);
    if (current[0]) {
      const bar = style(current[0], "::before");
      add("selection bar 2 × 20", `${px(bar.width)} × ${px(bar.height)}`, "2 × 20");
      add("selection bar square", px(bar.borderTopLeftRadius), 0);
    }
    const title = first(".set-head h2", form) ?? first("h2.t-dtitle", form);
    if (title) {
      add("section title 20/28", `${px(style(title).fontSize)}/${px(style(title).lineHeight)}`, "20/28");
      add("section title Manrope 600", /manrope/i.test(style(title).fontFamily) && style(title).fontWeight === "600", true);
      const line = title.nextElementSibling;
      add("description 12/16", `${px(style(line).fontSize)}/${px(style(line).lineHeight)}`, "12/16");
      add("description margins 4 / 32", `${px(style(line).marginTop)} / ${px(style(line).marginBottom)}`, "4 / 32");
    }
    const parts = all(".stack > *", form).slice(1);
    if (parts.length) add("parts 48 apart (32 on a phone)", [...new Set(parts.map((part) => px(style(part).marginTop)))].join(","), phone ? "32" : "48");
    add("no card on settings", all(".ws-card", set).length, 0);
    const toggles = all(".toggle", set);
    if (toggles.length) add("toggle rows ≥ 56", toggles.every((row) => Math.round(box(row).height) >= 56), true);
    const switches = all(".switch", set);
    if (switches.length) add("switch 36 × 20", switches.map((s) => `${Math.round(box(s).width)} × ${Math.round(box(s).height)}`).join(","), switches.map(() => "36 × 20").join(","));
    const segments = all(".ws-seg button", set);
    if (segments.length) add("segment height (30, 40 on a phone)", [...new Set(segments.map((b) => Math.round(box(b).height)))].join(","), phone ? "40" : "30");
    const fields = all(".field-control", set);
    if (fields.length) add("fields 40 high", [...new Set(fields.map((f) => Math.round(box(f).height)))].join(","), "40");
    const qr = first(".qr:not(.is-pairing)", set);
    if (qr) {
      // 136 in the prototype; 154 with D9's fallback, so the link's 45 modules get 3 px each.
      add("download QR box 154 (D9 fallback)", `${Math.round(box(qr).width)} × ${Math.round(box(qr).height)}`, "154 × 154");
      const canvas = first("canvas", qr);
      if (canvas) atLeast("download QR code ≥ 136 css px", Math.round(box(canvas).width), 136);
      add("download QR on white", style(qr).backgroundColor, "rgb(255, 255, 255)");
    }
    const box24 = all(".set-box", set);
    if (box24.length) add("box padding (24, 16 on a phone)", px(style(box24[0]).paddingTop), phone ? 16 : 24);
    if (phone) {
      // Buttons and links are 44 px targets; fields stay 40 and segments 40 (SPEC §7, styles.css phone block), and
      // the 36 × 20 switch has a 44 px hit area around it.
      const small = all("button, a.ws-btn, .rail-item", set).filter((element) => {
        if (element.classList.contains("switch") || element.closest(".ws-seg") || element.closest("summary")) return false;
        return box(element).height < 43.5;
      }).map((element) => (element.getAttribute("aria-label") || element.textContent || element.tagName).trim().slice(0, 24));
      const hit = all(".switch", set).map((element) => {
        const before = style(element, "::before");
        return Math.round(box(element).height - 2 * parseFloat(before.top || "0"));
      });
      if (hit.length) add("switch hit area 44 on a phone", [...new Set(hit)].join(","), "44");
      checks.push({ check: "phone targets ≥ 44", value: small.slice(0, 6), expected: [], pass: small.length === 0 });
    }
  }

  if (dialog) {
    if (!phone) {
      const width = Math.round(box(dialog).width);
      const pairing = Boolean(first(".pair-body, .qr", dialog));
      add("dialog width (520 pairing, 480 revoke)", width, pairing ? 520 : 480);
    } else {
      add("phone: dialog is a bottom sheet", Math.round(box(dialog).bottom), innerHeight, 1);
    }
    const code = first(".qr.is-pairing canvas", dialog) ?? first(".qr svg, .qr canvas", dialog);
    if (code) {
      atLeast("pairing QR ≥ 200 css px (owner 2026-11-08)", Math.round(box(code).width), 200);
      add("pairing QR square", Math.round(box(code).width), Math.round(box(code).height));
      const frame = code.closest(".qr");
      add("pairing QR on white with an 8 px quiet border", `${style(frame).backgroundColor} ${px(style(frame).paddingTop)}`, "rgb(255, 255, 255) 8");
      add("pairing QR canvas drawn at the pixel ratio", code.width >= Math.round(box(code).width * Math.max(1, Math.round(devicePixelRatio))), true);
    }
  }

  // Type scale, weights, text styles and borders over the visible text and boxes of the page or dialog.
  const texts = all("*", scope).filter((element) => [...element.childNodes].some((node) => node.nodeType === 3 && node.textContent.trim()));
  const sizes = [...new Set(texts.map((element) => px(style(element).fontSize)))].sort((a, b) => a - b);
  const weights = [...new Set(texts.map((element) => style(element).fontWeight))].sort();
  within("font sizes", sizes, [12, 14, 16, 20, 24, 32]);
  within("font weights", weights, ["400", "500", "600"]);
  const textStyles = new Set(texts.filter((element) => !element.closest("aside.ws-side, nav, header")).map((element) => `${px(style(element).fontSize)}/${style(element).fontWeight}`));
  checks.push({ check: "text styles (size/weight) in view", value: [...textStyles].join(" "), expected: "≤ 8", pass: textStyles.size <= 8 });
  add("no text of 33 px or more (no hero figure)", all("*", scope).filter((element) => element.childElementCount === 0 && px(style(element).fontSize) >= 33).length, 0);
  const thick = all("*", scope).filter((element) => {
    const s = style(element);
    return ["Top", "Right", "Bottom", "Left"].some((side) => px(s[`border${side}Width`]) >= 2 && s[`border${side}Style`] !== "none");
  }).map((element) => element.tagName.toLowerCase() + "." + [...element.classList].slice(0, 2).join("."));
  add("no border of 2 px or more", thick.join(","), "");
  const neutral = ["--line", "--line-strong", "--field-focus", "--muted"].map(rgb);
  const coloured = all("*", scope).filter((element) => {
    const s = style(element);
    return ["Top", "Right", "Bottom", "Left"].some((side) => s[`border${side}Style`] !== "none" && px(s[`border${side}Width`]) > 0 &&
      !/rgba\(0, 0, 0, 0\)|, 0\)$/.test(s[`border${side}Color`]) && !neutral.includes(s[`border${side}Color`]) && element !== document.activeElement);
  }).map((element) => element.tagName.toLowerCase() + "." + [...element.classList].slice(0, 2).join("."));
  checks.push({ check: "no coloured border (selection bar and focus aside)", value: coloured.slice(0, 6), expected: [], pass: coloured.length === 0 });
  const tinted = all("*", scope).filter((element) => /in-bg|out-bg|warn-bg/.test(element.className?.toString?.() ?? ""));
  add("no tinted status fills", tinted.length, 0);
  probe.remove();
  return { pass: checks.filter((item) => item.pass).length, fail: checks.filter((item) => !item.pass).length, checks };
})()
