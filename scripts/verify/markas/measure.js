// In-page checklist for the Markas+ global rules (option-6 SPEC §13): families, sizes, weights, text
// styles, big figures, borders, outlines, rings, card shadows, radii, tabular figures and overflow.
// Evaluated by capture.mjs in the page; window.__measureRoot may name a selector to limit the scope.
(() => {
  const root = (window.__measureRoot && document.querySelector(window.__measureRoot)) || document.body;
  const SIZES = [12, 14, 16, 20, 24, 32, 44];
  const WEIGHTS = ["400", "500", "600"];
  const RADII = [0, 6, 12, 16];
  const LAYER = '[role="dialog"], [role="menu"], [role="alertdialog"], [role="listbox"], [role="tooltip"], ' +
    '.ws-menu, .ws-explain, .ws-toast, dialog, .dialog, .popover, .toast';
  const EXAMPLES = 12;

  const probe = document.createElement("span");
  probe.style.display = "none";
  document.body.appendChild(probe);
  const colourCache = new Map();
  const toRgb = (value) => {
    if (!value) return "";
    if (!colourCache.has(value)) {
      probe.style.color = "";
      probe.style.color = value;
      colourCache.set(value, probe.style.color ? getComputedStyle(probe).color : "");
    }
    return colourCache.get(value);
  };
  const tokenColours = (element, names) => {
    const style = getComputedStyle(element);
    return names.map((name) => toRgb(style.getPropertyValue(name).trim())).filter(Boolean);
  };
  const transparent = (colour) => colour === "rgba(0, 0, 0, 0)" || colour === "transparent" || /,\s*0\)$/.test(colour);

  const visible = (element) => {
    const box = element.getBoundingClientRect();
    if (box.width < 1 || box.height < 1) return false;
    const style = getComputedStyle(element);
    if (style.visibility === "hidden" || style.display === "none" || Number(style.opacity) === 0) return false;
    if (element.closest(".sr-only")) return false;
    return box.right >= 0 && box.left <= innerWidth;
  };
  const describe = (element) => {
    const classes = typeof element.className === "string" ? element.className.trim().split(/\s+/).slice(0, 4).join(".") : "";
    const text = (element.getAttribute("aria-label") || element.textContent || "").replace(/\s+/g, " ").trim().slice(0, 40);
    return `${element.tagName.toLowerCase()}${classes ? "." + classes : ""} "${text}"`;
  };
  const tally = (items, keyOf) => {
    const counts = {};
    for (const item of items) {
      const key = keyOf(item);
      counts[key] = (counts[key] || 0) + 1;
    }
    return Object.fromEntries(Object.entries(counts).sort((a, b) => b[1] - a[1]));
  };
  const familyOf = (style) => {
    const first = style.fontFamily.split(",")[0].replace(/["']/g, "").trim().toLowerCase();
    if (first.startsWith("inter")) return "Inter";
    if (first.startsWith("manrope")) return "Manrope";
    return first;
  };

  const texts = [];
  const seen = new Set();
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  while (walker.nextNode()) {
    const element = walker.currentNode.parentElement;
    if (!element || seen.has(element) || ["SCRIPT", "STYLE", "NOSCRIPT", "TITLE"].includes(element.tagName)) continue;
    seen.add(element);
    const own = [...element.childNodes]
      .filter((child) => child.nodeType === 3)
      .map((child) => child.textContent)
      .join(" ")
      .replace(/\s+/g, " ")
      .trim();
    if (!own || !visible(element)) continue;
    const style = getComputedStyle(element);
    texts.push({
      element,
      text: own,
      size: parseFloat(style.fontSize),
      weight: style.fontWeight,
      family: familyOf(style),
      numeric: style.fontVariantNumeric,
      features: style.fontFeatureSettings,
      inShell: Boolean(element.closest("aside, nav, header, [role=banner]")),
    });
  }

  const offScale = texts.filter((item) => !SIZES.includes(item.size) || !WEIGHTS.includes(item.weight));
  const families = tally(texts, (item) => item.family);
  const manropeMisuse = texts.filter((item) => item.family === "Manrope" &&
    !item.element.closest("h1, h2, h3, [role=heading], .t-title, .t-dtitle, .t-hero, .q-h, .ws-brand-name, .ws-brand, .brand, .name"));
  const mainStyles = tally(texts.filter((item) => !item.inShell), (item) => `${item.family}|${item.size}|${item.weight}`);
  const bigFigures = texts.filter((item) => item.size >= 33);

  // An amount, count or time that stands on its own; amounts inside a sentence are prose (SPEC §1).
  const isAmount = (text) => text.length <= 40 &&
    /^[−+-]?\s?(?:Rp|USD|US\$)\s?[\d.,]+$|^[−+-]?\d[\d.,]*(?:rb|jt|%)?$|^\d{1,2}[.:]\d{2}$/.test(text);
  const isTabular = (item) => /tabular-nums/.test(item.numeric) || /"tnum"(?! 0)/.test(item.features);
  const amountsNotTabular = texts.filter((item) => isAmount(item.text) && !isTabular(item));
  const proseTabular = texts.filter((item) => /hari ke-\d/.test(item.text) && isTabular(item));

  const thickBorders = [];
  const colouredBorders = [];
  const outlines = [];
  const rings = [];
  const shadows = [];
  const radii = [];
  const layers = [];
  for (const element of root.querySelectorAll("*")) {
    if (!visible(element) || element.closest("svg")) continue;
    const style = getComputedStyle(element);
    const box = element.getBoundingClientRect();
    const neutral = tokenColours(element, ["--line", "--line-strong", "--field-focus", "--muted"]);
    const sides = ["Top", "Right", "Bottom", "Left"].filter((side) =>
      style[`border${side}Style`] !== "none" && parseFloat(style[`border${side}Width`]) > 0);
    for (const side of sides) {
      const width = parseFloat(style[`border${side}Width`]);
      const colour = style[`border${side}Color`];
      if (width >= 2 && !transparent(colour)) thickBorders.push(`${describe(element)} ${side} ${width}px`);
      if (!transparent(colour) && !neutral.includes(colour)) colouredBorders.push(`${describe(element)} ${side} ${colour}`);
    }
    const focused = element === document.activeElement;
    // A 1 px neutral or ink outline is allowed (SPEC §5: the selected swatch); 2 px only in the focus colour.
    const outlineWidth = parseFloat(style.outlineWidth);
    const focusColour = tokenColours(element, ["--focus"]);
    const outlineOk = outlineWidth < 2
      ? [...neutral, ...tokenColours(element, ["--text"])].includes(style.outlineColor)
      : focusColour.includes(style.outlineColor);
    if (style.outlineStyle !== "none" && outlineWidth > 0 && !transparent(style.outlineColor) && !focused && !outlineOk) {
      outlines.push(`${describe(element)} ${style.outlineWidth} ${style.outlineColor}`);
    }
    if (style.boxShadow !== "none") {
      const inLayer = Boolean(element.closest(LAYER)) && element === element.closest(LAYER);
      const ringOnly = /^(?:(?:rgba?\([^)]*\)|#\w+) )?0px 0px 0px \d+(?:\.\d+)?px(?: (?:rgba?\([^)]*\)))?$/.test(style.boxShadow);
      if (ringOnly) {
        const ringColour = (style.boxShadow.match(/rgba?\([^)]*\)/) || [""])[0];
        const allowed = [...neutral, ...tokenColours(element, ["--ring"])];
        if (!focused && !allowed.includes(ringColour)) rings.push(`${describe(element)} ${style.boxShadow}`);
      } else if (inLayer) {
        layers.push(`${describe(element)} ${style.boxShadow.slice(0, 90)}`);
      } else {
        shadows.push(`${describe(element)} ${style.boxShadow.slice(0, 90)}`);
      }
    }
    const radius = parseFloat(style.borderTopLeftRadius);
    const painted = !transparent(style.backgroundColor) || sides.length > 0;
    if (painted && radius > 0) {
      const short = Math.min(box.width, box.height);
      const circle = radius >= short / 2 - 0.5 && Math.abs(box.width - box.height) <= 2;
      if (!circle && !RADII.includes(Math.round(radius))) {
        radii.push(`${describe(element)} ${radius >= short / 2 ? "pill" : radius + "px"}`);
      }
    }
  }
  probe.remove();

  const examples = (items, map = (value) => value) => ({ count: items.length, examples: items.slice(0, EXAMPLES).map(map) });
  const itemText = (item) => `${item.family} ${item.size}/${item.weight} "${item.text.slice(0, 40)}"`;
  return {
    families,
    sizes: tally(texts, (item) => `${item.size}px`),
    weights: tally(texts, (item) => item.weight),
    textStylesInMain: Object.keys(mainStyles).length,
    mainStyles,
    offScale: examples(offScale, itemText),
    manropeOutsideTitles: examples(manropeMisuse, itemText),
    bigFigures: examples(bigFigures, itemText),
    amountsNotTabular: examples(amountsNotTabular, itemText),
    proseTabular: examples(proseTabular, itemText),
    thickBorders: examples(thickBorders),
    colouredBorders: examples(colouredBorders),
    outlines: examples(outlines),
    colouredRings: examples(rings),
    nonLayerShadows: examples(shadows),
    layerShadows: examples(layers),
    offScaleRadii: examples(radii),
    overflowX: document.documentElement.scrollWidth - document.documentElement.clientWidth,
  };
})();
