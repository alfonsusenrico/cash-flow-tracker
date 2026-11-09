// Compares two capture folders pixel by pixel (apply-markas-cleanup D16): for every PNG present in both, counts
// the pixels whose colour differs by more than --tolerance on any channel, and writes a diff image (changed pixels
// in red over a faded left image) for each pair that differs. The comparison runs in headless Chrome's canvas, so
// no image library is needed. Writes pixel-diff.json and prints one line per differing pair.
//
// Usage: node pixel-diff.mjs --left <dir> --right <dir> --out <dir> [--only <regex>] [--tolerance 0] [--port 9632]
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch } from "./cdp.mjs";

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 2) args[argv[index].replace(/^--/, "")] = argv[index + 1];
  return args;
}

const args = parseArgs(process.argv.slice(2));
if (!args.left || !args.right || !args.out) {
  console.error("Usage: node pixel-diff.mjs --left <dir> --right <dir> --out <dir> [--only <regex>] [--tolerance 0]");
  process.exit(2);
}
mkdirSync(args.out, { recursive: true });
const only = args.only ? new RegExp(args.only) : null;
const tolerance = Number(args.tolerance ?? 0);
const rightFiles = new Set(readdirSync(args.right).filter((name) => name.endsWith(".png")));
const names = readdirSync(args.left).filter((name) => name.endsWith(".png") && (!only || only.test(name))).sort();
const dataUrl = (file) => `data:image/png;base64,${readFileSync(file).toString("base64")}`;

// Runs in the page: loads both images, counts differing pixels and returns a diff image when any differ.
const COMPARE = `async (left, right, tolerance) => {
  const load = (src) => new Promise((resolve, reject) => { const image = new Image(); image.onload = () => resolve(image); image.onerror = reject; image.src = src; });
  const [a, b] = await Promise.all([load(left), load(right)]);
  if (a.width !== b.width || a.height !== b.height) return { size: [a.width, a.height, b.width, b.height], differing: null };
  const pixels = (image) => {
    const canvas = document.createElement("canvas");
    canvas.width = image.width; canvas.height = image.height;
    const context = canvas.getContext("2d");
    context.drawImage(image, 0, 0);
    return context.getImageData(0, 0, image.width, image.height);
  };
  const first = pixels(a), second = pixels(b);
  const out = new ImageData(a.width, a.height);
  let differing = 0;
  for (let index = 0; index < first.data.length; index += 4) {
    let changed = false;
    for (let channel = 0; channel < 4; channel += 1) {
      if (Math.abs(first.data[index + channel] - second.data[index + channel]) > tolerance) changed = true;
    }
    if (changed) {
      differing += 1;
      out.data.set([255, 0, 0, 255], index);
    } else {
      const grey = (first.data[index] + first.data[index + 1] + first.data[index + 2]) / 3;
      out.data.set([grey, grey, grey, 70], index);
    }
  }
  if (differing === 0) return { size: [a.width, a.height], differing: 0 };
  const canvas = document.createElement("canvas");
  canvas.width = a.width; canvas.height = a.height;
  canvas.getContext("2d").putImageData(out, 0, 0);
  return { size: [a.width, a.height], differing, image: canvas.toDataURL("image/png") };
}`;

const browser = await launch({ port: Number(args.port ?? 9632), profileDir: path.join(args.out, ".profile") });
const results = [];
try {
  await browser.navigate("about:blank");
  for (const name of names) {
    if (!rightFiles.has(name)) {
      results.push({ name, missing: "right" });
      console.log(`missing on the right: ${name}`);
      continue;
    }
    const left = JSON.stringify(dataUrl(path.join(args.left, name)));
    const right = JSON.stringify(dataUrl(path.join(args.right, name)));
    const result = await browser.evaluate(`(${COMPARE})(${left}, ${right}, ${tolerance})`);
    if (result.image) {
      writeFileSync(path.join(args.out, name), Buffer.from(result.image.split(",")[1], "base64"));
      delete result.image;
    }
    results.push({ name, ...result });
    if (result.differing !== 0) console.log(`${name}: ${result.differing === null ? `size ${result.size.join("x")}` : `${result.differing} px`}`);
  }
} finally {
  browser.close();
}
const differing = results.filter((result) => result.differing !== 0);
writeFileSync(path.join(args.out, "pixel-diff.json"), JSON.stringify({ compared: results.length, differing: differing.length, results }, null, 1));
console.log(`compared ${results.length}, identical ${results.length - differing.length}, different ${differing.length}`);
