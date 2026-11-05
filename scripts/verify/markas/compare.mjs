// Writes side-by-side images (left | right) of two capture folders, composed in the headless browser so no
// image library is needed.
//
// Usage: node compare.mjs --left <dir> --right <dir> --out <dir> --pairs beranda-1440-light,transaksi-390-dark
//        [--left-label "Prototype"] [--right-label "App"] [--right-name beranda=komponen]
// A pair names the capture file without ".png"; --right-name maps a left name to a different right name.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 2) args[argv[index].replace(/^--/, "")] = argv[index + 1];
  return args;
}

const args = parseArgs(process.argv.slice(2));
mkdirSync(args.out, { recursive: true });
const renames = Object.fromEntries((args["right-name"] ?? "").split(",").filter(Boolean).map((pair) => pair.split("=")));
const dataUrl = (file) => `data:image/png;base64,${readFileSync(file).toString("base64")}`;
const browser = await launch({ port: Number(args.port ?? 9632), profileDir: path.join(args.out, ".profile") });

try {
  for (const pair of args.pairs.split(",")) {
    const width = pair.includes("-390-") ? 390 : 1440;
    const height = pair.includes("-390-") ? 844 : 900;
    const rightName = Object.entries(renames).reduce((name, [from, to]) => name.replace(from, to), pair);
    const figure = (label, file) => `<figure style="margin:0">
      <figcaption style="height:32px">${label}</figcaption>
      <div style="width:${width}px;height:${height}px;overflow:hidden;outline:1px solid #C9C9CC">
        <img src="${dataUrl(file)}" width="${width}" style="display:block">
      </div></figure>`;
    const html = `<!doctype html><html><body style="margin:0;background:#EDEBE7;font:500 16px/1.4 -apple-system,Helvetica,sans-serif;color:#1C1E22">
      <div style="display:flex;gap:32px;padding:24px 32px">
        ${figure(`${args["left-label"] ?? "Left"} · ${pair}`, path.join(args.left, `${pair}.png`))}
        ${figure(`${args["right-label"] ?? "Right"} · ${rightName}`, path.join(args.right, `${rightName}.png`))}
      </div></body></html>`;
    const page = path.join(args.out, ".compare.html");
    writeFileSync(page, html);
    await browser.setCell({ width: width * 2 + 32 * 3, height: height + 32 + 48, theme: "light" });
    await browser.navigate(`file://${page}`);
    await sleep(400);
    await browser.shot(path.join(args.out, `${pair}.png`));
    console.log("wrote", pair);
  }
} finally {
  browser.close();
}
