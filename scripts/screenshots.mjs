// Drives Playwright over the route list produced by `mix screenshots` and
// writes tmp/shots/<route>-<viewport>.png at desktop and mobile sizes.
// Env: BASE_URL, ROUTES (JSON array of paths), OUT_DIR.
import { chromium } from "playwright";
import { mkdirSync } from "node:fs";
import { join } from "node:path";

const base = process.env.BASE_URL;
const routes = JSON.parse(process.env.ROUTES || "[]");
const outDir = process.env.OUT_DIR || "tmp/shots";
const viewports = [
  { name: "1280x800", width: 1280, height: 800 },
  { name: "390x844", width: 390, height: 844 },
];

if (!base || routes.length === 0) {
  console.error("BASE_URL and ROUTES are required");
  process.exit(2);
}

mkdirSync(outDir, { recursive: true });
const slug = (p) => (p === "/" ? "root" : p.replace(/^\//, "").replace(/[^a-zA-Z0-9]+/g, "_"));

const browser = await chromium.launch();
const problems = [];

for (const vp of viewports) {
  const context = await browser.newContext({ viewport: { width: vp.width, height: vp.height } });
  const page = await context.newPage();
  page.on("pageerror", (e) => problems.push(`${vp.name} pageerror: ${e.message}`));
  page.on("console", (m) => {
    if (m.type() === "error") problems.push(`${vp.name} console.error: ${m.text()}`);
  });

  for (const path of routes) {
    const url = base + path;
    const file = join(outDir, `${slug(path)}-${vp.name}.png`);
    try {
      const resp = await page.goto(url, { waitUntil: "networkidle", timeout: 30000 });
      if (!resp || resp.status() !== 200) {
        problems.push(`${vp.name} ${path}: HTTP ${resp ? resp.status() : "no response"}`);
      }
      // Give LiveView a moment to connect; not every page is a LiveView.
      await page
        .waitForSelector("[data-phx-main].phx-connected", { timeout: 3000 })
        .catch(() => {});
      await page.screenshot({ path: file, fullPage: true });
      console.log(`wrote ${file}`);
    } catch (e) {
      problems.push(`${vp.name} ${path}: ${e.message}`);
    }
  }
  await context.close();
}

await browser.close();

if (problems.length > 0) {
  console.error("\nscreenshot problems:");
  for (const p of problems) console.error(`  - ${p}`);
  process.exit(1);
}
console.log(`\n${routes.length * viewports.length} screenshots in ${outDir}`);
