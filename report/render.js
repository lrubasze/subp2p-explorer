// Render-check the built report before publishing — never ship a chart unlooked-at.
//
//   cd report && bun install && bun run render.js
//
// Writes to report/out/: full-page screenshots in light and dark, plus 2x PNGs of
// each tagged section (used as the embedded figures in the sdk issue).
//
// The installed playwright wants a newer browser build than the ones cached in
// ~/.cache/ms-playwright, so the executable path is pinned explicitly.
import { chromium } from 'playwright';
import { readFileSync, writeFileSync, mkdirSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, '..');
const OUT = join(HERE, 'out');
mkdirSync(OUT, { recursive: true });

const fragment = readFileSync(join(REPO, 'peer-capacity-report.html'), 'utf8');
// The fragment has no <html>/<head>/<body> (the artifact publisher wraps it); a
// browser loading it from file:// needs the doctype or it falls into quirks mode.
const wrapped = `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"></head><body>${fragment}</body></html>`;
writeFileSync(join(OUT, 'index.html'), wrapped);

const browser = await chromium.launch({
  executablePath: join(process.env.HOME, '.cache/ms-playwright/chromium-1228/chrome-linux64/chrome'),
});

let failed = false;
for (const scheme of ['light', 'dark']) {
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 900 }, colorScheme: scheme });
  const page = await ctx.newPage();
  page.on('pageerror', e => { failed = true; console.error(`PAGE ERROR (${scheme}):`, e.message); });
  page.on('console', m => { if (m.type() === 'error') { failed = true; console.error(`CONSOLE ERROR (${scheme}):`, m.text()); } });
  await page.goto(`file://${join(OUT, 'index.html')}`);
  await page.waitForTimeout(400);
  await page.screenshot({ path: join(OUT, `full-${scheme}.png`), fullPage: true });
  await ctx.close();
}

const ctx = await browser.newContext({ viewport: { width: 1080, height: 900 }, colorScheme: 'light', deviceScaleFactor: 2 });
const page = await ctx.newPage();
await page.goto(`file://${join(OUT, 'index.html')}`);
await page.waitForTimeout(400);
for (const id of ['sec-threshold', 'sec-cost', 'sec-long', 'sec-mix']) {
  const el = page.locator(`#${id}`);
  if (await el.count()) await el.screenshot({ path: join(OUT, `${id}.png`) });
}
// card-level figures for embedding in the sdk issue (chart + its own caption, no prose)
const figs = { threshold: '#c-threshold', cpu: '#c-cpu', rss: '#c-rss', 'drift-long': '#c-long', 'rss-long': '#c-rsstrace' };
for (const [name, sel] of Object.entries(figs)) {
  const card = page.locator('.card', { has: page.locator(sel) });
  if (await card.count()) await card.screenshot({ path: join(OUT, `fig-${name}.png`) });
}
await ctx.close();
await browser.close();
if (failed) { console.error('render had page errors'); process.exit(1); }
console.log('rendered to', OUT, '— now LOOK at the screenshots');
