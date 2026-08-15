import { chromium } from 'playwright';
import { mkdir, writeFile } from 'node:fs/promises';

const url = process.argv[2] ?? 'http://127.0.0.1:4321/baliza/comparar?ibge=3550308&objeto=merenda';
const outputDir = process.argv[3] ?? '../visual-evidence';
const deadlineMs = 60_000;
const pollMs = 500;

await mkdir(outputDir, { recursive: true });

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
const consoleMessages = [];
const pageErrors = [];

page.on('console', (message) => consoleMessages.push(`${message.type()}: ${message.text()}`));
page.on('pageerror', (error) => pageErrors.push(error.message));

await page.goto(url, { waitUntil: 'domcontentloaded' });

const startedAt = Date.now();
let state = 'incomplete';
let bodyText = '';
let scenarioCards = 0;

while (Date.now() - startedAt < deadlineMs) {
  bodyText = await page.locator('body').innerText();
  scenarioCards = await page.locator('[data-testid="comparar-peer-item"]').count();

  if (bodyText.includes('Não foi possível buscar dados')) {
    state = 'remote-data-unavailable';
    break;
  }

  if (
    bodyText.includes('Gasto Per-Capita') &&
    bodyText.includes('Referências simuladas') &&
    bodyText.includes('Não representam municípios reais') &&
    scenarioCards === 3
  ) {
    state = 'hydrated-with-explicit-simulation';
    break;
  }

  await page.waitForTimeout(pollMs);
}

const desktopFilename = `comparar-${state}-1280x900.png`;
await page.screenshot({ path: `${outputDir}/${desktopFilename}`, fullPage: true });

await page.setViewportSize({ width: 390, height: 844 });
await page.waitForTimeout(250);
const narrowFilename = `comparar-${state}-390x844.png`;
await page.screenshot({ path: `${outputDir}/${narrowFilename}`, fullPage: true });

const evidence = {
  route: '/comparar?ibge=3550308&objeto=merenda',
  url,
  state,
  screenshots: [
    { file: desktopFilename, viewport: { width: 1280, height: 900 } },
    { file: narrowFilename, viewport: { width: 390, height: 844 } },
  ],
  waited_ms: Date.now() - startedAt,
  scenario_cards: scenarioCards,
  simulation_disclaimer_visible: bodyText.includes('Não representam municípios reais'),
  page_errors: pageErrors,
  console_tail: consoleMessages.slice(-30),
};

await writeFile(`${outputDir}/comparar-capture-state.json`, `${JSON.stringify(evidence, null, 2)}\n`);

console.log(`capture_state=${state}`);
console.log(`capture_desktop=${desktopFilename}`);
console.log(`capture_narrow=${narrowFilename}`);
console.log(`capture_scenario_cards=${scenarioCards}`);

await browser.close();
