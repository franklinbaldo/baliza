import { chromium } from 'playwright';
import { mkdir, writeFile } from 'node:fs/promises';

const url = process.argv[2] ?? 'http://127.0.0.1:4321/baliza/publicacoes';
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
let tableRows = 0;

while (Date.now() - startedAt < deadlineMs) {
  bodyText = await page.locator('body').innerText();
  tableRows = await page.locator('.breakdown table tbody tr').count();

  if (bodyText.includes('Erro ao consultar parquet')) {
    state = 'remote-data-unavailable';
    break;
  }

  if (
    bodyText.includes('Nenhum parquet disponível no manifesto.') ||
    bodyText.includes('Sem dados de modalidade ainda.')
  ) {
    state = 'no-data';
    break;
  }

  if (bodyText.includes('Snapshot:') && tableRows > 0) {
    state = 'hydrated';
    break;
  }

  await page.waitForTimeout(pollMs);
}

const desktopFilename = `publicacoes-${state}-1280x900.png`;
await page.screenshot({ path: `${outputDir}/${desktopFilename}`, fullPage: true });

await page.setViewportSize({ width: 390, height: 844 });
await page.waitForTimeout(250);
const narrowFilename = `publicacoes-${state}-390x844.png`;
await page.screenshot({ path: `${outputDir}/${narrowFilename}`, fullPage: true });

const statusLine = bodyText
  .split('\n')
  .map((line) => line.trim())
  .find((line) =>
    line.startsWith('Snapshot:') ||
    line.includes('Erro ao consultar parquet') ||
    line.includes('Nenhum parquet disponível no manifesto') ||
    line.includes('Sem dados de modalidade ainda') ||
    line.includes('Buscando')
  ) ?? null;

const evidence = {
  route: '/publicacoes',
  url,
  state,
  screenshots: [
    { file: desktopFilename, viewport: { width: 1280, height: 900 } },
    { file: narrowFilename, viewport: { width: 390, height: 844 } },
  ],
  waited_ms: Date.now() - startedAt,
  table_rows: tableRows,
  final_status_excerpt: statusLine,
  page_errors: pageErrors,
  console_tail: consoleMessages.slice(-30),
};

await writeFile(`${outputDir}/capture-state.json`, `${JSON.stringify(evidence, null, 2)}\n`);

console.log(`capture_state=${state}`);
console.log(`capture_desktop=${desktopFilename}`);
console.log(`capture_narrow=${narrowFilename}`);
console.log(`capture_table_rows=${tableRows}`);
if (statusLine) console.log(`capture_status=${statusLine}`);

await browser.close();
