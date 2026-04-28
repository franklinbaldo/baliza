import fs from 'fs';
import path from 'path';
import os from 'os';
import duckdb from 'duckdb';

const QUERIES_DIR = path.resolve('./src/queries');
const DATA_DIR = path.resolve('./public/data');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const db = new duckdb.Database(':memory:');

function processQmd(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  
  const outputMatch = content.match(/output:\s*([^\n]+)/);
  if (!outputMatch) return;
  const relativeOut = outputMatch[1].trim().replace(/^\//, ''); // remove leading slash
  const outputFile = path.join(process.cwd(), 'public', relativeOut);

  const sqlMatch = content.match(/```\{sql\}([\s\S]*?)```/);
  if (!sqlMatch) return;
  const sql = sqlMatch[1].trim();

  db.all(sql, (err, res) => {
    if (err) {
      console.error(`Error executing ${filePath}:`, err);
      return;
    }
    
    const formatMatch = content.match(/format:\s*([^\n]+)/);
    const format = formatMatch ? formatMatch[1].trim() : 'array';
    const finalData = format === 'object' && res.length > 0 ? res[0] : res;
    
    fs.mkdirSync(path.dirname(outputFile), { recursive: true });
    fs.writeFileSync(outputFile, JSON.stringify(finalData, null, 2));
    console.log(`Generated ${outputFile}`);
  });
}

const IA_MANIFEST_CSV_URL = process.env.IA_MANIFEST_CSV_URL ?? 'https://archive.org/download/baliza-pncp-manifest/manifest.csv';

async function loadManifest() {
  const csvPath = process.env.BALIZA_MANIFEST_FIXTURE ?? IA_MANIFEST_CSV_URL;
  let filePath = csvPath;
  if (csvPath.startsWith('http')) {
    const resp = await fetch(csvPath);
    if (!resp.ok) throw new Error(`manifest fetch failed: ${resp.status} ${resp.url}`);
    const csv = await resp.text();
    filePath = path.join(os.tmpdir(), 'baliza-manifest.csv');
    fs.writeFileSync(filePath, csv, 'utf-8');
  }
  await new Promise((resolve, reject) =>
    db.run(
      `CREATE TABLE manifest AS SELECT * FROM read_csv_auto('${filePath}', header=true)`,
      (err) => (err ? reject(err) : resolve()),
    ),
  );
}

async function ensureHttpfs() {
  if (process.env.BALIZA_MANIFEST_FIXTURE) return;
  await new Promise((resolve, reject) =>
    db.run('LOAD httpfs;', (err) => {
      if (!err) return resolve();
      db.run('INSTALL httpfs; LOAD httpfs;', (e2) => (e2 ? reject(e2) : resolve()));
    }),
  );
}

async function build() {
  if (!fs.existsSync(QUERIES_DIR)) fs.mkdirSync(QUERIES_DIR, { recursive: true });
  await loadManifest();
  await ensureHttpfs();
  processFiles();
}

function processFiles() {
  const files = fs.readdirSync(QUERIES_DIR).filter(f => f.endsWith('.qmd'));
  for (const file of files) {
    processQmd(path.join(QUERIES_DIR, file));
  }
}

build().catch(err => {
  console.error("Build failed:", err);
  process.exit(1);
});
