import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import duckdb from 'duckdb';
import { parse } from 'csv-parse/sync';

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
  const csvPath = process.env.BALIZA_MANIFEST_FIXTURE || IA_MANIFEST_CSV_URL;
  let csvText;

  if (csvPath.startsWith('http')) {
    const resp = await fetch(csvPath);
    if (!resp.ok) throw new Error(`manifest fetch failed: ${resp.status} ${resp.url}`);
    csvText = await resp.text();
  } else {
    csvText = fs.readFileSync(csvPath, 'utf-8');
  }

  const rawRows = parse(csvText, { columns: false, skip_empty_lines: true });
  const columns = rawRows.length > 0 ? rawRows[0] : [];
  const dataRows = rawRows.slice(1);

  const tableDef = columns.length > 0
    ? columns.map(c => `"${c}" VARCHAR`).join(', ')
    : 'data_particao VARCHAR, table_name VARCHAR, parquet_url VARCHAR';

  await new Promise((resolve, reject) => {
    db.run(`CREATE TABLE manifest (${tableDef})`, (err) => {
      if (err) { reject(err); } else { resolve(); }
    });
  });

  if (dataRows.length === 0) return;

  const stmt = db.prepare(`INSERT INTO manifest VALUES (${columns.map(() => '?').join(', ')})`);

  for (const row of dataRows) {
    await new Promise((resolve, reject) => {
      stmt.run(...row, (err) => {
        if (err) { reject(err); } else { resolve(); }
      });
    });
  }

  await new Promise((resolve, reject) => {
    stmt.finalize((err) => {
      if (err) { reject(err); } else { resolve(); }
    });
  });
}

async function ensureHttpfs() {
  if (process.env.BALIZA_MANIFEST_FIXTURE) return;
  await new Promise((resolve, reject) =>
    db.run('LOAD httpfs;', (err) => {
      if (!err) return resolve();
      db.run('INSTALL httpfs; LOAD httpfs;', (e2) => { if (e2) { reject(e2); } else { resolve(); } });
    }),
  );
}

async function build() {
  console.log("Generating frontend config...");
  execSync('python3 scripts/build_frontend_config.py', { stdio: 'inherit', cwd: '..' });

  // Refresh public/data/sync_stats.json from the live IA manifest so the
  // homepage's "Sinal do arquivo" tile reflects real numbers. Tolerate
  // failure (offline build, IA outage) — the existing JSON stays put.
  console.log("Refreshing sync_stats.json from IA manifest...");
  try {
    execSync('python3 scripts/build_sync_stats.py', { stdio: 'inherit', cwd: '..' });
  } catch (err) {
    console.warn(`sync_stats.json refresh skipped: ${err.message}`);
  }

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
