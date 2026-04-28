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

async function build() {
  if (!fs.existsSync(QUERIES_DIR)) fs.mkdirSync(QUERIES_DIR, { recursive: true });
  
  let csvPath = process.env.BALIZA_MANIFEST_FIXTURE;
  let filePath = csvPath;

  if (!csvPath) {
    csvPath = IA_MANIFEST_CSV_URL;
  }

  if (csvPath.startsWith('http')) {
    const resp = await fetch(csvPath);
    if (!resp.ok) {
      throw new Error(`manifest fetch failed: ${resp.status} ${resp.url}`);
    }
    const csv = await resp.text();
    filePath = path.join(os.tmpdir(), 'baliza-manifest.csv');
    fs.writeFileSync(filePath, csv, 'utf-8');
  }

  db.run(`CREATE TABLE manifest AS SELECT * FROM read_csv_auto('${filePath}', header=true)`, (err) => {
    if (err) {
      console.error('Error creating manifest table:', err);
      return;
    }

    // We should ensure httpfs is loaded for the Parquet parts
    // Conditionally load/install httpfs if not in fixture mode
    if (!process.env.BALIZA_MANIFEST_FIXTURE) {
       db.run('LOAD httpfs;', (errLoad) => {
          if (errLoad) {
             db.run('INSTALL httpfs; LOAD httpfs;', (errInstall) => {
                if (errInstall) {
                   console.error('Failed to install/load httpfs', errInstall);
                } else {
                   processFiles();
                }
             });
          } else {
             processFiles();
          }
       });
    } else {
       processFiles();
    }
  });
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
