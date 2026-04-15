import fs from 'fs';
import path from 'path';
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

function build() {
  if (!fs.existsSync(QUERIES_DIR)) fs.mkdirSync(QUERIES_DIR, { recursive: true });
  
  // For the sake of this prototype, we'll create a fake manifest table
  // In production, this would read from the actual internet archive via httpfs extension
  db.run(`CREATE TABLE IF NOT EXISTS manifest AS 
          SELECT '2024-04-01' as date, 1754 as row_count, 0 as quarantine_count
          UNION ALL SELECT '2024-04-02', 2100, 3
          UNION ALL SELECT '2024-04-03', 1950, 0;`, 
  (err) => {
    if(err) console.error(err);
    const files = fs.readdirSync(QUERIES_DIR).filter(f => f.endsWith('.qmd'));
    for (const file of files) {
      processQmd(path.join(QUERIES_DIR, file));
    }
  });
}

build();
