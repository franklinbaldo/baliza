# Baliza Site - Quick Start Guide

**For Claude or any developer building the baliza-site repository**

---

## 🎯 Mission

Build a static Next.js website that visualizes Brazilian public procurement data using DuckDB-WASM to query Parquet files in the browser.

**Repo:** https://github.com/franklinbaldo/baliza-site (empty, needs implementation)

---

## 📦 Tech Stack

```
Next.js 14 + TypeScript + Tailwind + DuckDB-WASM + Parquet + GitHub Pages
```

**Key Dependencies:**
- `@duckdb/duckdb-wasm` - In-browser SQL database
- `apache-arrow` - Parquet file reading
- `@tanstack/react-table` - Data tables
- `recharts` - Charts
- `lucide-react` - Icons

---

## 🏗️ Architecture

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   PNCP API   │ ───▶ │  Baliza CLI  │ ───▶ │ baliza-site  │
│  (Source)    │      │  (Extract)   │      │  (Visualize) │
└──────────────┘      └──────────────┘      └──────────────┘
                             │
                             ▼
                      Parquet Files
                    (GitHub Release)
                             │
                             ▼
                      ┌──────────────┐
                      │ GitHub Pages │
                      │  (Static)    │
                      └──────────────┘
                             │
                             ▼
                      ┌──────────────┐
                      │ User Browser │
                      │ (DuckDB-WASM)│
                      └──────────────┘
```

**Data Flow:**
1. Baliza CLI extracts contracts from PNCP API
2. Exports to Parquet files (partitioned by year/month)
3. Publishes as GitHub Release assets
4. baliza-site syncs files daily (GitHub Actions)
5. User browser queries Parquet with DuckDB-WASM

---

## 📝 Pages to Build

| Path | Purpose | Key Features |
|------|---------|--------------|
| `/` | Dashboard | Stats cards, top orgs, trends chart |
| `/busca` | Search | Filters (date, value, org), results table |
| `/downloads` | Downloads | List Parquet files, download links |
| `/docs` | Documentation | Data dictionary, SQL examples |
| `/sobre` | About | Project info, links to CLI repo |

---

## 🗄️ Data Schema (Parquet)

```typescript
interface Contrato {
  numeroControlePNCP: string;           // Primary key
  dataPublicacaoPncp: string;          // Publication date
  valorInicial: number;                // Contract value (BRL)
  objetoContrato: string;              // Description
  "orgaoEntidade.cnpj": string;        // Org CNPJ (quoted!)
  "orgaoEntidade.razaoSocial": string; // Org name (quoted!)
  "fornecedor.cpfCnpj": string;        // Supplier ID (quoted!)
  "fornecedor.nome": string;           // Supplier name (quoted!)
}
```

**⚠️ CRITICAL:** Field names with dots MUST be quoted in SQL:
```sql
-- ❌ Wrong
SELECT orgaoEntidade.cnpj FROM contratos

-- ✅ Correct
SELECT "orgaoEntidade.cnpj" FROM contratos
```

---

## 🚀 Quick Setup

```bash
# 1. Initialize Next.js
cd baliza-site
npx create-next-app@latest . --typescript --tailwind --app --no-src-dir

# 2. Install deps
npm install @duckdb/duckdb-wasm apache-arrow @tanstack/react-table recharts lucide-react clsx

# 3. Configure static export (next.config.js)
output: 'export'
images: { unoptimized: true }

# 4. Add CORS headers for WASM
async headers() {
  return [{
    source: '/:path*',
    headers: [
      { key: 'Cross-Origin-Embedder-Policy', value: 'require-corp' },
      { key: 'Cross-Origin-Opener-Policy', value: 'same-origin' },
    ],
  }];
}

# 5. Build
npm run build  # Output to ./out/
```

---

## 💻 Key Code Snippets

### DuckDB Init (`lib/duckdb.ts`)

```typescript
import * as duckdb from '@duckdb/duckdb-wasm';

let db: duckdb.AsyncDuckDB | null = null;

export async function initDuckDB() {
  if (db) return db;

  const BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(BUNDLES);

  const worker = new Worker(
    URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`],
      { type: 'text/javascript' })
    )
  );

  db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule);

  return db;
}

export async function query<T>(sql: string): Promise<T[]> {
  const database = await initDuckDB();
  const conn = await database.connect();
  const result = await conn.query(sql);
  return result.toArray() as T[];
}
```

### Load Parquet Files

```typescript
// Create view from all Parquet files
export async function loadParquetFiles(files: string[]) {
  const database = await initDuckDB();
  const conn = await database.connect();

  const fileList = files.map(f => `'${f}'`).join(', ');
  await conn.query(`
    CREATE OR REPLACE VIEW contratos AS
    SELECT * FROM read_parquet([${fileList}])
  `);
}

// Usage
await loadParquetFiles([
  '/data/contratos/ano=2024/mes=10/data.parquet',
  '/data/contratos/ano=2024/mes=11/data.parquet',
]);
```

### Example Queries (`lib/queries.ts`)

```typescript
export const queries = {
  getStats: `
    SELECT
      COUNT(*) as total,
      SUM(valorInicial) as valor_total
    FROM contratos
  `,

  getTopOrgaos: (limit = 10) => `
    SELECT
      "orgaoEntidade.razaoSocial" as orgao,
      COUNT(*) as num_contratos,
      SUM(valorInicial) as valor_total
    FROM contratos
    GROUP BY "orgaoEntidade.razaoSocial"
    ORDER BY valor_total DESC
    LIMIT ${limit}
  `,

  search: (filters: {cnpj?: string, dataMin?: string}) => `
    SELECT * FROM contratos
    WHERE 1=1
      ${filters.cnpj ? `AND "orgaoEntidade.cnpj" = '${filters.cnpj}'` : ''}
      ${filters.dataMin ? `AND dataPublicacaoPncp >= '${filters.dataMin}'` : ''}
    LIMIT 100
  `,
};
```

---

## ⚙️ GitHub Actions

### Sync Data (`.github/workflows/sync-data.yml`)

```yaml
name: Sync Data
on:
  schedule:
    - cron: '0 6 * * *'  # Daily 6 AM UTC
  workflow_dispatch:

permissions:
  contents: write

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Download Parquet from baliza
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          mkdir -p public/data/contratos
          gh release download \
            --repo franklinbaldo/baliza \
            --pattern "*.parquet" \
            --dir public/data/contratos \
            --clobber

      - name: Commit changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add public/data/
          git diff --staged --quiet || \
            (git commit -m "chore: sync data $(date -I)" && git push)
```

### Deploy (`.github/workflows/deploy.yml`)

```yaml
name: Deploy to GitHub Pages
on:
  push:
    branches: [main]

permissions:
  contents: read
  pages: write
  id-token: write

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'
      - run: npm ci
      - run: npm run build
      - uses: actions/upload-pages-artifact@v3
        with:
          path: ./out
      - uses: actions/deploy-pages@v4
```

**Enable in Settings → Pages → Source: GitHub Actions**

---

## 📋 Checklist

- [ ] Initialize Next.js with TypeScript + Tailwind
- [ ] Install DuckDB-WASM + dependencies
- [ ] Configure static export + CORS headers
- [ ] Create `lib/duckdb.ts` wrapper
- [ ] Create `lib/queries.ts` with SQL templates
- [ ] Build pages: Dashboard, Search, Downloads, Docs, About
- [ ] Create UI components: Header, Footer, StatCard, DataTable
- [ ] Add GitHub Actions: sync-data.yml, deploy.yml
- [ ] Enable GitHub Pages in repo settings
- [ ] Write README.md
- [ ] Test with sample Parquet data
- [ ] Commit and push to trigger deployment

---

## 🎨 Design Guidelines

- **Colors:** Blue (primary), Green (accent), Orange (metrics)
- **Language:** Portuguese (pt-BR)
- **Mobile-first:** Responsive from 320px
- **Loading states:** Spinners for async operations
- **Error handling:** Graceful fallbacks

---

## 🧪 Testing Locally

```bash
# Generate sample data in baliza CLI repo
cd ../baliza
uv run baliza extract
uv run baliza export --table contratos --out data/contratos

# Copy to baliza-site
cp -r data/contratos ../baliza-site/public/data/

# Create manifest
cd ../baliza-site
find public/data -name "*.parquet" | \
  jq -R -s 'split("\n") | map(select(length > 0))' > public/data/manifest.json

# Run dev server
npm run dev
```

---

## 📚 Reference Docs

| Document | Purpose |
|----------|---------|
| [ARCHITECTURE.md](https://github.com/franklinbaldo/baliza/blob/main/docs/ARCHITECTURE.md) | Two-repo architecture |
| [BALIZA-SITE-IMPLEMENTATION-BRIEF.md](https://github.com/franklinbaldo/baliza/blob/main/docs/BALIZA-SITE-IMPLEMENTATION-BRIEF.md) | Full technical spec |
| [baliza-site-setup-guide.md](https://github.com/franklinbaldo/baliza/blob/main/docs/baliza-site-setup-guide.md) | Detailed setup |
| [DuckDB-WASM Docs](https://duckdb.org/docs/api/wasm) | Database API |
| [Next.js Static Export](https://nextjs.org/docs/app/building-your-application/deploying/static-exports) | Framework docs |

---

## 🎯 Success Criteria

✅ Site loads at https://franklinbaldo.github.io/baliza-site
✅ Dashboard displays contract statistics
✅ Search filters and displays results
✅ DuckDB-WASM queries Parquet in browser
✅ Data syncs daily via GitHub Actions
✅ Mobile responsive
✅ TypeScript with no errors
✅ README with clear instructions

---

## 🚀 Ready to Build!

```bash
git clone https://github.com/franklinbaldo/baliza-site.git
cd baliza-site
# Start coding! 🎨
```

**Questions? Check the full docs or ask!**
