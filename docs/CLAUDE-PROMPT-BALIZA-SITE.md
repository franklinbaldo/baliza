# 🤖 Prompt for Claude: Build baliza-site

**Copy and paste this entire document to a new Claude session to implement the baliza-site repository.**

---

## Your Task

I need you to build a complete Next.js static website for the **baliza-site** repository.

**Repository URL:** https://github.com/franklinbaldo/baliza-site
**Status:** Empty repository, needs initial implementation

---

## Context

baliza-site is the web frontend for visualizing Brazilian public procurement data. It's part of a two-repo architecture:

1. **baliza** (CLI) - Extracts data from PNCP API → exports Parquet files
2. **baliza-site** (this) - Static website that queries Parquet files in-browser using DuckDB-WASM

**Full architectural documentation:**
- https://github.com/franklinbaldo/baliza/blob/main/docs/ARCHITECTURE.md
- https://github.com/franklinbaldo/baliza/blob/main/docs/baliza-site-setup-guide.md
- https://github.com/franklinbaldo/baliza/blob/main/docs/BALIZA-SITE-IMPLEMENTATION-BRIEF.md

---

## Requirements

### Technology Stack (REQUIRED)

- **Framework:** Next.js 14 with App Router, TypeScript, Tailwind CSS
- **Database:** DuckDB-WASM (runs in browser)
- **Data Format:** Apache Parquet files
- **UI Components:** Recharts (charts), TanStack Table (tables), Lucide React (icons)
- **Hosting:** GitHub Pages (static export)
- **Language:** Portuguese (pt-BR)

### Features to Implement

1. **Homepage (Dashboard)**
   - Overall statistics cards (total contracts, organizations, value)
   - Top 10 organizations by contract value
   - Monthly trends chart

2. **Search Page**
   - Filter by date range, value range, organization
   - Results table with sorting
   - Export filtered results

3. **Downloads Page**
   - List available Parquet files by year/month
   - Download individual files
   - Data coverage status

4. **Documentation Page**
   - Data dictionary (PNCP schema)
   - SQL query examples
   - How to use the site

5. **About Page**
   - Project description
   - Links to Baliza CLI repo
   - Open source licenses

### GitHub Actions (REQUIRED)

1. **sync-data.yml** - Daily job that downloads latest Parquet files from baliza CLI releases
2. **deploy.yml** - Builds Next.js and deploys to GitHub Pages

---

## Data Schema

Parquet files contain contracts with this structure:

```typescript
{
  numeroControlePNCP: string;           // Primary key (CNPJ-2-seq/year)
  dataPublicacaoPncp: string;          // Publication date
  dataVigenciaInicio: string;          // Contract start date
  dataVigenciaFim: string;             // Contract end date
  objetoContrato: string;              // Contract description
  valorInicial: number;                // Initial value (BRL)
  "orgaoEntidade.cnpj": string;        // Organization CNPJ
  "orgaoEntidade.razaoSocial": string; // Organization name
  "fornecedor.cpfCnpj": string;        // Supplier CNPJ/CPF
  "fornecedor.nome": string;           // Supplier name
  modalidadeId: number;                // Procurement modality ID
  modalidadeNome: string;              // Procurement modality name
}
```

**Note:** Field names with dots (e.g., `orgaoEntidade.cnpj`) must be quoted in SQL: `"orgaoEntidade.cnpj"`

---

## Project Structure

```
baliza-site/
├── app/
│   ├── layout.tsx              # Root layout
│   ├── page.tsx                # Dashboard (homepage)
│   ├── busca/page.tsx          # Search
│   ├── downloads/page.tsx      # Downloads
│   ├── docs/page.tsx           # Documentation
│   └── sobre/page.tsx          # About
├── components/
│   ├── Header.tsx
│   ├── Footer.tsx
│   ├── StatCard.tsx
│   ├── DataTable.tsx
│   └── FilterPanel.tsx
├── lib/
│   ├── duckdb.ts               # DuckDB-WASM wrapper
│   ├── queries.ts              # SQL query templates
│   └── utils.ts
├── types/
│   └── contrato.ts
├── .github/workflows/
│   ├── sync-data.yml           # Download Parquet daily
│   └── deploy.yml              # Build and deploy
├── public/data/                # Parquet files (gitignored)
├── next.config.js              # Static export config
└── README.md
```

---

## Implementation Steps

### Step 1: Initialize Project

```bash
cd baliza-site
npx create-next-app@latest . --typescript --tailwind --app --no-src-dir
npm install @duckdb/duckdb-wasm apache-arrow @tanstack/react-table recharts lucide-react clsx tailwind-merge
```

### Step 2: Configure next.config.js

```javascript
module.exports = {
  output: 'export',
  images: { unoptimized: true },
  async headers() {
    return [{
      source: '/:path*',
      headers: [
        { key: 'Cross-Origin-Embedder-Policy', value: 'require-corp' },
        { key: 'Cross-Origin-Opener-Policy', value: 'same-origin' },
      ],
    }];
  },
};
```

### Step 3: Create DuckDB Integration (lib/duckdb.ts)

```typescript
import * as duckdb from '@duckdb/duckdb-wasm';

let db: duckdb.AsyncDuckDB | null = null;

export async function initDuckDB() {
  if (db) return db;

  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );

  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule);

  return db;
}

export async function query<T = any>(sql: string): Promise<T[]> {
  const database = await initDuckDB();
  const conn = await database.connect();
  const result = await conn.query(sql);
  return result.toArray() as T[];
}
```

### Step 4: Create SQL Queries (lib/queries.ts)

```typescript
export const queries = {
  getStats: `
    SELECT
      COUNT(*) as total_contratos,
      COUNT(DISTINCT "orgaoEntidade.cnpj") as total_orgaos,
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
};
```

### Step 5: Build UI Components

- Header with navigation
- StatCard for metrics
- DataTable for contracts
- FilterPanel for search
- Charts for trends

### Step 6: Create GitHub Actions

**.github/workflows/sync-data.yml:**
```yaml
name: Sync Data
on:
  schedule:
    - cron: '0 6 * * *'
  workflow_dispatch:

permissions:
  contents: write

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Download Parquet
        run: |
          gh release download --repo franklinbaldo/baliza --pattern "*.parquet" --dir public/data/contratos
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      - name: Commit
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add public/data/
          git diff --staged --quiet || git commit -m "chore: sync data" && git push
```

**.github/workflows/deploy.yml:**
```yaml
name: Deploy
on:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

jobs:
  build:
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

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - uses: actions/deploy-pages@v4
        id: deployment
```

### Step 7: Enable GitHub Pages

Go to repository Settings → Pages → Source: GitHub Actions

---

## Important Notes

1. **Field Names:** PNCP fields with dots MUST be quoted in SQL: `"orgaoEntidade.cnpj"`
2. **Language:** All UI text in Portuguese (pt-BR)
3. **Mobile First:** Responsive design required
4. **Loading States:** Show spinners during data loading
5. **Error Handling:** Graceful fallbacks if data missing
6. **Performance:** Lazy load Parquet files, paginate results

---

## Testing Locally

Since Parquet files don't exist yet in baliza releases:

1. Generate sample data from baliza CLI:
   ```bash
   cd ../baliza
   uv run baliza extract
   uv run baliza export --table contratos --out data/contratos
   ```

2. Copy to baliza-site:
   ```bash
   cp -r data/contratos ../baliza-site/public/data/
   ```

3. Create manifest file:
   ```bash
   cd ../baliza-site
   find public/data -name "*.parquet" | jq -R -s 'split("\n") | map(select(length > 0))' > public/data/manifest.json
   ```

---

## Acceptance Criteria

✅ Homepage loads and displays statistics
✅ DuckDB-WASM successfully queries Parquet files
✅ Search page filters work correctly
✅ GitHub Actions sync data daily
✅ Site deploys to GitHub Pages
✅ Mobile responsive
✅ TypeScript with no errors
✅ README with clear setup instructions

---

## Your Deliverables

Please implement:

1. **Complete Next.js project** with all pages and components
2. **DuckDB integration** working in browser
3. **GitHub Actions** for data sync and deployment
4. **README.md** with setup instructions
5. **Commit all files** to the main branch
6. **Push to GitHub** so Actions can run

---

## Questions to Consider

- How to discover available Parquet files dynamically?
- Should we cache query results in localStorage?
- How to handle large datasets (millions of rows)?
- Should we add a SQL query playground?

---

## Reference Documentation

- **Full technical brief:** https://github.com/franklinbaldo/baliza/blob/main/docs/BALIZA-SITE-IMPLEMENTATION-BRIEF.md
- **Architecture:** https://github.com/franklinbaldo/baliza/blob/main/docs/ARCHITECTURE.md
- **Setup guide:** https://github.com/franklinbaldo/baliza/blob/main/docs/baliza-site-setup-guide.md
- **DuckDB-WASM:** https://duckdb.org/docs/api/wasm
- **Next.js Static Export:** https://nextjs.org/docs/app/building-your-application/deploying/static-exports

---

## Let's Get Started!

Clone the repo and begin implementation:

```bash
git clone https://github.com/franklinbaldo/baliza-site.git
cd baliza-site
```

**Please implement the complete baliza-site as specified above. Ask questions if anything is unclear!**
