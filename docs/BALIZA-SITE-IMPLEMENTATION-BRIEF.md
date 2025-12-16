# Baliza Site - Implementation Brief

**Repository:** https://github.com/franklinbaldo/baliza-site
**Parent Project:** https://github.com/franklinbaldo/baliza
**Status:** New repository - needs initial setup
**Date:** 2025-10-30

---

## 🎯 Project Overview

**baliza-site** is a static website that provides a user-friendly interface for exploring Brazilian public procurement data extracted by the [Baliza CLI](https://github.com/franklinbaldo/baliza).

### Key Characteristics

- **100% Static Site** - No backend server required
- **DuckDB-WASM** - SQL queries run directly in the browser
- **Zero-cost Hosting** - Deploy to GitHub Pages or Vercel
- **Auto-updating Data** - Syncs daily with Baliza CLI exports
- **Fast & Scalable** - CDN-delivered static assets

---

## 📐 Architecture Context

This site is part of a two-repository architecture:

| Repository | Role | Status |
|-----------|------|--------|
| **franklinbaldo/baliza** | CLI for data extraction from PNCP API | ✅ Active |
| **franklinbaldo/baliza-site** | Web UI for data visualization | 🆕 This repo |

**Data Flow:**
```
PNCP API → Baliza CLI → Parquet Files → GitHub Release → baliza-site → User Browser
```

**Integration Points:**
- Baliza CLI exports Parquet files partitioned by year/month
- Files are published as GitHub Release assets
- baliza-site downloads and serves these files
- DuckDB-WASM queries Parquet directly in browser

For complete architectural documentation, see:
- https://github.com/franklinbaldo/baliza/blob/main/docs/ARCHITECTURE.md
- https://github.com/franklinbaldo/baliza/blob/main/docs/baliza-site-setup-guide.md

---

## 🎯 Project Goals

### Primary Objectives

1. **Accessibility** - Make PNCP contract data easily searchable by journalists, researchers, and citizens
2. **Performance** - Handle millions of records with fast query response
3. **Transparency** - Open source, auditable code
4. **Zero Cost** - No hosting fees, entirely static
5. **Freshness** - Daily automatic updates from Baliza CLI

### User Stories

**As a journalist**, I want to:
- Search for contracts by organization CNPJ
- Filter contracts by date range and value
- Download filtered results as CSV
- See statistics about contracts over time

**As a researcher**, I want to:
- Download complete datasets in Parquet format
- Run custom SQL queries in the browser
- See data coverage status
- Access documentation about the data schema

**As a citizen**, I want to:
- Understand public spending trends
- Find contracts from specific government agencies
- See which companies win the most contracts
- Access data without technical barriers

---

## 🛠 Technology Stack

### Required Technologies

| Component | Technology | Justification |
|-----------|-----------|---------------|
| **Framework** | Next.js 14 (App Router) | Modern React framework with static export |
| **Language** | TypeScript | Type safety and better DX |
| **Styling** | Tailwind CSS | Rapid UI development |
| **Database** | DuckDB-WASM | SQL queries in browser, reads Parquet |
| **Data Format** | Apache Parquet | Columnar format, efficient compression |
| **Charts** | Recharts | React-native charts |
| **Tables** | TanStack Table | Powerful data tables |
| **Icons** | Lucide React | Clean, modern icon set |
| **Hosting** | GitHub Pages | Free, automatic from Actions |

### Package Dependencies

```json
{
  "dependencies": {
    "next": "^14.0.0",
    "react": "^18.0.0",
    "react-dom": "^18.0.0",
    "@duckdb/duckdb-wasm": "^1.28.0",
    "apache-arrow": "^14.0.0",
    "@tanstack/react-table": "^8.10.0",
    "recharts": "^2.10.0",
    "lucide-react": "^0.300.0",
    "clsx": "^2.0.0",
    "tailwind-merge": "^2.0.0"
  },
  "devDependencies": {
    "typescript": "^5.0.0",
    "@types/node": "^20.0.0",
    "@types/react": "^18.0.0",
    "tailwindcss": "^3.4.0",
    "autoprefixer": "^10.4.0",
    "postcss": "^8.4.0",
    "eslint": "^8.0.0",
    "eslint-config-next": "^14.0.0"
  }
}
```

---

## 📁 Project Structure

```
baliza-site/
├── README.md                      # Project overview and setup
├── package.json                   # Dependencies and scripts
├── next.config.js                 # Next.js configuration
├── tsconfig.json                  # TypeScript configuration
├── tailwind.config.ts             # Tailwind CSS configuration
├── postcss.config.js              # PostCSS configuration
├── .gitignore                     # Git ignore patterns
│
├── public/                        # Static assets
│   ├── data/                      # Parquet files (synced from Baliza)
│   │   └── contratos/
│   │       └── ano=YYYY/
│   │           └── mes=MM/
│   │               └── *.parquet
│   └── favicon.ico
│
├── app/                           # Next.js App Router
│   ├── layout.tsx                 # Root layout with header/footer
│   ├── page.tsx                   # Homepage (Dashboard)
│   ├── globals.css                # Global styles
│   │
│   ├── busca/                     # Search page
│   │   └── page.tsx
│   │
│   ├── downloads/                 # Download page
│   │   └── page.tsx
│   │
│   ├── sobre/                     # About page
│   │   └── page.tsx
│   │
│   └── docs/                      # Documentation
│       └── page.tsx
│
├── components/                    # React components
│   ├── ui/                        # Reusable UI components
│   │   ├── Button.tsx
│   │   ├── Card.tsx
│   │   ├── Input.tsx
│   │   └── Select.tsx
│   │
│   ├── DataTable.tsx              # Data table with sorting/filtering
│   ├── StatCard.tsx               # Statistics card
│   ├── ChartContainer.tsx         # Chart wrapper
│   ├── FilterPanel.tsx            # Filter controls
│   ├── Header.tsx                 # Site header/navigation
│   └── Footer.tsx                 # Site footer
│
├── lib/                           # Utility libraries
│   ├── duckdb.ts                  # DuckDB-WASM initialization
│   ├── queries.ts                 # SQL query templates
│   ├── data-loader.ts             # Load Parquet files
│   ├── format.ts                  # Number/date formatting
│   └── utils.ts                   # General utilities
│
├── types/                         # TypeScript type definitions
│   └── contrato.ts                # Contract data types
│
├── scripts/                       # Build/deployment scripts
│   └── sync-data.js               # Download Parquet from Baliza
│
├── .github/                       # GitHub Actions
│   └── workflows/
│       ├── deploy.yml             # Build and deploy to Pages
│       └── sync-data.yml          # Daily data sync
│
└── docs/                          # Additional documentation
    ├── data-dictionary.md         # PNCP data schema
    ├── sql-examples.md            # Example queries
    └── development.md             # Development guide
```

---

## 🎨 Design Guidelines

### Visual Identity

- **Primary Color**: Blue (#2563eb) - represents transparency
- **Secondary Color**: Green (#10b981) - represents data/growth
- **Accent Color**: Orange (#f59e0b) - for important metrics
- **Background**: Light gray (#f9fafb) for main areas
- **Cards**: White (#ffffff) with subtle shadow

### Typography

- **Headings**: Inter font, bold weights
- **Body**: Inter font, regular weight
- **Code/Numbers**: Monospace font for data tables

### Layout Principles

1. **Mobile-first** - Responsive from 320px up
2. **Clear hierarchy** - Important info stands out
3. **Generous whitespace** - Not cluttered
4. **Accessible** - WCAG AA compliance
5. **Fast loading** - Optimize images and fonts

---

## 🔧 Implementation Tasks

### Phase 1: Project Setup (Priority: Critical)

**Task 1.1: Initialize Next.js Project**
```bash
cd baliza-site
npx create-next-app@latest . --typescript --tailwind --app --no-src-dir
```

**Task 1.2: Install Dependencies**
```bash
npm install @duckdb/duckdb-wasm apache-arrow @tanstack/react-table recharts lucide-react clsx tailwind-merge
```

**Task 1.3: Configure Next.js for Static Export**

Create `next.config.js`:
```javascript
/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',
  images: {
    unoptimized: true,
  },
  // Enable headers for WASM
  async headers() {
    return [
      {
        source: '/:path*',
        headers: [
          {
            key: 'Cross-Origin-Embedder-Policy',
            value: 'require-corp',
          },
          {
            key: 'Cross-Origin-Opener-Policy',
            value: 'same-origin',
          },
        ],
      },
    ];
  },
};

module.exports = nextConfig;
```

**Task 1.4: Setup TypeScript**

Ensure `tsconfig.json` includes:
```json
{
  "compilerOptions": {
    "target": "ES2020",
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "jsx": "preserve",
    "module": "esnext",
    "moduleResolution": "bundler",
    "resolveJsonModule": true,
    "allowJs": true,
    "strict": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "paths": {
      "@/*": ["./*"]
    }
  }
}
```

**Task 1.5: Create .gitignore**
```
# Dependencies
node_modules/
.pnp/
.pnp.js

# Next.js
/.next/
/out/

# Production
/build

# Testing
/coverage

# Misc
.DS_Store
*.pem

# Debug
npm-debug.log*
yarn-debug.log*
yarn-error.log*

# Local env files
.env*.local

# Vercel
.vercel

# TypeScript
*.tsbuildinfo
next-env.d.ts

# Parquet data (downloaded by CI)
public/data/**/*.parquet
```

---

### Phase 2: Core Infrastructure (Priority: Critical)

**Task 2.1: DuckDB-WASM Integration**

Create `lib/duckdb.ts`:
```typescript
import * as duckdb from '@duckdb/duckdb-wasm';

let db: duckdb.AsyncDuckDB | null = null;
let conn: duckdb.AsyncDuckDBConnection | null = null;

export async function initDuckDB(): Promise<duckdb.AsyncDuckDB> {
  if (db) return db;

  // Load DuckDB bundles
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  // Create worker
  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], {
      type: 'text/javascript',
    })
  );
  const worker = new Worker(worker_url);

  // Initialize DuckDB
  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule);

  return db;
}

export async function getConnection(): Promise<duckdb.AsyncDuckDBConnection> {
  if (conn) return conn;

  const database = await initDuckDB();
  conn = await database.connect();

  return conn;
}

export async function loadParquetFiles(files: string[]): Promise<void> {
  const connection = await getConnection();

  // Create view from all Parquet files
  const fileList = files.map((f) => `'${f}'`).join(', ');
  await connection.query(`
    CREATE OR REPLACE VIEW contratos AS
    SELECT * FROM read_parquet([${fileList}])
  `);
}

export async function query<T = any>(sql: string): Promise<T[]> {
  const connection = await getConnection();
  const result = await connection.query(sql);
  return result.toArray() as T[];
}

export async function close(): Promise<void> {
  if (conn) {
    await conn.close();
    conn = null;
  }
  if (db) {
    await db.terminate();
    db = null;
  }
}
```

**Task 2.2: SQL Query Library**

Create `lib/queries.ts`:
```typescript
export const queries = {
  // Get overall statistics
  getStats: `
    SELECT
      COUNT(*) as total_contratos,
      COUNT(DISTINCT "orgaoEntidade.cnpj") as total_orgaos,
      COUNT(DISTINCT "fornecedor.cpfCnpj") as total_fornecedores,
      SUM(valorInicial) as valor_total,
      AVG(valorInicial) as valor_medio,
      MIN(dataPublicacaoPncp) as data_inicio,
      MAX(dataPublicacaoPncp) as data_fim
    FROM contratos
  `,

  // Get top organizations by contract count
  getTopOrgaos: (limit = 10) => `
    SELECT
      "orgaoEntidade.razaoSocial" as orgao,
      "orgaoEntidade.cnpj" as cnpj,
      COUNT(*) as num_contratos,
      SUM(valorInicial) as valor_total
    FROM contratos
    WHERE "orgaoEntidade.razaoSocial" IS NOT NULL
    GROUP BY "orgaoEntidade.razaoSocial", "orgaoEntidade.cnpj"
    ORDER BY valor_total DESC
    LIMIT ${limit}
  `,

  // Get monthly contract trends
  getMonthlyTrends: `
    SELECT
      strftime(dataPublicacaoPncp, '%Y-%m') as mes,
      COUNT(*) as num_contratos,
      SUM(valorInicial) as valor_total
    FROM contratos
    WHERE dataPublicacaoPncp IS NOT NULL
    GROUP BY strftime(dataPublicacaoPncp, '%Y-%m')
    ORDER BY mes
  `,

  // Search by CNPJ
  searchByCNPJ: (cnpj: string) => `
    SELECT *
    FROM contratos
    WHERE "orgaoEntidade.cnpj" = '${cnpj}'
       OR "fornecedor.cpfCnpj" = '${cnpj}'
    ORDER BY dataPublicacaoPncp DESC
    LIMIT 100
  `,

  // Advanced filter
  filterContratos: (filters: {
    dataInicio?: string;
    dataFim?: string;
    valorMin?: number;
    valorMax?: number;
    orgao?: string;
  }) => {
    const where: string[] = [];

    if (filters.dataInicio) {
      where.push(`dataPublicacaoPncp >= '${filters.dataInicio}'`);
    }
    if (filters.dataFim) {
      where.push(`dataPublicacaoPncp <= '${filters.dataFim}'`);
    }
    if (filters.valorMin !== undefined) {
      where.push(`valorInicial >= ${filters.valorMin}`);
    }
    if (filters.valorMax !== undefined) {
      where.push(`valorInicial <= ${filters.valorMax}`);
    }
    if (filters.orgao) {
      where.push(
        `"orgaoEntidade.razaoSocial" ILIKE '%${filters.orgao}%'`
      );
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';

    return `
      SELECT
        numeroControlePNCP,
        dataPublicacaoPncp,
        "orgaoEntidade.razaoSocial" as orgao,
        "fornecedor.nome" as fornecedor,
        objetoContrato,
        valorInicial
      FROM contratos
      ${whereClause}
      ORDER BY dataPublicacaoPncp DESC
      LIMIT 100
    `;
  },
};
```

**Task 2.3: Data Types**

Create `types/contrato.ts`:
```typescript
export interface Contrato {
  numeroControlePNCP: string;
  dataPublicacaoPncp: string;
  dataVigenciaInicio: string;
  dataVigenciaFim: string;
  objetoContrato: string;
  valorInicial: number;
  orgaoEntidade: {
    cnpj: string;
    razaoSocial: string;
    esferaId: string;
  };
  fornecedor: {
    cpfCnpj: string;
    nome: string;
    tipoDocumento: string;
  };
  modalidadeId: number;
  modalidadeNome: string;
}

export interface Stats {
  total_contratos: number;
  total_orgaos: number;
  total_fornecedores: number;
  valor_total: number;
  valor_medio: number;
  data_inicio: string;
  data_fim: string;
}

export interface OrgaoStats {
  orgao: string;
  cnpj: string;
  num_contratos: number;
  valor_total: number;
}
```

---

### Phase 3: UI Components (Priority: High)

**Task 3.1: Root Layout**

Create `app/layout.tsx`:
```typescript
import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import './globals.css';
import Header from '@/components/Header';
import Footer from '@/components/Footer';

const inter = Inter({ subsets: ['latin'] });

export const metadata: Metadata = {
  title: 'Baliza - Dados do PNCP',
  description: 'Explorador de dados de contratos públicos do Brasil',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="pt-BR">
      <body className={inter.className}>
        <div className="min-h-screen flex flex-col">
          <Header />
          <main className="flex-grow">{children}</main>
          <Footer />
        </div>
      </body>
    </html>
  );
}
```

**Task 3.2: Header Component**

Create `components/Header.tsx`:
```typescript
import Link from 'next/link';
import { Database } from 'lucide-react';

export default function Header() {
  return (
    <header className="bg-blue-600 text-white shadow-lg">
      <div className="container mx-auto px-4 py-4">
        <div className="flex items-center justify-between">
          <Link href="/" className="flex items-center gap-2 text-2xl font-bold">
            <Database size={32} />
            <span>Baliza</span>
          </Link>

          <nav className="hidden md:flex gap-6">
            <Link href="/" className="hover:text-blue-200 transition">
              Dashboard
            </Link>
            <Link href="/busca" className="hover:text-blue-200 transition">
              Buscar
            </Link>
            <Link href="/downloads" className="hover:text-blue-200 transition">
              Downloads
            </Link>
            <Link href="/docs" className="hover:text-blue-200 transition">
              Docs
            </Link>
            <Link href="/sobre" className="hover:text-blue-200 transition">
              Sobre
            </Link>
          </nav>

          <a
            href="https://github.com/franklinbaldo/baliza"
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm hover:text-blue-200 transition"
          >
            GitHub
          </a>
        </div>
      </div>
    </header>
  );
}
```

**Task 3.3: Dashboard Page**

Create `app/page.tsx`:
```typescript
'use client';

import { useEffect, useState } from 'react';
import { initDuckDB, loadParquetFiles, query } from '@/lib/duckdb';
import { queries } from '@/lib/queries';
import { Stats, OrgaoStats } from '@/types/contrato';
import StatCard from '@/components/StatCard';
import { TrendingUp, Building2, Users, DollarSign } from 'lucide-react';

export default function HomePage() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [topOrgaos, setTopOrgaos] = useState<OrgaoStats[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);

        // Initialize DuckDB
        await initDuckDB();

        // Discover available Parquet files via manifest.json (Dynamic Discovery)
        const manifestRes = await fetch('/data/manifest.json');
        if (!manifestRes.ok) {
          throw new Error(`Failed to load data manifest: ${manifestRes.statusText}`);
        }

        const manifestFiles: string[] = await manifestRes.json();

        if (manifestFiles.length === 0) {
          throw new Error('No data files found');
        }

        // Ensure files have absolute path for DuckDB
        const files = manifestFiles.map((f) => {
          // Remove 'public/' prefix if present and ensure leading slash
          const cleanPath = f.replace(/^public\//, '');
          return cleanPath.startsWith('/') ? cleanPath : `/${cleanPath}`;
        });

        await loadParquetFiles(files);

        // Load statistics
        const statsResult = await query<Stats>(queries.getStats);
        setStats(statsResult[0]);

        // Load top organizations
        const orgaosResult = await query<OrgaoStats>(queries.getTopOrgaos(10));
        setTopOrgaos(orgaosResult);
      } catch (err) {
        console.error('Error loading data:', err);
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    }

    loadData();
  }, []);

  if (loading) {
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          <p className="mt-4 text-gray-600">Carregando dados...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="bg-red-50 border border-red-200 rounded-lg p-6">
          <h2 className="text-red-800 font-bold mb-2">Erro ao carregar dados</h2>
          <p className="text-red-600">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <h1 className="text-4xl font-bold mb-2">Dados do PNCP</h1>
      <p className="text-gray-600 mb-8">
        Explore contratos públicos do Brasil de forma transparente
      </p>

      {/* Statistics Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <StatCard
          title="Total de Contratos"
          value={stats?.total_contratos.toLocaleString('pt-BR') ?? '0'}
          icon={<TrendingUp />}
          color="blue"
        />
        <StatCard
          title="Órgãos"
          value={stats?.total_orgaos.toLocaleString('pt-BR') ?? '0'}
          icon={<Building2 />}
          color="green"
        />
        <StatCard
          title="Fornecedores"
          value={stats?.total_fornecedores.toLocaleString('pt-BR') ?? '0'}
          icon={<Users />}
          color="purple"
        />
        <StatCard
          title="Valor Total"
          value={`R$ ${((stats?.valor_total ?? 0) / 1e9).toFixed(2)}B`}
          icon={<DollarSign />}
          color="orange"
        />
      </div>

      {/* Top Organizations */}
      <div className="bg-white rounded-lg shadow-lg p-6">
        <h2 className="text-2xl font-bold mb-4">Top 10 Órgãos por Valor</h2>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b">
                <th className="text-left py-2 px-4">Órgão</th>
                <th className="text-left py-2 px-4">CNPJ</th>
                <th className="text-right py-2 px-4">Contratos</th>
                <th className="text-right py-2 px-4">Valor Total</th>
              </tr>
            </thead>
            <tbody>
              {topOrgaos.map((orgao, idx) => (
                <tr key={idx} className="border-b hover:bg-gray-50">
                  <td className="py-2 px-4">{orgao.orgao}</td>
                  <td className="py-2 px-4 font-mono text-sm">{orgao.cnpj}</td>
                  <td className="py-2 px-4 text-right">
                    {orgao.num_contratos.toLocaleString('pt-BR')}
                  </td>
                  <td className="py-2 px-4 text-right font-semibold">
                    R$ {(orgao.valor_total / 1e6).toFixed(2)}M
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
```

**Task 3.4: Stat Card Component**

Create `components/StatCard.tsx`:
```typescript
import { ReactNode } from 'react';
import { clsx } from 'clsx';

interface StatCardProps {
  title: string;
  value: string;
  icon: ReactNode;
  color: 'blue' | 'green' | 'purple' | 'orange';
}

const colorClasses = {
  blue: 'bg-blue-50 text-blue-600',
  green: 'bg-green-50 text-green-600',
  purple: 'bg-purple-50 text-purple-600',
  orange: 'bg-orange-50 text-orange-600',
};

export default function StatCard({ title, value, icon, color }: StatCardProps) {
  return (
    <div className="bg-white rounded-lg shadow-lg p-6">
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-sm font-medium text-gray-600">{title}</h3>
        <div className={clsx('p-2 rounded-lg', colorClasses[color])}>{icon}</div>
      </div>
      <p className="text-3xl font-bold">{value}</p>
    </div>
  );
}
```

---

### Phase 4: GitHub Actions (Priority: High)

**Task 4.1: Data Sync Workflow**

Create `.github/workflows/sync-data.yml`:
```yaml
name: Sync Parquet Data

on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch:
  repository_dispatch:
    types: [baliza-updated]

permissions:
  contents: write

jobs:
  sync-data:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Create data directory
        run: mkdir -p public/data/contratos

      - name: Download latest Parquet from baliza releases
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          # Get latest release
          LATEST_RELEASE=$(gh release list --repo franklinbaldo/baliza --limit 1 --json tagName --jq '.[0].tagName')
          echo "Latest release: $LATEST_RELEASE"

          # Download all parquet files
          gh release download "$LATEST_RELEASE" \
            --repo franklinbaldo/baliza \
            --pattern "*.parquet" \
            --dir public/data/contratos \
            --clobber || echo "No parquet files found in release"

          # List downloaded files
          echo "Downloaded files:"
          find public/data/contratos -name "*.parquet" -type f

      - name: Generate file manifest
        run: |
          # Create manifest.json with list of all parquet files
          # Change to public directory so paths are relative to web root
          cd public
          find data/contratos -name "*.parquet" -type f | \
            jq -R -s -c 'split("\n") | map(select(length > 0))' > data/manifest.json

          echo "Manifest created:"
          cat data/manifest.json

      - name: Commit and push changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add public/data/
          if git diff --staged --quiet; then
            echo "No changes to commit"
          else
            git commit -m "chore: sync parquet data from baliza ($(date -I))"
            git push
          fi
```

**Task 4.2: Deploy Workflow**

Create `.github/workflows/deploy.yml`:
```yaml
name: Deploy to GitHub Pages

on:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'

      - name: Install dependencies
        run: npm ci

      - name: Build Next.js site
        run: npm run build
        env:
          NODE_ENV: production

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: ./out

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
```

---

### Phase 5: Documentation (Priority: Medium)

**Task 5.1: README.md**

Create `README.md`:
```markdown
# Baliza Site

Interface web para visualização e consulta dos dados do Portal Nacional de
Contratações Públicas (PNCP) extraídos pelo [Baliza CLI](https://github.com/franklinbaldo/baliza).

🌐 **Site:** https://franklinbaldo.github.io/baliza-site

## Características

- ✅ **100% Estático** - Sem backend, hospedado gratuitamente
- ✅ **DuckDB-WASM** - Consultas SQL diretas no browser
- ✅ **Dados Atualizados** - Sincronização diária automática
- ✅ **Performance** - Consultas rápidas em milhões de registros
- ✅ **Open Source** - Código totalmente aberto

## Tecnologias

- **Framework**: Next.js 14 (Static Export)
- **Database**: DuckDB-WASM
- **UI**: React + Tailwind CSS
- **Charts**: Recharts
- **Hosting**: GitHub Pages

## Desenvolvimento Local

\`\`\`bash
# Clone o repositório
git clone https://github.com/franklinbaldo/baliza-site.git
cd baliza-site

# Instale dependências
npm install

# Baixe dados de exemplo (opcional)
npm run sync-data

# Rode em modo desenvolvimento
npm run dev

# Build para produção
npm run build
\`\`\`

O site estará disponível em `http://localhost:3000`

## Arquitetura

Este site consome os arquivos Parquet gerados pelo
[Baliza CLI](https://github.com/franklinbaldo/baliza).

```
PNCP API → Baliza CLI → Parquet → GitHub Release → baliza-site → Browser
```

Veja [ARCHITECTURE.md](https://github.com/franklinbaldo/baliza/blob/main/docs/ARCHITECTURE.md)
para entender como os dois projetos se integram.

## Estrutura do Projeto

- `app/` - Next.js App Router (páginas)
- `components/` - Componentes React reutilizáveis
- `lib/` - Utilitários (DuckDB, queries, etc)
- `types/` - Definições TypeScript
- `public/data/` - Arquivos Parquet (sincronizados via CI)

## Contribuindo

1. Fork este repositório
2. Crie uma branch: `git checkout -b minha-feature`
3. Commit: `git commit -m 'Adiciona nova feature'`
4. Push: `git push origin minha-feature`
5. Abra um Pull Request

## Licença

MIT

## Links

- [Baliza CLI](https://github.com/franklinbaldo/baliza) - Ferramenta de extração
- [PNCP](https://pncp.gov.br) - Portal Nacional de Contratações Públicas
- [Documentação](https://github.com/franklinbaldo/baliza/tree/main/docs) - Arquitetura e guias
```

---

## ✅ Acceptance Criteria

The baliza-site repository is considered complete when:

### Functionality
- [ ] Homepage displays aggregate statistics from Parquet files
- [ ] DuckDB-WASM successfully loads and queries Parquet data
- [ ] Search page allows filtering by date, value, and organization
- [ ] Download page lists available Parquet files
- [ ] Documentation page explains data schema and usage

### Code Quality
- [ ] TypeScript with no type errors
- [ ] ESLint passes with no warnings
- [ ] All components properly typed
- [ ] Responsive design (mobile to desktop)
- [ ] Loading states and error handling

### Infrastructure
- [ ] GitHub Actions sync data daily
- [ ] GitHub Actions build and deploy to Pages
- [ ] Site accessible at franklinbaldo.github.io/baliza-site
- [ ] CORS headers properly configured for WASM

### Documentation
- [ ] README with setup instructions
- [ ] Code comments for complex logic
- [ ] Data dictionary for PNCP fields
- [ ] SQL query examples

### Performance
- [ ] Initial page load < 3 seconds
- [ ] Queries return results < 1 second
- [ ] Lighthouse score > 90

---

## 🚀 Getting Started Instructions for Claude

**When implementing this project:**

1. **Start with Phase 1** - Get the basic Next.js project scaffolded
2. **Then Phase 2** - DuckDB integration is critical
3. **Then Phase 3** - Build the UI components
4. **Then Phase 4** - Set up CI/CD
5. **Finally Phase 5** - Complete documentation

**Important Notes:**

- Use TypeScript strictly - no `any` types
- Follow Next.js 14 App Router conventions
- Test DuckDB-WASM integration early
- Make responsive design a priority
- Add loading states to all async operations
- Handle errors gracefully with user-friendly messages

**When stuck:**

- Refer to the parent Baliza CLI repo for data schema
- Check DuckDB-WASM docs: https://duckdb.org/docs/api/wasm
- Check Next.js docs: https://nextjs.org/docs
- Look at the setup guide: `docs/baliza-site-setup-guide.md` in baliza repo

**Testing locally:**

Since Parquet files may not exist yet, create sample data:
```bash
# In the baliza CLI repo
uv run baliza extract
uv run baliza export --table contratos --out data/contratos

# Copy to baliza-site
cp -r data/contratos ../baliza-site/public/data/
```

---

## 📞 Questions?

If anything is unclear, consult:
- https://github.com/franklinbaldo/baliza/blob/main/docs/ARCHITECTURE.md
- https://github.com/franklinbaldo/baliza/blob/main/docs/baliza-site-setup-guide.md

---

**Good luck building baliza-site! 🚀**
