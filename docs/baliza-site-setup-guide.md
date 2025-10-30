# Guia de Setup: baliza-site

Este documento contém instruções para criar o repositório `baliza-site` —
a interface web para visualização e consulta dos dados extraídos pelo
[Baliza CLI](https://github.com/franklinbaldo/baliza).

---

## 1. Criar o Repositório

```bash
# Criar novo repositório no GitHub
gh repo create franklinbaldo/baliza-site --public --description "Interface web para visualização de dados do PNCP extraídos pelo Baliza CLI"

# Clonar localmente
git clone https://github.com/franklinbaldo/baliza-site.git
cd baliza-site
```

---

## 2. Escolher Stack Tecnológico

### Opção Recomendada: Next.js com Static Export

**Vantagens:**
- ✅ Site estático = zero custo de hosting
- ✅ DuckDB-WASM para consultas no browser
- ✅ Deploy gratuito no GitHub Pages ou Vercel
- ✅ Framework moderno e bem documentado

**Setup inicial:**

```bash
# Criar projeto Next.js
npx create-next-app@latest . --typescript --tailwind --app --no-src-dir

# Instalar dependências do DuckDB-WASM
npm install @duckdb/duckdb-wasm
npm install apache-arrow

# Instalar bibliotecas de UI
npm install @tanstack/react-table
npm install recharts
npm install lucide-react
```

### Opção Alternativa: Astro

**Vantagens:**
- ✅ Ainda mais performático
- ✅ Menos JavaScript no cliente
- ✅ Ótimo para sites focados em conteúdo

```bash
# Criar projeto Astro
npm create astro@latest . -- --template minimal --typescript strict --install
```

---

## 3. Estrutura Inicial do Projeto

```
baliza-site/
├── README.md
├── package.json
├── next.config.js (ou astro.config.mjs)
├── public/
│   └── data/                  # Arquivos Parquet (via GitHub Actions)
│       └── .gitkeep
├── app/                       # Next.js App Router
│   ├── layout.tsx
│   ├── page.tsx              # Dashboard principal
│   ├── busca/
│   │   └── page.tsx          # Página de busca
│   ├── downloads/
│   │   └── page.tsx          # Página de downloads
│   └── docs/
│       └── page.tsx          # Documentação
├── components/
│   ├── DataTable.tsx         # Tabela de dados
│   ├── Charts.tsx            # Gráficos
│   ├── Filters.tsx           # Filtros de busca
│   └── Header.tsx            # Cabeçalho
├── lib/
│   ├── duckdb.ts             # DuckDB-WASM wrapper
│   ├── queries.ts            # Consultas SQL prontas
│   └── data.ts               # Utilitários de dados
├── scripts/
│   └── download-parquet.js   # Download automático do baliza
├── .github/
│   └── workflows/
│       ├── deploy.yml        # Build e deploy do site
│       └── sync-data.yml     # Sincronizar dados do baliza
└── docs/
    └── user-guide.md
```

---

## 4. Configurar DuckDB-WASM

Criar `lib/duckdb.ts`:

```typescript
import * as duckdb from '@duckdb/duckdb-wasm';

let db: duckdb.AsyncDuckDB | null = null;

export async function getDuckDB(): Promise<duckdb.AsyncDuckDB> {
  if (db) return db;

  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], {
      type: 'text/javascript',
    })
  );

  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule);

  return db;
}

export async function loadParquetFiles(files: string[]) {
  const db = await getDuckDB();
  const conn = await db.connect();

  // Criar view unificada de todos os arquivos Parquet
  const fileList = files.map(f => `'${f}'`).join(', ');
  await conn.query(`
    CREATE OR REPLACE VIEW contratos AS
    SELECT * FROM read_parquet([${fileList}])
  `);

  return conn;
}

export async function query(sql: string) {
  const db = await getDuckDB();
  const conn = await db.connect();
  const result = await conn.query(sql);
  return result.toArray();
}
```

Criar `lib/queries.ts`:

```typescript
export const queries = {
  // Estatísticas gerais
  getStats: `
    SELECT
      COUNT(*) as total_contratos,
      COUNT(DISTINCT orgaoEntidade.cnpj) as total_orgaos,
      SUM(valorInicial) as valor_total,
      MIN(dataPublicacaoPncp) as data_inicio,
      MAX(dataPublicacaoPncp) as data_fim
    FROM contratos
  `,

  // Top órgãos por valor
  getTopOrgaos: (limit = 10) => `
    SELECT
      orgaoEntidade.razaoSocial as orgao,
      COUNT(*) as num_contratos,
      SUM(valorInicial) as valor_total
    FROM contratos
    GROUP BY orgaoEntidade.razaoSocial
    ORDER BY valor_total DESC
    LIMIT ${limit}
  `,

  // Buscar por CNPJ
  searchByCNPJ: (cnpj: string) => `
    SELECT * FROM contratos
    WHERE orgaoEntidade.cnpj = '${cnpj}'
       OR fornecedor.cpfCnpj = '${cnpj}'
    ORDER BY dataPublicacaoPncp DESC
  `,

  // Filtros avançados
  filterContratos: (filters: {
    dataInicio?: string;
    dataFim?: string;
    valorMin?: number;
    valorMax?: number;
  }) => {
    let where = [];
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

    const whereClause = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';

    return `
      SELECT * FROM contratos
      ${whereClause}
      ORDER BY dataPublicacaoPncp DESC
      LIMIT 100
    `;
  },
};
```

---

## 5. Configurar GitHub Actions para Sincronizar Dados

Criar `.github/workflows/sync-data.yml`:

```yaml
name: Sync Parquet Data

on:
  schedule:
    - cron: '0 6 * * *'  # Diariamente às 6h UTC
  workflow_dispatch:
  repository_dispatch:
    types: [baliza-updated]

jobs:
  sync-data:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Download latest Parquet from baliza releases
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          mkdir -p public/data/contratos

          # Download arquivos Parquet da última release do baliza
          gh release download \
            --repo franklinbaldo/baliza \
            --pattern "*.parquet" \
            --dir public/data/contratos \
            --clobber || echo "Nenhum arquivo encontrado"

          # Listar arquivos baixados
          ls -lah public/data/contratos/

      - name: Commit and push if changed
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add public/data/
          git diff --quiet && git diff --staged --quiet || \
            (git commit -m "chore: sync parquet data from baliza" && git push)
```

Criar `.github/workflows/deploy.yml`:

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

      - name: Build
        run: npm run build
        env:
          NODE_ENV: production

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: ./out  # Next.js static export

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

## 6. Configurar Next.js para Static Export

Editar `next.config.js`:

```javascript
/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',  // Habilita static export
  images: {
    unoptimized: true,  // Necessário para static export
  },
  // Habilitar headers para WASM
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

Editar `package.json`:

```json
{
  "scripts": {
    "dev": "next dev",
    "build": "next build",
    "start": "next start",
    "lint": "next lint"
  }
}
```

---

## 7. Criar Componentes Básicos

### Dashboard Principal (`app/page.tsx`)

```typescript
'use client';

import { useEffect, useState } from 'react';
import { getDuckDB, query } from '@/lib/duckdb';
import { queries } from '@/lib/queries';

export default function Home() {
  const [stats, setStats] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      try {
        // Carregar arquivos Parquet
        const db = await getDuckDB();

        // Executar query de estatísticas
        const result = await query(queries.getStats);
        setStats(result[0]);
      } catch (error) {
        console.error('Erro ao carregar dados:', error);
      } finally {
        setLoading(false);
      }
    }

    loadData();
  }, []);

  if (loading) {
    return <div className="p-8">Carregando dados...</div>;
  }

  return (
    <main className="p-8">
      <h1 className="text-4xl font-bold mb-8">Baliza - Dados do PNCP</h1>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-white p-6 rounded-lg shadow">
          <h2 className="text-lg font-semibold text-gray-600">Total de Contratos</h2>
          <p className="text-3xl font-bold">{stats?.total_contratos?.toLocaleString()}</p>
        </div>

        <div className="bg-white p-6 rounded-lg shadow">
          <h2 className="text-lg font-semibold text-gray-600">Órgãos</h2>
          <p className="text-3xl font-bold">{stats?.total_orgaos?.toLocaleString()}</p>
        </div>

        <div className="bg-white p-6 rounded-lg shadow">
          <h2 className="text-lg font-semibold text-gray-600">Valor Total</h2>
          <p className="text-3xl font-bold">
            R$ {(stats?.valor_total / 1e9).toFixed(2)}B
          </p>
        </div>
      </div>
    </main>
  );
}
```

---

## 8. README.md do baliza-site

```markdown
# Baliza Site

Interface web para visualização e consulta dos dados do Portal Nacional de
Contratações Públicas (PNCP) extraídos pelo [Baliza CLI](https://github.com/franklinbaldo/baliza).

## Características

- ✅ **100% Estático**: sem backend, hospedado gratuitamente
- ✅ **DuckDB-WASM**: consultas SQL diretas no browser
- ✅ **Dados Atualizados**: sincronização diária automática
- ✅ **Performance**: consultas rápidas em milhões de registros
- ✅ **Open Source**: código totalmente aberto

## Tecnologias

- **Framework**: Next.js 14 (Static Export)
- **Database**: DuckDB-WASM
- **UI**: React + Tailwind CSS
- **Charts**: Recharts
- **Hosting**: GitHub Pages

## Desenvolvimento Local

\`\`\`bash
# Instalar dependências
npm install

# Baixar dados de exemplo
npm run sync-data

# Rodar em modo dev
npm run dev

# Build para produção
npm run build
\`\`\`

## Arquitetura

Este site consome os arquivos Parquet gerados pelo
[Baliza CLI](https://github.com/franklinbaldo/baliza).

Veja [ARCHITECTURE.md](https://github.com/franklinbaldo/baliza/blob/main/docs/ARCHITECTURE.md)
para entender como os dois projetos se integram.

## Contribuindo

1. Fork este repositório
2. Crie uma branch: \`git checkout -b minha-feature\`
3. Commit: \`git commit -m 'Adiciona nova feature'\`
4. Push: \`git push origin minha-feature\`
5. Abra um Pull Request

## Licença

MIT
```

---

## 9. Próximos Passos

1. ✅ Criar repositório `baliza-site` no GitHub
2. ✅ Fazer setup inicial com Next.js
3. ✅ Configurar DuckDB-WASM
4. ✅ Implementar componentes básicos
5. ✅ Configurar GitHub Actions para sincronizar dados
6. ✅ Configurar GitHub Pages
7. ✅ Implementar busca e filtros
8. ✅ Adicionar visualizações (gráficos)
9. ✅ Documentação de uso

---

## Referências Úteis

- [DuckDB-WASM Docs](https://duckdb.org/docs/api/wasm)
- [Next.js Static Export](https://nextjs.org/docs/app/building-your-application/deploying/static-exports)
- [GitHub Pages Deploy](https://github.com/actions/deploy-pages)
- [Apache Arrow (Parquet)](https://arrow.apache.org/docs/js/)

---

## Exemplo de Integração Completa

### No repositório `baliza` (CLI):

```yaml
# .github/workflows/monthly-release.yml
- name: Create release with Parquet
  uses: softprops/action-gh-release@v1
  with:
    files: data/contratos/**/*.parquet
    tag_name: v${{ env.YEAR }}-${{ env.MONTH }}
```

### No repositório `baliza-site`:

```yaml
# .github/workflows/sync-data.yml
- name: Download Parquet from baliza
  run: |
    gh release download \
      --repo franklinbaldo/baliza \
      --pattern "*.parquet" \
      --dir public/data/contratos
```

### No browser do usuário:

```typescript
// Consulta SQL diretamente no browser
const result = await query(`
  SELECT * FROM contratos
  WHERE valorInicial > 1000000
  ORDER BY dataPublicacaoPncp DESC
  LIMIT 10
`);
```

---

## Estimativa de Tamanho

Considerando ~1 milhão de contratos:
- Parquet comprimido: ~100-200 MB
- DuckDB-WASM bundle: ~10 MB
- Site estático: ~5 MB
- **Total para download**: ~115-215 MB (primeira visita)
- **Consultas subsequentes**: instantâneas (cache local)

---

Este guia fornece uma base sólida para iniciar o desenvolvimento do `baliza-site`.
Ajuste conforme necessário para as suas necessidades específicas.
