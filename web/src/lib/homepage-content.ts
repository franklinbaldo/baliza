/**
 * Baliza Portal — shared copy used across components.
 *
 * Holds two small registries:
 *
 *  - `HOMEPAGE_SQL_EXAMPLES` (legacy name: FEATURED_QUERIES) drives
 *    DuckDBExplorer's chip strip. Each entry is a (label, sql) pair where
 *    `'IA_URL'` is substituted with the resolved Parquet URL at runtime.
 *
 *  - `FEATURED_QUERIES` is the curated public-watch registry consumed by
 *    Journey 7. The Python uploader (`src/baliza/rss_feed.py`) duplicates
 *    the slug+title list to keep the static-only architecture; both copies
 *    must stay in sync until we cross the static boundary.
 */

export interface HomepageSqlExample {
  label: string;
  sql: string;
}

export const HOMEPAGE_SQL_EXAMPLES: HomepageSqlExample[] = [
  {
    label: "Top 10 fornecedores/mês",
    sql: "SELECT nome_fornecedor, SUM(valor_total) as total FROM read_parquet('IA_URL') GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
  },
  {
    label: "Compras emergenciais",
    sql: "SELECT * FROM read_parquet('IA_URL') WHERE compra_emergencial = true",
  },
];

export interface FeaturedQuery {
  /** URL-safe slug used in the IA feed filename `feed-{slug}.xml`. */
  slug: string;
  /** Human-readable title used as the RSS channel title. */
  title: string;
  /** Free-text term passed to fetchPublicacaoPagesForObjeto for previews. */
  q: string;
  /** Optional minimum value threshold (BRL); applied client-side and in SQL. */
  minValor?: number;
}

export const FEATURED_QUERIES: FeaturedQuery[] = [
  {
    slug: 'dispensas-acima-1mi',
    title: 'Dispensas acima de R$ 1 mi',
    q: 'dispensa',
    minValor: 1_000_000,
  },
  {
    slug: 'compras-emergenciais',
    title: 'Compras emergenciais',
    q: 'emergencial',
  },
];
