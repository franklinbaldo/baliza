/**
 * Baliza Portal — shared copy used across components.
 *
 * Kept intentionally small: the homepage itself owns its narrative in
 * index.astro / CityHero.svelte / TrustStrip.astro. This module only holds
 * strings still referenced by sibling components (currently just
 * DuckDBExplorer's FEATURED_QUERIES).
 */

export const FEATURED_QUERIES = [
  {
    label: "Top 10 fornecedores/mês",
    sql: "SELECT nome_fornecedor, SUM(valor_total) as total FROM read_parquet('IA_URL') GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
  },
  {
    label: "Compras emergenciais",
    sql: "SELECT * FROM read_parquet('IA_URL') WHERE compra_emergencial = true",
  },
];
