/**
 * Baliza Portal — shared copy used across components.
 *
 * Kept intentionally small: the homepage itself owns its narrative in
 * index.astro / CityHero.svelte / TrustStrip.astro. This module only holds
 * strings still referenced by sibling components (SearchHero, DuckDBExplorer).
 */

export const PROJECT_MISSION = {
  title: "A memória permanente das contratações públicas brasileiras.",
  tagline:
    "Cada contrato publicado no PNCP, arquivado diariamente no Internet Archive e consultável no seu navegador — com URL estável, hash e SQL aberto. Feito para quem trabalha com licitação todos os dias; útil para qualquer pessoa que precisa auditar o Estado.",
};

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

export const SEARCH_HINTS = [
  "CNPJ da empresa",
  "Secretaria de Saúde",
  "Merenda escolar",
  "Dispensa de licitação",
];
