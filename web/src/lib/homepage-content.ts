/**
 * Baliza Portal — Centralized Content & Meta-Data
 * Modeled after the 'Reference & Sharing' philosophy.
 */

export const PROJECT_MISSION = {
  title: "A memória permanente das contratações públicas brasileiras.",
  tagline: "Preservamos e auditamos cada centavo do PNCP em um arquivo público imutável.",
};

export const HOW_IT_WORKS_CARDS = [
  {
    title: "1. Captura Stateless",
    description: "Extraímos dados do PNCP em tempo real, sem dependência de bancos de dados locais pesados.",
    icon: "⚡",
  },
  {
    title: "2. Fé Pública Digital",
    description: "Cada dia de contratação é registrado no Internet Archive, garantindo que o passado não seja apagado.",
    icon: "🏛️",
  },
  {
    title: "3. Exploração WASM",
    description: "Analise milhões de linhas direto no seu navegador usando o motor DuckDB. Sem servidores, sem rastro.",
    icon: "🦆",
  },
];

export const QUICK_ACCESS_CARDS = [
  {
    title: "Explorador SQL",
    description: "O playground definitivo para analistas. Rode queries complexas nos arquivos Parquet.",
    href: "/explorador",
    cta: "Abrir SQL",
    icon: "📊",
  },
  {
    title: "API Playground",
    description: "Consulte a API do PNCP com validação Zod e schemas prontos para integração.",
    href: "/api",
    cta: "Testar API",
    icon: "🔌",
  },
  {
    title: "Metodologia",
    description: "Entenda os critérios de quarentena, limpeza e transparência do projeto Baliza.",
    href: "/sobre",
    cta: "Ler Documentos",
    icon: "📖",
  },
];

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
