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
    icon: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-zap"><path d="M4 14a1 1 0 0 1-.78-1.63l9.9-10.2a.5.5 0 0 1 .86.46l-1.92 6.02A1 1 0 0 0 13 10h7a1 1 0 0 1 .78 1.63l-9.9 10.2a.5.5 0 0 1-.86-.46l1.92-6.02A1 1 0 0 0 11 14z"/></svg>`,
  },
  {
    title: "2. Fé Pública Digital",
    description: "Cada dia de contratação é registrado no Internet Archive, garantindo que o passado não seja apagado.",
    icon: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-landmark"><line x1="3" x2="21" y1="22" y2="22"/><line x1="6" x2="6" y1="18" y2="11"/><line x1="10" x2="10" y1="18" y2="11"/><line x1="14" x2="14" y1="18" y2="11"/><line x1="18" x2="18" y1="18" y2="11"/><polygon points="12 2 20 7 4 7"/></svg>`,
  },
  {
    title: "3. Exploração WASM",
    description: "Analise milhões de linhas direto no seu navegador usando o motor DuckDB. Sem servidores, sem rastro.",
    icon: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-code"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>`,
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
