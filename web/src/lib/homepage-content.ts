/**
 * Baliza Portal — Centralized Content & Meta-Data
 *
 * Copy here follows VISION.md: preservation first, analysis second, and the
 * exigent professional case (supplier prospecting, public buyer) leads.
 */

export const PROJECT_MISSION = {
  title: "A memória permanente das contratações públicas brasileiras.",
  tagline:
    "Cada contrato publicado no PNCP, arquivado diariamente no Internet Archive e consultável no seu navegador — com URL estável, hash e SQL aberto. Feito para quem trabalha com licitação todos os dias; útil para qualquer pessoa que precisa auditar o Estado.",
};

export const HOW_IT_WORKS_CARDS = [
  {
    title: "1. Preservação primeiro",
    description:
      "Cada dia de contratações vira um Parquet imutável no Internet Archive, com URL estável e hash verificável. Não desaparece se o PNCP sair do ar.",
    icon: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-landmark"><line x1="3" x2="21" y1="22" y2="22"/><line x1="6" x2="6" y1="18" y2="11"/><line x1="10" x2="10" y1="18" y2="11"/><line x1="14" x2="14" y1="18" y2="11"/><line x1="18" x2="18" y1="18" y2="11"/><polygon points="12 2 20 7 4 7"/></svg>`,
  },
  {
    title: "2. Análise sem servidor",
    description:
      "SQL completo sobre os Parquets roda no seu navegador via DuckDB WASM. Nada passa por um backend nosso. Sua consulta é privada e a página é estática.",
    icon: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-code"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>`,
  },
  {
    title: "3. Citabilidade por padrão",
    description:
      "Toda contratação tem permalink Baliza e link de volta para o PNCP. A consulta SQL vai no URL — compartilhe o link, quem abrir reproduz o mesmo resultado.",
    icon: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-link"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>`,
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
    title: "Para desenvolvedores",
    description: "URLs estáveis de Parquet + exemplos em Python, R e JS para consumir os dados no seu projeto.",
    href: "/desenvolvedores",
    cta: "Ver manifesto",
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

/**
 * The six journeys from VISION.md. Each has the persona's verbatim central
 * question and a status reflecting VISION.md's "current state" annotations:
 *
 *   green   → capabilities largely shipped for this persona
 *   wip     → partial; some capabilities shipped, others missing
 *   planned → none of the journey's capabilities ship yet
 *
 * `href` points to the best existing landing point; "planned" journeys link to
 * /sobre until their dedicated landing pages exist.
 */
export const JOURNEYS = [
  {
    slug: "fornecedor",
    role: "Fornecedor B2G",
    question:
      "Como eu vendo meu produto ao setor público? Quem comprou o que eu vendo, por quanto, em qual modalidade, de quem?",
    status: "wip" as const,
    href: "/explorador",
    icon: "🏢",
  },
  {
    slug: "comprador",
    role: "Comprador público",
    question:
      "Como faço essa compra de forma defensável? Existe ata vigente que posso aderir? Qual é o preço de referência evidenciado?",
    status: "planned" as const,
    href: "/sobre",
    icon: "🏛️",
  },
  {
    slug: "jornalista",
    role: "Jornalista investigativo",
    question:
      "Esse contrato tem padrão suspeito? Preciso de permalink citável, link de volta ao PNCP, export CSV/markdown e rollups por CNPJ.",
    status: "green" as const,
    href: "/explorador",
    icon: "📰",
  },
  {
    slug: "cidadao",
    role: "Cidadão informado",
    question:
      "A notícia disse que o hospital X recebeu R$ Y — é verdade? Quero busca em linguagem comum.",
    status: "wip" as const,
    href: "#busca",
    icon: "👤",
  },
  {
    slug: "pesquisador",
    role: "Pesquisador acadêmico",
    question:
      "Preciso de dataset reprodutível, schema estável, citação acadêmica, changelog e download em lote dos Parquets.",
    status: "wip" as const,
    href: "/desenvolvedores",
    icon: "🎓",
  },
  {
    slug: "desenvolvedor",
    role: "Desenvolvedor integrador",
    question:
      "Quero consumir os dados do Baliza no meu projeto — manifesto Parquet com URLs e hashes, exemplos em Python, R e JS.",
    status: "wip" as const,
    href: "/desenvolvedores",
    icon: "⚙️",
  },
];
