/**
 * Per-route OG card metadata. Each entry generates a static SVG card at
 * build time via /og/<slug>.svg. Add a row when adding a new top-level
 * route. Detail pages (contratacao, orgao, fornecedor, municipio,
 * vigilancia) reuse their route-level card and update title/description
 * at runtime via lib/pageMeta.
 */
export interface OgRoute {
  slug: string;
  pathname: string;
  title: string;
  subtitle: string;
  accent?: string;
}

export const OG_ROUTES: ReadonlyArray<OgRoute> = [
  { slug: 'home', pathname: '/', title: 'Baliza Monitor', subtitle: 'Contratações públicas brasileiras, abertas e preservadas.' },
  { slug: 'busca', pathname: '/busca', title: 'Busca livre no PNCP', subtitle: 'Pesquise contratações públicas pelo objeto ou nº de controle.' },
  { slug: 'mercado', pathname: '/mercado', title: 'Mercado — Agregação', subtitle: 'Compradores, fornecedores e faixa de preço.' },
  { slug: 'comparar', pathname: '/comparar', title: 'Comparar Municípios', subtitle: 'Compare gastos com cidades de porte similar.' },
  { slug: 'atas', pathname: '/atas', title: 'Atas de Registro de Preços', subtitle: 'Contratos vigentes servidos do arquivo Parquet.' },
  { slug: 'dispensas', pathname: '/dispensas', title: 'Dispensas — Bases Legais', subtitle: 'Artigos da Lei 14.133/2021 mais citados.' },
  { slug: 'catmat', pathname: '/catmat', title: 'Busca CATMAT/CATSER', subtitle: 'Encontre o código de material/serviço público.' },
  { slug: 'explorador', pathname: '/explorador', title: 'Explorador SQL', subtitle: 'Consulte os dados do PNCP via DuckDB no navegador.' },
  { slug: 'desenvolvedores', pathname: '/desenvolvedores', title: 'Para desenvolvedores', subtitle: 'Como consumir o arquivo Parquet de Python, R, JS.' },
  { slug: 'sobre', pathname: '/sobre', title: 'Sobre o Baliza', subtitle: 'Metodologia, arquitetura e objetivos.' },
  { slug: 'status', pathname: '/status', title: 'Status do Pipeline', subtitle: 'Monitoramento da integridade dos dados.' },
  { slug: 'vigilancia', pathname: '/vigilancia', title: 'Vigilância', subtitle: 'Acompanhe novidades de uma vigilância salva.' },
  { slug: 'contratacao', pathname: '/contratacao', title: 'Detalhamento de Contratação', subtitle: 'Detalhes preservados de uma contratação pública.' },
  { slug: 'orgao', pathname: '/orgao', title: 'Painel do Órgão', subtitle: 'Histórico de contratações de um órgão público.' },
  { slug: 'fornecedor', pathname: '/fornecedor', title: 'Painel do Fornecedor', subtitle: 'Histórico de contratos arquivados do fornecedor.' },
  { slug: 'municipio', pathname: '/municipio', title: 'Painel do Município', subtitle: 'Contratações registradas pelo município.' },
];

const BY_PATHNAME = new Map(OG_ROUTES.map((r) => [r.pathname.replace(/\/$/, ''), r]));

export function findOgRoute(pathname: string): OgRoute {
  // Astro provides Astro.url.pathname including the base prefix in some
  // setups. Normalise by stripping a trailing slash and any leading
  // /baliza prefix so the lookup matches OG_ROUTES values.
  const stripped = pathname
    .replace(/^\/baliza/, '')
    .replace(/\/$/, '')
    .toLowerCase();
  const match = BY_PATHNAME.get(stripped || '/');
  return match ?? OG_ROUTES[0]!;
}
