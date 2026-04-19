// When PNCP returns zero results for a free-text query, it is often because the
// user typed an unaccented form ("construcao", "acao") that does not match the
// canonical accented noun in the index. This helper produces a best-effort
// accented alternative using a small lexicon of common procurement terms. We
// intentionally avoid a general-purpose diacritic-repair library: the
// procurement vocabulary is narrow and a closed map keeps the UX predictable.

const ACCENT_MAP: Record<string, string> = {
  acao: 'ação',
  acoes: 'ações',
  agua: 'água',
  alimentacao: 'alimentação',
  ambulatorio: 'ambulatório',
  area: 'área',
  areas: 'áreas',
  aquisicao: 'aquisição',
  aquisicoes: 'aquisições',
  atencao: 'atenção',
  basico: 'básico',
  basica: 'básica',
  calcado: 'calçado',
  calcados: 'calçados',
  cirurgico: 'cirúrgico',
  cirurgica: 'cirúrgica',
  civica: 'cívica',
  construcao: 'construção',
  construcoes: 'construções',
  contratacao: 'contratação',
  contratacoes: 'contratações',
  creche: 'creche',
  combustivel: 'combustível',
  combustiveis: 'combustíveis',
  credito: 'crédito',
  dispensa: 'dispensa',
  edificacao: 'edificação',
  educacao: 'educação',
  eletronico: 'eletrônico',
  eletronica: 'eletrônica',
  emergencia: 'emergência',
  energia: 'energia',
  equipamentos: 'equipamentos',
  escritorio: 'escritório',
  especial: 'especial',
  exames: 'exames',
  exercicio: 'exercício',
  farmaceutico: 'farmacêutico',
  farmacia: 'farmácia',
  fisico: 'físico',
  funcao: 'função',
  gestao: 'gestão',
  hidraulica: 'hidráulica',
  higiene: 'higiene',
  hospitalar: 'hospitalar',
  imoveis: 'imóveis',
  imovel: 'imóvel',
  implementacao: 'implementação',
  informatica: 'informática',
  instalacao: 'instalação',
  limpeza: 'limpeza',
  locacao: 'locação',
  manutencao: 'manutenção',
  materia: 'matéria',
  medica: 'médica',
  medicamentos: 'medicamentos',
  medico: 'médico',
  merenda: 'merenda',
  modernizacao: 'modernização',
  municipio: 'município',
  obra: 'obra',
  odontologico: 'odontológico',
  operacao: 'operação',
  orgao: 'órgão',
  organico: 'orgânico',
  pavimentacao: 'pavimentação',
  pedagogico: 'pedagógico',
  pregao: 'pregão',
  pregoes: 'pregões',
  prestacao: 'prestação',
  producao: 'produção',
  publico: 'público',
  publica: 'pública',
  publicacao: 'publicação',
  recuperacao: 'recuperação',
  reforma: 'reforma',
  saude: 'saúde',
  seguranca: 'segurança',
  servico: 'serviço',
  servicos: 'serviços',
  sistema: 'sistema',
  tecnologia: 'tecnologia',
  telefonico: 'telefônico',
  transporte: 'transporte',
  uniformes: 'uniformes',
  veiculo: 'veículo',
  veiculos: 'veículos',
  vigilancia: 'vigilância',
};

function stripDiacritics(word: string): string {
  return word.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

// Returns the query rewritten with accented forms for any whitespace-separated
// token that has a mapping, or `null` when no token was changed. The null
// return is load-bearing: the caller only shows the suggestion UI when a
// different, meaningful alternative exists.
export function suggestAccented(query: string): string | null {
  if (!query) return null;
  const tokens = query.split(/(\s+)/);
  let changed = false;
  const rewritten = tokens.map((tok) => {
    if (!tok || /^\s+$/.test(tok)) return tok;
    const lower = tok.toLowerCase();
    const bare = stripDiacritics(lower);
    const hit = ACCENT_MAP[bare];
    if (hit && hit !== lower) {
      changed = true;
      return hit;
    }
    return tok;
  });
  return changed ? rewritten.join('') : null;
}
