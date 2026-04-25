// Closed lexicon of common procurement nouns whose accented form is the
// canonical Portuguese spelling. The map is keyed by the unaccented form so
// a query like "construcao" can be normalized in O(1) into "construção".
//
// Why static and not derived from the corpus: the corpus is large and
// contains many noisy spellings; a hand-picked closed list matches what a
// supplier would reasonably mistype and fits Baliza's no-API/static
// philosophy. Length cap is intentional — each new entry has to clear a
// "would suppliers actually type the unaccented form" bar.

const ENTRIES: Record<string, string> = {
  // Acts of contracting
  aquisicao: 'aquisição',
  contratacao: 'contratação',
  prestacao: 'prestação',
  manutencao: 'manutenção',
  construcao: 'construção',
  reformacao: 'reforma',
  ampliacao: 'ampliação',
  modernizacao: 'modernização',
  restauracao: 'restauração',
  reabilitacao: 'reabilitação',
  demolicao: 'demolição',
  pavimentacao: 'pavimentação',
  iluminacao: 'iluminação',
  sinalizacao: 'sinalização',
  urbanizacao: 'urbanização',
  instalacao: 'instalação',
  implantacao: 'implantação',
  operacao: 'operação',
  conservacao: 'conservação',
  preparacao: 'preparação',
  divulgacao: 'divulgação',
  publicacao: 'publicação',
  comunicacao: 'comunicação',
  impressao: 'impressão',
  traducao: 'tradução',
  revisao: 'revisão',
  inspecao: 'inspeção',
  fiscalizacao: 'fiscalização',
  supervisao: 'supervisão',
  producao: 'produção',
  distribuicao: 'distribuição',
  separacao: 'separação',
  avaliacao: 'avaliação',
  capacitacao: 'capacitação',
  formacao: 'formação',
  vacinacao: 'vacinação',
  medicacao: 'medicação',
  alimentacao: 'alimentação',
  educacao: 'educação',
  gestao: 'gestão',
  administracao: 'administração',
  fundacao: 'fundação',
  estacao: 'estação',
  racao: 'ração',
  lubrificacao: 'lubrificação',
  // Goods (frequent unaccented mistypes)
  veiculo: 'veículo',
  veiculos: 'veículos',
  onibus: 'ônibus',
  mobiliario: 'mobiliário',
  oleo: 'óleo',
  combustivel: 'combustível',
  gas: 'gás',
  agua: 'água',
  alcool: 'álcool',
  mascara: 'máscara',
  oculos: 'óculos',
  camera: 'câmera',
  video: 'vídeo',
  copia: 'cópia',
  // Health & care
  saude: 'saúde',
  medico: 'médico',
  medica: 'médica',
  cirurgico: 'cirúrgico',
  cirurgica: 'cirúrgica',
  farmaceutico: 'farmacêutico',
  ambulancia: 'ambulância',
  veterinario: 'veterinário',
  higienico: 'higiênico',
  higienica: 'higiênica',
  sanitario: 'sanitário',
  // Tech & equipment
  eletronico: 'eletrônico',
  eletronica: 'eletrônica',
  telefonico: 'telefônico',
  telefonica: 'telefônica',
  didatico: 'didático',
  didatica: 'didática',
  pedagogico: 'pedagógico',
  pedagogica: 'pedagógica',
  // Logistics & utilities
  energia: 'energia',
  semaforo: 'semáforo',
  transito: 'trânsito',
  // Legal & administrative
  publico: 'público',
  publica: 'pública',
};

// Strip diacritics from a string. NFD splits accented characters into the
// base letter + combining mark, the regex then drops every combining mark
// in the U+0300–U+036F range.
export function stripAccents(s: string): string {
  return s.normalize('NFD').replace(/[̀-ͯ]/g, '');
}

// Suggest the canonical accented spelling for a free-text query. Returns
// null when no word in the term has a canonical form different from what
// the user typed (i.e. the suggestion would equal the input).
export function suggestAccented(term: string): string | null {
  const trimmed = term.trim();
  if (!trimmed) return null;

  // Split on whitespace and punctuation, but preserve the original
  // separators so the suggestion reads naturally back to the user.
  const tokens = trimmed.split(/(\s+)/);
  let changed = false;
  const rewritten = tokens.map((tok) => {
    if (!tok || /^\s+$/.test(tok)) return tok;
    const lower = tok.toLowerCase();
    const stripped = stripAccents(lower);
    const canonical = ENTRIES[stripped];
    if (canonical && canonical !== lower) {
      changed = true;
      return canonical;
    }
    return tok;
  });

  return changed ? rewritten.join('') : null;
}
