// Bundled procurement glossary surfaced as inline tooltips on detail pages.
// Closed map by design: the citizen journey wants a familiar set of terms
// (modalities + the "isso é normal?" exemption flavors), not a generalized
// thesaurus. Add entries here when a new term shows up enough that an
// uninitiated reader would benefit from a sentence of context.

export interface GlossaryEntry {
  term: string;
  plain: string;
}

function key(raw: string): string {
  return raw
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim();
}

const ENTRIES: GlossaryEntry[] = [
  {
    term: 'Pregão',
    plain:
      'Modalidade de licitação por disputa de preços, geralmente eletrônica, usada para bens e serviços comuns.',
  },
  {
    term: 'Pregão Eletrônico',
    plain:
      'Pregão realizado pela internet, em que os fornecedores enviam lances em tempo real até a melhor oferta vencer.',
  },
  {
    term: 'Dispensa',
    plain:
      'Compra direta sem licitação aberta, permitida em casos previstos em lei (valor pequeno, emergência etc.).',
  },
  {
    term: 'Dispensa Eletrônica',
    plain:
      'Dispensa de licitação processada por sistema eletrônico, permitindo concorrência ainda que sem licitação formal.',
  },
  {
    term: 'Inexigibilidade',
    plain:
      'Contratação sem licitação porque há um único fornecedor possível ou competição é inviável.',
  },
  {
    term: 'Concorrência',
    plain:
      'Modalidade de licitação para contratos de maior valor, com ampla publicidade e exigências mais rigorosas.',
  },
  {
    term: 'Tomada de Preços',
    plain:
      'Modalidade da Lei 8.666 para valores médios; foi substituída pelo pregão e pela concorrência na Lei 14.133.',
  },
  {
    term: 'Convite',
    plain:
      'Modalidade da Lei 8.666 com convite a fornecedores específicos; também substituída pela Lei 14.133.',
  },
  {
    term: 'Concurso',
    plain:
      'Modalidade usada para escolher trabalho técnico, científico ou artístico mediante prêmio.',
  },
  {
    term: 'Leilão',
    plain:
      'Modalidade para venda de bens públicos a quem oferecer o maior lance.',
  },
  {
    term: 'RDC',
    plain:
      'Regime Diferenciado de Contratações, criado para obras específicas; sua aplicabilidade migrou para a Lei 14.133.',
  },
  {
    term: 'Credenciamento',
    plain:
      'Procedimento que habilita todos os interessados que cumprem os requisitos a contratar com a administração.',
  },
  {
    term: 'Manifestação de Interesse',
    plain:
      'Chamamento para que privados apresentem estudos ou propostas que podem virar uma futura licitação.',
  },
  {
    term: 'Ata de Registro de Preços',
    plain:
      'Documento que registra preços vencedores para compras futuras parceladas, sem novo certame.',
  },
];

const INDEX: Map<string, GlossaryEntry> = new Map(ENTRIES.map((e) => [key(e.term), e]));

export function lookupGlossary(term: string | null | undefined): GlossaryEntry | null {
  if (!term) return null;
  return INDEX.get(key(term)) ?? null;
}

export const GLOSSARY = ENTRIES;
