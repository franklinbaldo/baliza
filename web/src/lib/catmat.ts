// Bundled CATMAT/CATSER taxonomy for static catalog lookup.
// CATMAT covers goods (materiais); CATSER covers services (serviços).
// This is a curated subset of the official Brazilian government catalog.
// Expand entries here as new procurement objects appear frequently.

export interface CatmatEntry {
  code: string;
  description: string;
  type: 'CATMAT' | 'CATSER';
}

function normalize(raw: string): string {
  return raw
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .trim();
}

export const CATMAT_ENTRIES: CatmatEntry[] = [
  // Papel e material de escritório
  { code: '7510.00.00', description: 'Papel sulfite branco A4 75g/m²', type: 'CATMAT' },
  { code: '7510.00.01', description: 'Papel para impressão branco formato A4 75g/m² pacote com 500 folhas', type: 'CATMAT' },
  { code: '7510.00.02', description: 'Papel sulfite branco A4 90g/m²', type: 'CATMAT' },
  { code: '7510.00.03', description: 'Papel sulfite branco ofício 75g/m²', type: 'CATMAT' },
  { code: '7510.00.04', description: 'Papel sulfite colorido A4 75g/m²', type: 'CATMAT' },
  { code: '7540.00.00', description: 'Envelope branco saco 26x36cm com 250 unidades', type: 'CATMAT' },
  { code: '7540.00.01', description: 'Envelope ofício com janela 114x229mm', type: 'CATMAT' },
  // Canetas e lápis
  { code: '7520.00.00', description: 'Caneta esferográfica azul ponta 1.0mm', type: 'CATMAT' },
  { code: '7520.00.01', description: 'Caneta esferográfica preta ponta 0.7mm', type: 'CATMAT' },
  { code: '7520.00.02', description: 'Caneta esferográfica vermelha ponta 1.0mm', type: 'CATMAT' },
  { code: '7520.00.03', description: 'Lápis grafite nº 2 HB caixa com 12 unidades', type: 'CATMAT' },
  { code: '7520.00.04', description: 'Marcador para quadro branco azul ponta chanfrada', type: 'CATMAT' },
  // Toner e cartucho
  { code: '7612.00.00', description: 'Cartucho de tinta preta para impressora jato de tinta', type: 'CATMAT' },
  { code: '7612.00.01', description: 'Cartucho de tinta colorida para impressora jato de tinta', type: 'CATMAT' },
  { code: '7612.00.02', description: 'Toner preto para impressora laser', type: 'CATMAT' },
  { code: '7612.00.03', description: 'Toner colorido para impressora laser colorida', type: 'CATMAT' },
  // Combustível
  { code: '1305.00.00', description: 'Gasolina comum', type: 'CATMAT' },
  { code: '1305.00.01', description: 'Gasolina aditivada', type: 'CATMAT' },
  { code: '1305.00.02', description: 'Óleo diesel S-10', type: 'CATMAT' },
  { code: '1305.00.03', description: 'Óleo diesel S-500', type: 'CATMAT' },
  { code: '1305.00.04', description: 'Etanol hidratado combustível', type: 'CATMAT' },
  // Informática — hardware
  { code: '7021.00.00', description: 'Microcomputador desktop processador i5 memória 8GB SSD 256GB', type: 'CATMAT' },
  { code: '7021.00.01', description: 'Notebook processador i7 memória 16GB SSD 512GB tela 14 polegadas', type: 'CATMAT' },
  { code: '7021.00.02', description: 'Monitor LED 21,5 polegadas full HD', type: 'CATMAT' },
  { code: '7021.00.03', description: 'Impressora laser monocromática', type: 'CATMAT' },
  { code: '7021.00.04', description: 'Impressora multifuncional laser colorida', type: 'CATMAT' },
  { code: '7021.00.05', description: 'No-break 1000VA bivolt com 4 tomadas', type: 'CATMAT' },
  // Merenda e gêneros alimentícios
  { code: '8910.00.00', description: 'Arroz branco tipo 1 polido pacote 5kg', type: 'CATMAT' },
  { code: '8910.00.01', description: 'Feijão carioca tipo 1 pacote 1kg', type: 'CATMAT' },
  { code: '8910.00.02', description: 'Óleo de soja refinado garrafa 900ml', type: 'CATMAT' },
  { code: '8910.00.03', description: 'Açúcar cristal tipo 1 pacote 5kg', type: 'CATMAT' },
  { code: '8910.00.04', description: 'Sal refinado iodado pacote 1kg', type: 'CATMAT' },
  { code: '8910.00.05', description: 'Macarrão espaguete 500g', type: 'CATMAT' },
  // Limpeza
  { code: '7920.00.00', description: 'Detergente líquido neutro 500ml', type: 'CATMAT' },
  { code: '7920.00.01', description: 'Desinfetante concentrado de uso geral 2L', type: 'CATMAT' },
  { code: '7920.00.02', description: 'Álcool etílico 70% líquido 1L', type: 'CATMAT' },
  { code: '7920.00.03', description: 'Sabão em pó multiuso caixa 1kg', type: 'CATMAT' },
  { code: '7920.00.04', description: 'Papel toalha folha dupla rolo com 2 unidades', type: 'CATMAT' },
  { code: '7920.00.05', description: 'Papel higiênico folha simples rolo com 4 unidades', type: 'CATMAT' },
  // Serviços de TI
  { code: 'S00701', description: 'Serviço de desenvolvimento de software sob demanda', type: 'CATSER' },
  { code: 'S00702', description: 'Serviço de suporte e manutenção de sistemas de informação', type: 'CATSER' },
  { code: 'S00703', description: 'Serviço de hospedagem de sistemas e dados em nuvem', type: 'CATSER' },
  { code: 'S00704', description: 'Serviço de consultoria em tecnologia da informação', type: 'CATSER' },
  { code: 'S00705', description: 'Serviço de licenciamento de software', type: 'CATSER' },
  // Serviços de limpeza e conservação
  { code: 'S00801', description: 'Serviço de limpeza e conservação predial', type: 'CATSER' },
  { code: 'S00802', description: 'Serviço de desinsetização e desratização', type: 'CATSER' },
  { code: 'S00803', description: 'Serviço de coleta e transporte de resíduos sólidos', type: 'CATSER' },
  // Serviços de vigilância
  { code: 'S00901', description: 'Serviço de vigilância patrimonial desarmada', type: 'CATSER' },
  { code: 'S00902', description: 'Serviço de vigilância patrimonial armada', type: 'CATSER' },
  { code: 'S00903', description: 'Serviço de monitoramento eletrônico por câmeras CFTV', type: 'CATSER' },
  // Serviços gráficos e publicidade
  { code: 'S01001', description: 'Serviço de impressão gráfica offset', type: 'CATSER' },
  { code: 'S01002', description: 'Serviço de publicidade e propaganda', type: 'CATSER' },
];

export function searchCatmat(query: string): CatmatEntry[] {
  const q = normalize(query);
  if (!q) return [];

  const words = q.split(/\s+/).filter(Boolean);

  return CATMAT_ENTRIES.filter((entry) => {
    const desc = normalize(entry.description);
    const code = entry.code.toLowerCase();
    // Match if query is a prefix of the code OR all words appear in description
    if (code.startsWith(q)) return true;
    return words.every((w) => desc.includes(w));
  }).sort((a, b) => {
    const da = normalize(a.description);
    const db = normalize(b.description);
    // Exact code match first
    if (a.code.toLowerCase() === q) return -1;
    if (b.code.toLowerCase() === q) return 1;
    // Shorter description = more specific match = higher rank
    return da.length - db.length;
  });
}
