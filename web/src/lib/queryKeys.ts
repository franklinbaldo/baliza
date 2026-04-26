export const QUERY_KEYS = {
  syncStats:   ['sync-stats']                                                  as const,
  orgao:       (cnpj: string, dias: number) => ['orgao', cnpj, dias]           as const,
  fornecedor:  (cnpj: string)               => ['fornecedor', cnpj]            as const,
  municipio:   (ibge: string, dias: number) => ['municipio', ibge, dias]       as const,
  contratacao: (id: string)                 => ['contratacao', id]             as const,
  localBids:   (ibge: string)               => ['local-bids', ibge]            as const,
  atas:        (objeto: string)             => ['atas', objeto]                as const,
  dispensas:   (objeto: string)             => ['dispensas', objeto]           as const,
  // `extras` carries any additional registry filters (uf, modo, …) so
  // adding a filter in lib/searchFilters doesn't require touching this
  // file. Identity (===) on the dict is fine for tanstack-query because
  // BuscaView builds a fresh object on every submit.
  busca:       (q: string, cnpj?: string, ibge?: string, extras?: Record<string, string>) =>
    ['busca', q, cnpj, ibge, extras] as const,
} as const;
