export const QUERY_KEYS = {
  syncStats:   ['sync-stats']                                                  as const,
  orgao:       (cnpj: string, dias: number) => ['orgao', cnpj, dias]           as const,
  fornecedor:  (cnpj: string)               => ['fornecedor', cnpj]            as const,
  municipio:   (ibge: string, dias: number) => ['municipio', ibge, dias]       as const,
  contratacao: (id: string)                 => ['contratacao', id]             as const,
  localBids:   (ibge: string)               => ['local-bids', ibge]            as const,
  atas:        (objeto: string)             => ['atas', objeto]                as const,
  dispensas:   (objeto: string)             => ['dispensas', objeto]           as const,
  busca:       (q: string, cnpj?: string, ibge?: string, extras?: Record<string, string>) =>
    ['busca', q, cnpj, ibge, extras] as const,
  cityAggregates: (ibge: string) => ['city-aggregates', ibge] as const,
  publishingCnpjRaizes: ['publishing-cnpj-raizes'] as const,
  publicCnpjReference: ['public-cnpj-reference'] as const,
} as const;
