export const QUERY_KEYS = {
  syncStats:   ['sync-stats']                                                  as const,
  orgao:       (cnpj: string, dias: number) => ['orgao', cnpj, dias]           as const,
  fornecedor:  (cnpj: string)               => ['fornecedor', cnpj]            as const,
  municipio:   (ibge: string, dias: number) => ['municipio', ibge, dias]       as const,
  contratacao: (id: string)                 => ['contratacao', id]             as const,
  localBids:   (ibge: string)               => ['local-bids', ibge]            as const,
  atas:        (objeto: string)             => ['atas', objeto]                as const,
} as const;
