export const QUERY_KEYS = {
  syncStats:   ['sync-stats']                          as const,
  orgao:       (cnpj: string) => ['orgao', cnpj]       as const,
  municipio:   (ibge: string) => ['municipio', ibge]   as const,
  contratacao: (id: string)   => ['contratacao', id]   as const,
  localBids:   (ibge: string) => ['local-bids', ibge]  as const,
} as const;
