// PNCP Resource Atlas — frontend-side catalog.
//
// This is the web layer's mirror of the backend resource registry in
// `src/baliza/resources/__init__.py`. It exists so the UI can render
// the conceptual spine PCA → Publicações → Atas → Contratos → Itens
// generically, without hard-coding `contratos` everywhere a resource
// label is needed.
//
// "registered" entries here MUST correspond to a resource in the backend
// `RESOURCES` dict (i.e. one that produces parquet files on Internet
// Archive). "planned" entries correspond to `PLANNED_RESOURCES` and are
// shown to the user as upcoming, never as queryable.
//
// Adding a new resource is a single edit here, on the frontend side.
// The architectural contract is documented in
// `web/features/pncp-resource-atlas.feature`.

export type ResourceLayer =
  | 'intent'
  | 'open'
  | 'reusable'
  | 'final'
  | 'comparable';

export type ResourceStatus = 'registered' | 'planned';

export interface ResourceCatalogEntry {
  /** Backend resource_name; matches `PNCPResource.resource_name`. */
  name: string;
  /** Display label, Portuguese. */
  label: string;
  /** One-sentence description in plain language. */
  description: string;
  /** Conceptual layer in the resource atlas. */
  layer: ResourceLayer;
  /** Whether the backend pipeline already mirrors this resource. */
  status: ResourceStatus;
}

export const RESOURCE_CATALOG: readonly ResourceCatalogEntry[] = [
  {
    name: 'pca',
    label: 'PCA',
    description:
      'Plano Anual de Contratações: o que cada órgão pretende comprar.',
    layer: 'intent',
    status: 'registered',
  },
  {
    name: 'publicacoes',
    label: 'Publicações',
    description:
      'Contratações abertas no momento — oportunidades em curso no PNCP.',
    layer: 'open',
    status: 'registered',
  },
  {
    name: 'atas',
    label: 'Atas de Registro de Preços',
    description:
      'Atas vigentes que outros órgãos podem usar como referência ou carona.',
    layer: 'reusable',
    status: 'registered',
  },
  {
    name: 'contratos',
    label: 'Contratos',
    description:
      'Contratos celebrados — fato jurídico e econômico finalizado.',
    layer: 'final',
    status: 'registered',
  },
  {
    name: 'itens',
    label: 'Itens',
    description:
      'Linhas de item por contrato/ata, base para inteligência de preços.',
    layer: 'comparable',
    status: 'planned',
  },
] as const;

export const REGISTERED_RESOURCE_NAMES: ReadonlySet<string> = new Set(
  RESOURCE_CATALOG.filter((r) => r.status === 'registered').map((r) => r.name),
);

export function getResourceEntry(
  name: string,
): ResourceCatalogEntry | undefined {
  return RESOURCE_CATALOG.find((r) => r.name === name);
}
