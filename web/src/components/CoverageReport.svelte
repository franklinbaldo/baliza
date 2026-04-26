<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { resolve } from '../lib/baseUrl';
  import { formatInteger, formatParticao } from '../lib/format';
  import { queryPublishingCnpjRaizes } from '../lib/parquetFallback';

  setQueryClientContext(getQueryClient());

  /** Reference entry shipped in web/public/data/cnpj-orgaos-publicos.json. */
  interface PublicCnpjEntry {
    raiz: string;
    nome: string;
    natureza: string;
  }

  // Lazy-load the reference list so the JS chunk for /status doesn't grow
  // by the size of the (potentially MB-scale) reference itself.
  const referenceQuery = createQuery(() => ({
    queryKey: QUERY_KEYS.publicCnpjReference,
    staleTime: 24 * 60 * 60 * 1000,
    queryFn: async (): Promise<PublicCnpjEntry[]> => {
      const url = resolve('data/cnpj-orgaos-publicos.json');
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const raw = (await res.json()) as PublicCnpjEntry[];
      if (!Array.isArray(raw)) throw new Error('reference file is not an array');
      return raw;
    },
    retry: false,
  }));

  // Numerator: distinct cnpj_orgao raizes from the latest contratos
  // parquet (queried in-browser via DuckDB-WASM). Gated on the reference
  // dataset being available — without a denominator the SELECT DISTINCT
  // result has nothing to compare against and the UI exits early via the
  // reference-error banner, so kicking off the parquet shard download
  // and DuckDB query would be pure waste in the bootstrap state.
  const publishingQuery = createQuery(() => ({
    queryKey: QUERY_KEYS.publishingCnpjRaizes,
    enabled: !!referenceQuery.data && referenceQuery.data.length > 0,
    staleTime: 5 * 60 * 1000,
    queryFn: async (): Promise<{ raizes: string[]; dataParticao: string | null }> => {
      const r = await queryPublishingCnpjRaizes();
      if (!r.ok) throw new Error(r.reason);
      // r.rows is string[] — each row is one raiz string from the SELECT
      // DISTINCT result. ArchiveResult<T> wraps T[] for the row payload.
      return { raizes: r.rows, dataParticao: r.dataParticao };
    },
    retry: false,
  }));

  const reference = $derived(referenceQuery.data ?? []);
  const publishing = $derived(publishingQuery.data?.raizes ?? []);
  const dataParticao = $derived(publishingQuery.data?.dataParticao ?? null);

  const loading = $derived(referenceQuery.isFetching || publishingQuery.isFetching);
  const referenceError = $derived(referenceQuery.error as Error | null);
  const publishingError = $derived(publishingQuery.error as Error | null);

  // Group natureza_juridica codes by the first digit of the second pair —
  // captures the federative level (Federal, Estadual, Municipal). Codes
  // 1015..1058 sit at the federal/state/municipal direct-administration
  // bands per Receita Federal's table; mapping is approximate but useful
  // as a top-line breakdown in this v1.
  function esferaOf(natureza: string): 'Federal' | 'Estadual' | 'Municipal' | 'Outras' {
    const n = parseInt(natureza, 10);
    if (!Number.isFinite(n)) return 'Outras';
    // Direct administration:
    //   1015 Órgão Público do Poder Executivo Federal
    //   1023 Órgão Público do Poder Executivo Estadual ou Distrito Federal
    //   1031 Órgão Público do Poder Executivo Municipal
    // Indirect (autarquias / fundações / empresas):
    //   1040..1074 Federal · 1112..1147 Estadual · 1180..1228 Municipal
    if (n === 1015 || (n >= 1040 && n <= 1074) || n === 1244 || n === 1252) return 'Federal';
    if (n === 1023 || (n >= 1112 && n <= 1147) || n === 1260 || n === 1279) return 'Estadual';
    if (n === 1031 || (n >= 1180 && n <= 1228) || n === 1287 || n === 1295) return 'Municipal';
    return 'Outras';
  }

  // Aggregate stats by esfera. Pure derivation from the two arrays —
  // the heavy lifting (the Set membership tests) runs once per data
  // change, then the template just reads the dict.
  const stats = $derived.by(() => {
    if (reference.length === 0 || publishing.length === 0) return null;
    const publishedSet = new Set(publishing);
    const groups: Record<string, { total: number; published: number }> = {
      Federal: { total: 0, published: 0 },
      Estadual: { total: 0, published: 0 },
      Municipal: { total: 0, published: 0 },
      Outras: { total: 0, published: 0 },
    };
    let totalPublished = 0;
    for (const entry of reference) {
      const e = esferaOf(entry.natureza);
      groups[e].total += 1;
      if (publishedSet.has(entry.raiz)) {
        groups[e].published += 1;
        totalPublished += 1;
      }
    }
    return {
      totalReference: reference.length,
      totalPublished,
      coveragePct: reference.length > 0 ? (totalPublished / reference.length) * 100 : 0,
      esferas: groups,
    };
  });

  // "Ausências notáveis": reference entries with no PNCP record. Cap to
  // the first 50 sorted by raiz so the rendered list stays readable;
  // the full audit is the kind of thing that lives in a follow-up
  // dedicated page if anyone needs it.
  const absences = $derived.by(() => {
    if (reference.length === 0 || publishing.length === 0) return [];
    const publishedSet = new Set(publishing);
    return reference
      .filter((e) => !publishedSet.has(e.raiz))
      .slice(0, 50);
  });

  function fmtCnpjRaiz(r: string): string {
    return `${r.slice(0, 2)}.${r.slice(2, 5)}.${r.slice(5, 8)}`;
  }
</script>

<section class="coverage" aria-label="Cobertura de órgãos públicos no PNCP">
  <header>
    <h2>Cobertura de órgãos públicos</h2>
    <p>
      Comparação entre <strong>quem publica no PNCP</strong> (CNPJs raiz
      vistos no parquet arquivado) e o universo de <strong>quem deveria
      publicar</strong> (cadastro da Receita Federal, naturezas jurídicas
      1xxx — administração pública direta e indireta).
    </p>
  </header>

  {#if referenceError && referenceError.message.includes('404')}
    <div class="banner banner--info" role="status">
      <p>
        <strong>Cadastro de referência ainda não publicado.</strong>
        O dataset <code>cnpj-orgaos-publicos.json</code> é gerado pela
        action mensal <code>build-orgaos-publicos.yml</code>. Aguarde a
        primeira execução agendada (dia 15 de cada mês) ou dispare
        manualmente no Actions tab para popular esta seção.
      </p>
    </div>
  {:else if referenceError}
    <div class="banner banner--warn" role="alert">
      <p>Falha ao carregar o cadastro de referência: {referenceError.message}.</p>
    </div>
  {:else if publishingError}
    <div class="banner banner--warn" role="alert">
      <p>
        Falha ao consultar o parquet de contratos: {publishingError.message}.
        O numerador (CNPJs publicadores) não pôde ser computado agora.
      </p>
    </div>
  {:else if loading && !stats}
    <div class="skeleton-row" aria-busy="true">
      <div class="skeleton skel-num"></div>
      <div class="skeleton skel-line"></div>
      <div class="skeleton skel-line"></div>
    </div>
  {:else if stats}
    <div class="headline">
      <span class="big">{formatInteger(stats.totalPublished)}</span>
      <span class="of">de</span>
      <span class="big big--ref">{formatInteger(stats.totalReference)}</span>
      <span class="label">órgãos públicos publicaram no PNCP</span>
      <span class="pct" class:pct--low={stats.coveragePct < 50}>
        {stats.coveragePct.toFixed(1)}%
      </span>
    </div>

    <table class="esferas">
      <thead>
        <tr><th>Esfera</th><th>Cobertos</th><th>Cadastrados</th><th>%</th></tr>
      </thead>
      <tbody>
        {#each ['Federal', 'Estadual', 'Municipal', 'Outras'] as label (label)}
          {@const g = stats.esferas[label]}
          {@const p = g.total > 0 ? (g.published / g.total) * 100 : 0}
          <tr>
            <th scope="row">{label}</th>
            <td>{formatInteger(g.published)}</td>
            <td>{formatInteger(g.total)}</td>
            <td class:low={p < 50}>{p.toFixed(1)}%</td>
          </tr>
        {/each}
      </tbody>
    </table>

    {#if absences.length > 0}
      <details class="absences">
        <summary>
          Ausências notáveis · primeiras {absences.length} de {formatInteger(stats.totalReference - stats.totalPublished)}
        </summary>
        <ul>
          {#each absences as a (a.raiz)}
            <li>
              <span class="cnpj">{fmtCnpjRaiz(a.raiz)}</span>
              <span class="nome">{a.nome}</span>
              <span class="natureza">nat. {a.natureza}</span>
            </li>
          {/each}
        </ul>
      </details>
    {/if}

    {#if dataParticao}
      <p class="footnote">
        Dados a partir do snapshot Parquet de {formatParticao(dataParticao)} no Internet Archive.
      </p>
    {/if}
  {/if}
</section>

<style>
  .coverage {
    display: grid;
    gap: var(--space-4);
  }
  header h2 {
    font-family: var(--font-display);
    font-weight: 400;
    font-size: clamp(20px, 2vw, 28px);
    margin: 0;
  }
  header p {
    color: var(--color-text-dim);
    line-height: 1.6;
    max-width: 72ch;
    margin: var(--space-2) 0 0;
  }
  .banner {
    padding: var(--space-3) var(--space-4);
    border: 1px solid var(--color-border);
    border-left: 4px solid var(--color-azul);
    background: color-mix(in srgb, var(--color-bg) 92%, transparent);
  }
  .banner--warn { border-left-color: var(--color-tijolo); }
  .banner code {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    background: color-mix(in srgb, var(--color-azul) 10%, transparent);
    padding: 1px 5px;
    border-radius: var(--radius-sm);
  }
  .headline {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-2);
    padding: var(--space-4);
    background: var(--color-bg);
    border: 1px solid var(--color-border);
    border-left: 5px solid var(--color-accent);
  }
  .big {
    font-family: var(--font-display);
    font-weight: 300;
    font-size: clamp(32px, 4vw, 56px);
    line-height: 1;
  }
  .big--ref { color: var(--color-text-dim); }
  .of { color: var(--color-text-dim); font-size: var(--text-md); }
  .label {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--color-text-dim);
    flex: 1;
  }
  .pct {
    font-family: var(--font-display);
    font-weight: 500;
    font-size: clamp(20px, 2vw, 28px);
    color: var(--color-verde);
  }
  .pct--low { color: var(--color-tijolo); }
  .esferas {
    width: 100%;
    border-collapse: collapse;
  }
  .esferas th,
  .esferas td {
    padding: var(--space-2) var(--space-3);
    text-align: right;
    border-bottom: 1px solid var(--color-border);
  }
  .esferas th[scope='row'] {
    text-align: left;
    font-family: var(--font-mono);
    font-weight: 500;
    font-size: var(--text-sm);
  }
  .esferas thead th {
    text-align: right;
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--color-text-dim);
  }
  .esferas thead th:first-child { text-align: left; }
  .esferas .low { color: var(--color-tijolo); }
  .absences summary {
    cursor: pointer;
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    color: var(--color-azul);
    padding: var(--space-2) 0;
  }
  .absences ul {
    list-style: none;
    margin: 0;
    padding: var(--space-2) 0;
    display: grid;
    gap: 4px;
    max-height: 360px;
    overflow-y: auto;
  }
  .absences li {
    display: grid;
    grid-template-columns: max-content 1fr max-content;
    gap: var(--space-3);
    align-items: baseline;
    padding: 4px 0;
    border-bottom: 1px dotted var(--color-border);
    font-size: var(--text-sm);
  }
  .cnpj {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    color: var(--color-azul);
  }
  .natureza {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    color: var(--color-text-dim);
  }
  .footnote {
    font-size: var(--text-xs);
    color: var(--color-text-dim);
    margin: 0;
    font-family: var(--font-mono);
  }
  .skeleton-row {
    display: grid;
    gap: var(--space-2);
    padding: var(--space-4);
    background: var(--color-bg);
    border: 1px solid var(--color-border);
  }
  .skel-num { height: 3rem; width: 40%; }
  .skel-line { height: 1rem; width: 100%; }
</style>
