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

<section aria-label="Cobertura de órgãos públicos no PNCP">
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
    <article role="status">
      <p>
        <strong>Cadastro de referência ainda não publicado.</strong>
        O dataset <code>cnpj-orgaos-publicos.json</code> é gerado pela
        action mensal <code>build-orgaos-publicos.yml</code>. Aguarde a
        primeira execução agendada (dia 15 de cada mês) ou dispare
        manualmente no Actions tab para popular esta seção.
      </p>
    </article>
  {:else if referenceError}
    <article role="alert" data-invalid="true">
      <p>Falha ao carregar o cadastro de referência: {referenceError.message}.</p>
    </article>
  {:else if publishingError}
    <article role="alert" data-invalid="true">
      <p>
        Falha ao consultar o parquet de contratos: {publishingError.message}.
        O numerador (CNPJs publicadores) não pôde ser computado agora.
      </p>
    </article>
  {:else if loading && !stats}
    <article aria-busy="true" class="is-skeleton"><p>&nbsp;</p><p>&nbsp;</p><p>&nbsp;</p></article>
  {:else if stats}
    <article>
      <hgroup>
        <strong>{formatInteger(stats.totalPublished)}</strong> <small>de</small>
        <strong>{formatInteger(stats.totalReference)}</strong>
        <p><small>órgãos públicos publicaram no PNCP</small></p>
      </hgroup>
      <progress value={stats.coveragePct} max="100"></progress>
      <p><strong>{stats.coveragePct.toFixed(1)}%</strong></p>
    </article>

    <table>
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
            <td>{p.toFixed(1)}%</td>
          </tr>
        {/each}
      </tbody>
    </table>

    {#if absences.length > 0}
      <details>
        <summary>
          Ausências notáveis · primeiras {absences.length} de {formatInteger(stats.totalReference - stats.totalPublished)}
        </summary>
        <ul>
          {#each absences as a (a.raiz)}
            <li>
              <code>{fmtCnpjRaiz(a.raiz)}</code>
              <strong>{a.nome}</strong>
              <small>nat. {a.natureza}</small>
            </li>
          {/each}
        </ul>
      </details>
    {/if}

    {#if dataParticao}
      <footer>
        <small>Dados a partir do snapshot Parquet de {formatParticao(dataParticao)} no Internet Archive.</small>
      </footer>
    {/if}
  {/if}
</section>

