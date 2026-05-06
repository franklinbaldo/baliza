<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import {
    archiveErrorMessage,
    prefetchArchive,
    queryArchivedTableWhere,
  } from '../lib/parquetFallback';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import { formatBRL, formatDate } from '../lib/format';
  import { resolve } from '../lib/baseUrl';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';
  import HubHeader from './HubHeader.svelte';
  import Skeleton from './Skeleton.svelte';

  setQueryClientContext(getQueryClient());

  const { objeto: objetoProp = '' }: { objeto?: string } = $props();

  function initialObjeto(): string {
    if (objetoProp) return objetoProp;
    if (typeof window === 'undefined') return '';
    return new URLSearchParams(window.location.search).get('objeto') ?? '';
  }

  let searchInput = $state(initialObjeto());
  let submittedObjeto = $state(initialObjeto());

  $effect(() => {
    if (submittedObjeto) prefetchArchive('contratos');
  });

  function todayIso(): string {
    // Local date parts — toISOString() returns UTC, which in negative-offset
    // timezones during local evening advances the cutoff by one day and
    // excludes contracts still vigent on the current local date.
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }

  const atasQuery = createQuery(() => ({
    queryKey: QUERY_KEYS.atas(submittedObjeto),
    enabled: submittedObjeto.length >= 3,
    queryFn: async (): Promise<ArchivedContrato[]> => {
      const result = await queryArchivedTableWhere(
        'contratos',
        [
          { column: 'objeto_contrato', op: 'ilike', value: submittedObjeto },
          { column: 'data_vigencia_fim', op: 'gte', value: todayIso() },
        ],
        { limit: 50, orderByColumn: 'data_vigencia_fim' },
      );
      if (result.ok) return result.rows;
      if (result.reason === 'empty') return [];
      throw new Error(archiveErrorMessage(result.reason));
    },
  }));

  const rows = $derived(atasQuery.data ?? []);
  const loading = $derived(atasQuery.isFetching);
  const error = $derived(atasQuery.error as Error | null);

  function handleSubmit(ev: Event) {
    ev.preventDefault();
    const term = searchInput.trim();
    submittedObjeto = term;
    if (typeof window === 'undefined') return;
    // eslint-disable-next-line svelte/prefer-svelte-reactivity
    const params = new URLSearchParams(window.location.search);
    if (term) params.set('objeto', term);
    else params.delete('objeto');
    const qs = params.toString();
    window.history.replaceState({}, '', qs ? `${window.location.pathname}?${qs}` : window.location.pathname);
  }

  function truncate(s: string | null | undefined, n: number): string {
    if (!s) return '';
    return s.length > n ? `${s.slice(0, n)}…` : s;
  }
</script>

<section>
  <HubHeader kicker="📒 Atas vigentes" title="Atas de Registro de Preços">
    {#snippet lede()}
      <p>Busca contratos vigentes cujo objeto corresponde ao termo pesquisado. Fonte: arquivo Parquet (IA).</p>
    {/snippet}
  </HubHeader>

  <form role="search" onsubmit={handleSubmit}>
    <label for="atas-objeto-input" class="sr-only">Objeto a pesquisar</label>
    <input
      id="atas-objeto-input"
      type="search"
      bind:value={searchInput}
      placeholder="Ex.: papel A4, merenda escolar, medicamentos..."
      aria-label="Objeto a pesquisar"
    />
    <button type="submit">Buscar</button>
  </form>

  {#if !submittedObjeto || submittedObjeto.length < 3}
    <EmptyState
      title="Digite o objeto a pesquisar"
      message="Use ao menos 3 caracteres para buscar atas vigentes pelo objeto contratado."
    />
  {:else if loading}
    <Skeleton label="Buscando atas vigentes" rows={3} />
  {:else if error}
    <AlertBanner title="Não foi possível buscar as atas" message={error.message} level="error" />
  {:else if rows.length === 0}
    <EmptyState
      title="Nenhuma ata vigente encontrada"
      message="Nenhum contrato vigente corresponde ao objeto pesquisado no arquivo consolidado."
    />
  {:else}
    <section data-testid="atas-list">
      {#each rows as row (row.numero_controle_pncp ?? `${row.cnpj_orgao}-${row.sequencial_contrato}`)}
        <article>
          <a href={resolve(`contratacao?id=${row.numero_controle_pncp ?? ''}`)}>
            <header>
              <strong>{row.razao_social_orgao ?? 'Órgão Arquivado'}</strong>
              <code>{row.cnpj_orgao ?? ''}</code>
            </header>
            <p>{truncate(row.objeto_contrato, 150)}</p>
            <footer>
              <small>
                {formatDate(row.data_vigencia_inicio ?? '')} → {formatDate(row.data_vigencia_fim ?? '')}
              </small>
              <strong>{formatBRL(row.valor_global ?? row.valor_inicial ?? null)}</strong>
            </footer>
          </a>
        </article>
      {/each}
    </section>
  {/if}
</section>

