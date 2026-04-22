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

<div class="atas container">
  <header class="hub-header">
    <span class="type-badge">📒 ATAS VIGENTES</span>
    <h1>Atas de Registro de Preços</h1>
    <p class="meta-row">
      Busca contratos vigentes cujo objeto corresponde ao termo pesquisado. Fonte: arquivo Parquet (IA).
    </p>
  </header>

  <form class="search-form" onsubmit={handleSubmit}>
    <label class="sr-only" for="atas-objeto-input">Objeto a pesquisar</label>
    <input
      id="atas-objeto-input"
      type="search"
      bind:value={searchInput}
      placeholder="Ex.: papel A4, merenda escolar, medicamentos..."
      aria-label="Objeto a pesquisar"
    />
    <button type="submit" class="btn btn-primary">Buscar</button>
  </form>

  {#if !submittedObjeto || submittedObjeto.length < 3}
    <EmptyState
      title="Digite o objeto a pesquisar"
      message="Use ao menos 3 caracteres para buscar atas vigentes pelo objeto contratado."
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Buscando atas vigentes">
      <div class="skeleton skeleton-bid"></div>
      <div class="skeleton skeleton-bid"></div>
      <div class="skeleton skeleton-bid"></div>
    </div>
  {:else if error}
    <AlertBanner title="Não foi possível buscar as atas" message={error.message} level="error" />
  {:else if rows.length === 0}
    <EmptyState
      title="Nenhuma ata vigente encontrada"
      message="Nenhum contrato vigente corresponde ao objeto pesquisado no arquivo consolidado."
    />
  {:else}
    <section class="atas-list" data-testid="atas-list">
      {#each rows as row (row.numero_controle_pncp ?? `${row.cnpj_orgao}-${row.sequencial_contrato}`)}
        <a
          href={resolve(`contratacao?id=${row.numero_controle_pncp ?? ''}`)}
          class="ata-card"
        >
          <div class="ata-header">
            <span class="agency">{row.razao_social_orgao ?? 'Órgão Arquivado'}</span>
            <span class="cnpj">{row.cnpj_orgao ?? ''}</span>
          </div>
          <p class="objeto">{truncate(row.objeto_contrato, 150)}</p>
          <div class="ata-footer">
            <span class="vigencia">
              {formatDate(row.data_vigencia_inicio ?? '')} → {formatDate(row.data_vigencia_fim ?? '')}
            </span>
            <span class="valor">{formatBRL(row.valor_global ?? row.valor_inicial ?? null)}</span>
          </div>
        </a>
      {/each}
    </section>
  {/if}
</div>

<style>
  .atas { padding: var(--space-2xl) 0; }
  .hub-header { margin-bottom: var(--space-lg); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  .type-badge { font-family: var(--font-mono); font-size: 0.7rem; background: var(--color-primary); color: white; padding: 2px 8px; border-radius: 4px; }
  h1 { font-size: var(--font-size-2xl); margin-top: var(--space-sm); }
  .meta-row { color: var(--color-secondary); font-size: var(--font-size-sm); margin: 4px 0 0; }

  .search-form { display: flex; gap: var(--space-sm); margin-bottom: var(--space-lg); }
  .search-form input {
    flex: 1;
    padding: var(--space-sm) var(--space-md);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    font-size: var(--font-size-md);
  }
  .sr-only {
    position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px;
    overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;
  }

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-bid { height: 5rem; border-radius: var(--radius-sm); }

  .atas-list { display: grid; gap: var(--space-md); }
  .ata-card {
    display: block;
    text-decoration: none;
    color: inherit;
    background: var(--color-base-100);
    padding: var(--space-md);
    border-radius: var(--radius-sm);
    border: 1px solid var(--color-base-300);
    transition: transform 0.2s, border-color 0.2s;
  }
  .ata-card:hover { transform: translateX(5px); border-color: var(--color-primary); }
  .ata-header {
    display: flex; justify-content: space-between; align-items: baseline; gap: var(--space-sm);
    font-size: var(--font-size-sm); margin-bottom: var(--space-sm);
  }
  .agency { font-weight: 700; }
  .cnpj { font-family: var(--font-mono); color: var(--color-secondary); font-size: var(--font-size-xs); }
  .objeto { font-size: var(--font-size-sm); line-height: 1.5; margin-bottom: var(--space-sm); }
  .ata-footer {
    display: flex; justify-content: space-between; align-items: baseline; flex-wrap: wrap; gap: var(--space-sm);
    font-size: var(--font-size-sm);
  }
  .vigencia { font-family: var(--font-mono); color: var(--color-secondary); }
  .valor { font-weight: 800; color: var(--color-primary); }
</style>
