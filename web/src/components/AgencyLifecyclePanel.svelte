<script lang="ts">
  import { onMount } from 'svelte';
  import { queryArchivedTableWhere, prefetchArchive } from '../lib/parquetFallback';
  import { resolve } from '../lib/baseUrl';
  import { formatBRL, formatDate } from '../lib/format';
  import type { ArchivedAta, ArchivedPublicacao, ArchivedPca } from '../lib/archive/schema';
  import Skeleton from './Skeleton.svelte';

  const { cnpj }: { cnpj: string } = $props();

  // Per-resource results
  let atas = $state<ArchivedAta[]>([]);
  let publicacoes = $state<ArchivedPublicacao[]>([]);
  let pca = $state<ArchivedPca[]>([]);

  let loadingAtas = $state(true);
  let loadingPublicacoes = $state(true);
  let loadingPca = $state(true);

  // Prefetch all three tables in parallel on mount
  onMount(() => {
    prefetchArchive('atas');
    prefetchArchive('publicacoes');
    prefetchArchive('pca');

    queryArchivedTableWhere(
      'atas',
      [{ column: 'cnpj_orgao', op: 'eq', value: cnpj }],
      { limit: 5, orderByColumn: 'data_publicacao_pncp' },
    ).then((res) => {
      if (res.ok) atas = res.rows as ArchivedAta[];
      loadingAtas = false;
    });

    queryArchivedTableWhere(
      'publicacoes',
      [{ column: 'cnpj_orgao', op: 'eq', value: cnpj }],
      { limit: 5, orderByColumn: 'data_publicacao_pncp' },
    ).then((res) => {
      if (res.ok) publicacoes = res.rows as ArchivedPublicacao[];
      loadingPublicacoes = false;
    });

    queryArchivedTableWhere(
      'pca',
      [{ column: 'cnpj_orgao', op: 'eq', value: cnpj }],
      { limit: 5 },
    ).then((res) => {
      if (res.ok) pca = res.rows as ArchivedPca[];
      loadingPca = false;
    });
  });

  const hasAnyData = $derived(atas.length > 0 || publicacoes.length > 0 || pca.length > 0);
  const allLoaded = $derived(!loadingAtas && !loadingPublicacoes && !loadingPca);
</script>

{#if !allLoaded}
  <section class="lifecycle" aria-labelledby="lifecycle-title">
    <h3 id="lifecycle-title">Cadeia de contratação</h3>
    <div class="skeleton-row"><Skeleton /><Skeleton /><Skeleton /></div>
  </section>
{:else if hasAnyData}
  <section class="lifecycle" aria-labelledby="lifecycle-title">
    <h3 id="lifecycle-title">Cadeia de contratação</h3>
    <p class="lifecycle-sub">
      PCA → Publicações → Atas → Contratos — todos os estágios deste órgão no arquivo.
    </p>

    <div class="panels">
      <!-- PCA -->
      {#if pca.length > 0}
        <details class="panel" open>
          <summary>
            <span class="panel-label">PCA — Itens planejados</span>
            <span class="panel-count">{pca.length}</span>
          </summary>
          <ul class="item-list">
            {#each pca as item (item.id_pca_pncp + '-' + item.numero_item)}
              <li class="item-row">
                <span class="item-main">{item.descricao_item ?? item.codigo_item ?? item.id_pca_pncp}</span>
                {#if item.valor_total != null}
                  <span class="item-valor">{formatBRL(item.valor_total)}</span>
                {/if}
              </li>
            {/each}
          </ul>
          <a class="panel-more" href={resolve('pca')}>Ver todos os itens PCA →</a>
        </details>
      {/if}

      <!-- Publicações -->
      {#if publicacoes.length > 0}
        <details class="panel" open>
          <summary>
            <span class="panel-label">Publicações abertas</span>
            <span class="panel-count">{publicacoes.length}</span>
          </summary>
          <ul class="item-list">
            {#each publicacoes as pub (pub.numero_controle_pncp)}
              <li class="item-row">
                <a
                  href={resolve(`busca?q=${pub.numero_controle_pncp}`)}
                  class="item-main"
                >
                  {pub.objeto_compra ?? pub.numero_controle_pncp}
                </a>
                <span class="item-meta">
                  {pub.modalidade_nome ?? ''}
                  {#if pub.valor_total_estimado != null}
                    · {formatBRL(pub.valor_total_estimado)}
                  {/if}
                </span>
              </li>
            {/each}
          </ul>
          <a class="panel-more" href={resolve('publicacoes')}>Ver publicações →</a>
        </details>
      {/if}

      <!-- Atas -->
      {#if atas.length > 0}
        <details class="panel" open>
          <summary>
            <span class="panel-label">Atas em vigor</span>
            <span class="panel-count">{atas.length}</span>
          </summary>
          <ul class="item-list">
            {#each atas as ata (ata.numero_controle_pncp_ata)}
              <li class="item-row">
                <span class="item-main">{ata.objeto_contratacao ?? ata.numero_controle_pncp_ata}</span>
                <span class="item-meta">
                  {#if ata.vigencia_fim}vigência até {formatDate(ata.vigencia_fim)}{/if}
                </span>
              </li>
            {/each}
          </ul>
          <a class="panel-more" href={resolve('atas')}>Buscar atas →</a>
        </details>
      {/if}
    </div>
  </section>
{/if}

<style>
  .lifecycle {
    margin-top: var(--space-6);
    padding-top: var(--space-6);
    border-top: 1px solid var(--color-border);
  }
  h3 {
    font-size: var(--text-lg);
    font-weight: 500;
    margin: 0 0 var(--space-1);
  }
  .lifecycle-sub {
    font-size: var(--text-sm);
    color: var(--color-text-dim);
    margin: 0 0 var(--space-4);
  }
  .panels {
    display: grid;
    gap: var(--space-3);
  }
  .panel {
    background: var(--color-surface);
    border: 1px solid var(--color-border);
    border-radius: 4px;
    padding: var(--space-3);
  }
  summary {
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: space-between;
    list-style: none;
    user-select: none;
  }
  summary::-webkit-details-marker { display: none; }
  .panel-label {
    font-weight: 500;
    font-size: var(--text-sm);
  }
  .panel-count {
    font-size: var(--text-xs);
    background: var(--color-border);
    border-radius: 999px;
    padding: 2px 8px;
    color: var(--color-text-dim);
  }
  .item-list {
    list-style: none;
    padding: 0;
    margin: var(--space-3) 0 var(--space-2);
    display: grid;
    gap: var(--space-2);
  }
  .item-row {
    display: grid;
    gap: 2px;
    padding: var(--space-2) 0;
    border-bottom: 1px solid var(--color-border);
    font-size: var(--text-sm);
  }
  .item-row:last-child { border-bottom: none; }
  .item-main {
    color: var(--color-text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  a.item-main { text-decoration: none; color: var(--color-primary); }
  a.item-main:hover { text-decoration: underline; }
  .item-valor, .item-meta {
    font-size: var(--text-xs);
    color: var(--color-text-dim);
  }
  .panel-more {
    font-size: var(--text-xs);
    color: var(--color-text-dim);
    text-decoration: none;
    display: block;
    text-align: right;
    margin-top: var(--space-1);
  }
  .panel-more:hover { text-decoration: underline; }
  .skeleton-row {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: var(--space-3);
  }
</style>
