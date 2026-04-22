<script lang="ts">
  import { onMount } from 'svelte';
  import { fetchManifestRows, IA_MANIFEST_URL, type ManifestRow } from '../lib/ia-manifest';
  import AlertBanner from './AlertBanner.svelte';

  // Cap so the table stays scannable; the full CSV is one click away.
  const ROW_LIMIT = 24;

  let rows = $state<ManifestRow[]>([]);
  let loading = $state(true);
  let failed = $state(false);

  function shortHash(sha256: string | undefined): string {
    if (!sha256) return '—';
    return `${sha256.slice(0, 8)}…${sha256.slice(-4)}`;
  }

  onMount(async () => {
    try {
      const all = await fetchManifestRows();
      // Sort by partition desc so the latest snapshot is at the top.
      const sorted = [...all].sort((a, b) =>
        b.data_particao.localeCompare(a.data_particao),
      );
      rows = sorted.slice(0, ROW_LIMIT);
      failed = all.length === 0;
    } catch {
      failed = true;
    } finally {
      loading = false;
    }
  });
</script>

<div class="manifest-wrapper">
  {#if loading}
    <div class="manifest-skeleton" aria-busy="true" aria-label="Carregando manifesto">
      {#each [1, 2, 3] as _, i (i)}
        <div class="skeleton-row"></div>
      {/each}
    </div>
  {:else if failed}
    <AlertBanner
      title="Manifesto indisponível"
      message="Não foi possível carregar o CSV do manifesto agora. Tente novamente em alguns minutos."
      level="warning"
    />
  {:else}
    <div class="table-scroll">
      <table data-testid="manifest-table">
        <thead>
          <tr>
            <th scope="col">Parquet</th>
            <th scope="col">Tabela</th>
            <th scope="col">Partição</th>
            <th scope="col">SHA-256</th>
            <th scope="col">Row group</th>
          </tr>
        </thead>
        <tbody>
          {#each rows as row (row.parquet_url)}
            <tr>
              <td>
                <a href={row.parquet_url} target="_blank" rel="noopener" class="parquet-link">
                  {row.parquet_url.split('/').slice(-2).join('/')}
                </a>
              </td>
              <td>{row.table_name}</td>
              <td>{row.data_particao}</td>
              <td title={row.sha256 ?? ''} class="hash-cell">{shortHash(row.sha256)}</td>
              <td>{row.row_group_size ?? '—'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <p class="manifest-foot">
      Mostrando {rows.length} de {rows.length === ROW_LIMIT ? `≥${ROW_LIMIT}` : rows.length} linhas. <a href={IA_MANIFEST_URL} target="_blank" rel="noopener">Ver manifesto completo</a>.
    </p>
  {/if}
</div>

<style>
  .manifest-wrapper {
    margin-top: var(--space-md);
  }
  .manifest-skeleton {
    display: grid;
    gap: var(--space-xs);
  }
  .skeleton-row {
    height: 1.5rem;
    background: var(--color-base-200);
    border-radius: var(--radius-sm);
    animation: skeleton-pulse 1.6s ease-in-out infinite;
  }
  @keyframes skeleton-pulse {
    0%, 100% { opacity: 0.6; }
    50% { opacity: 1; }
  }
  @media (prefers-reduced-motion: reduce) {
    .skeleton-row {
      animation: none;
      opacity: 0.85;
    }
  }
  .table-scroll {
    overflow-x: auto;
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--font-size-sm);
  }
  th {
    text-align: left;
    background: var(--color-base-200);
    padding: 8px 12px;
    border-bottom: 1px solid var(--color-base-300);
    font-weight: 700;
  }
  td {
    padding: 8px 12px;
    border-bottom: 1px solid var(--color-base-300);
    white-space: nowrap;
  }
  tr:last-child td {
    border-bottom: none;
  }
  .parquet-link {
    color: var(--color-primary);
    font-family: var(--font-mono);
    font-size: var(--font-size-xs);
  }
  .hash-cell {
    font-family: var(--font-mono);
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
  }
  .manifest-foot {
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    margin-top: var(--space-xs);
  }
</style>
