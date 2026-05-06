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

<figure>
  {#if loading}
    <article aria-busy="true" class="is-skeleton" aria-label="Carregando manifesto"><p>&nbsp;</p><p>&nbsp;</p><p>&nbsp;</p></article>
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
                <a href={row.parquet_url} target="_blank" rel="noopener">
                  {row.parquet_url.split('/').slice(-2).join('/')}
                </a>
              </td>
              <td><code>{row.table_name}</code></td>
              <td>{row.data_particao}</td>
              <td title={row.sha256 ?? ''}><code>{shortHash(row.sha256)}</code></td>
              <td>{row.row_group_size ?? '—'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <figcaption>
      <small>
        Mostrando {rows.length} de {rows.length === ROW_LIMIT ? `≥${ROW_LIMIT}` : rows.length} linhas.
        <a href={IA_MANIFEST_URL} target="_blank" rel="noopener">Ver manifesto completo</a>.
      </small>
    </figcaption>
  {/if}
</figure>

