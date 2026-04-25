<script lang="ts">
  import { onMount } from 'svelte';
  import AlertBanner from './AlertBanner.svelte';
  import StatCard from './StatCard.svelte';
  import { fetchRawArchiveStatus, type RawArchiveStatus } from '../lib/ia-raw-status';
  import { formatInteger, formatRelativeTime } from '../lib/format';

  let status = $state<RawArchiveStatus | null>(null);
  let loading = $state(true);
  let error = $state<string | null>(null);

  function formatBytes(bytes: number | null | undefined): string {
    if (bytes == null || !Number.isFinite(bytes)) return '—';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let value = bytes;
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit += 1;
    }
    return `${value.toLocaleString('pt-BR', {
      maximumFractionDigits: unit === 0 ? 0 : 1,
      minimumFractionDigits: unit === 0 ? 0 : 1,
    })} ${units[unit]}`;
  }

  function formatPeriod(period: string | null): string {
    if (!period) return '—';
    const [year, month] = period.split('-');
    return `${month}/${year}`;
  }

  async function loadStatus() {
    loading = true;
    error = null;
    try {
      status = await fetchRawArchiveStatus();
    } catch (e) {
      error = (e as Error).message;
      status = null;
    } finally {
      loading = false;
    }
  }

  onMount(loadStatus);
</script>

{#if error}
  <AlertBanner title="Internet Archive indisponível" message={error} level="error" />
  <div class="retry-row">
    <button class="btn btn-outline" onclick={loadStatus}>Tentar novamente</button>
  </div>
{:else if loading}
  <div class="status-grid" aria-busy="true" aria-label="Carregando metadados do Internet Archive">
    {#each [1, 2, 3, 4] as _, i (i)}
      <div class="card skeleton-card">
        <div class="skeleton skeleton-label"></div>
        <div class="skeleton skeleton-value"></div>
      </div>
    {/each}
  </div>
{:else if status}
  <div class="source-bar">
    <div>
      <span class="source-kicker">Fonte viva</span>
      <strong>{status.downloadUrl}</strong>
    </div>
    <a class="btn btn-outline btn-sm" href={status.metadataUrl} target="_blank" rel="noopener">
      Ver metadata JSON
    </a>
  </div>

  <div class="status-grid">
    <StatCard
      title="Arquivos raw"
      value={formatInteger(status.rawZipCount)}
      hint={`cobertura ${formatPeriod(status.firstPeriod)} a ${formatPeriod(status.latestPeriod)}`}
    />
    <StatCard
      title="Contratos raw"
      value={status.contractsCount === null ? '—' : formatInteger(status.contractsCount)}
      tone="success"
      hint={status.contractsCountCoverage > 0
        ? `somando contratos_p1.json de ${formatInteger(status.contractsCountCoverage)} ZIPs`
        : 'contagem indisponível via view_archive.php'}
    />
    <StatCard
      title="Volume bruto"
      value={formatBytes(status.rawBytes)}
      hint={`${formatInteger(status.filesCount)} arquivos no item`}
    />
    <StatCard
      title="Último mês"
      value={formatPeriod(status.latestPeriod)}
      tone="warning"
      hint={status.latestContractsCount === null
        ? (status.latestRawFile?.name ?? 'sem arquivo raw detectado')
        : `${formatInteger(status.latestContractsCount)} contratos em ${status.latestRawFile?.name}`}
      href={status.latestRawFile ? `${status.downloadUrl}/${status.latestRawFile.name}` : undefined}
    />
    <StatCard
      title="Atualização IA"
      value={status.lastUpdatedIso ? formatRelativeTime(status.lastUpdatedIso) : '—'}
      hint={status.server ? `${status.server}${status.directory ? status.directory : ''}` : 'metadados do item'}
    />
  </div>

  <dl class="archive-facts">
    <div>
      <dt>Item</dt>
      <dd><a href={status.downloadUrl} target="_blank" rel="noopener">baliza-pncp-raw</a></dd>
    </div>
    <div>
      <dt>Arquivos de metadados</dt>
      <dd>{formatInteger(status.metadataFileCount)}</dd>
    </div>
    <div>
      <dt>Tamanho total do item</dt>
      <dd>{formatBytes(status.totalBytes)}</dd>
    </div>
    <div>
      <dt>Página de contagem</dt>
      <dd>
        {#if status.rawArchives.at(-1)?.contractsPageUrl}
          <a href={status.rawArchives.at(-1)?.contractsPageUrl ?? '#'} target="_blank" rel="noopener">contratos_p1.json do último ZIP</a>
        {:else}
          —
        {/if}
      </dd>
    </div>
    <div>
      <dt>Último ZIP publicado</dt>
      <dd>{status.latestRawFile?.name ?? '—'}</dd>
    </div>
  </dl>
{/if}

<style>
  .status-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: var(--space-4);
    margin-top: var(--space-4);
  }

  .status-grid :global(.stat-value) {
    font-size: clamp(2rem, 4vw, 2.75rem);
    overflow-wrap: anywhere;
  }

  .source-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-4);
    padding: var(--space-4);
    border: 1px solid var(--color-border);
    border-left: 5px solid var(--color-azul);
    background: var(--color-bg);
  }

  .source-bar > div {
    min-width: 0;
    display: grid;
    gap: var(--space-1);
  }

  .source-kicker {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--color-text-dim);
  }

  .source-bar strong {
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    color: var(--color-text);
    overflow-wrap: anywhere;
  }

  .archive-facts {
    margin-top: var(--space-5);
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 0;
    border: 1px solid var(--color-border);
    background: var(--color-bg);
  }

  .archive-facts div {
    padding: var(--space-4);
    border-right: 1px solid var(--color-border);
    border-bottom: 1px solid var(--color-border);
  }

  .archive-facts dt {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--color-text-dim);
  }

  .archive-facts dd {
    margin: var(--space-2) 0 0;
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    color: var(--color-text);
    overflow-wrap: anywhere;
  }

  .archive-facts a {
    color: var(--color-azul);
    text-decoration: underline;
  }

  .skeleton-card {
    min-height: 8rem;
    padding: var(--space-4);
  }

  .skeleton-label {
    height: 0.75rem;
    width: 60%;
  }

  .skeleton-value {
    height: 2.5rem;
    width: 80%;
    margin-top: var(--space-3);
  }

  .retry-row {
    margin-top: var(--space-2);
    display: flex;
    justify-content: center;
  }

  @media (max-width: 720px) {
    .source-bar {
      align-items: stretch;
      flex-direction: column;
    }
  }
</style>
