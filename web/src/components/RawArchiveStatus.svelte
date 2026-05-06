<script lang="ts">
  import { onMount } from 'svelte';
  import AlertBanner from './AlertBanner.svelte';
  import StatCard from './StatCard.svelte';
  import Skeleton from './Skeleton.svelte';
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
  <div class="actions">
    <button class="outline" onclick={loadStatus}>Tentar novamente</button>
  </div>
{:else if loading}
  <div class="grid" aria-busy="true" aria-label="Carregando metadados do Internet Archive">
    {#each [1, 2, 3, 4] as _, i (i)}
      <Skeleton />
    {/each}
  </div>
{:else if status}
  <header>
    <hgroup>
      <small>Fonte viva</small>
      <p><strong>{status.downloadUrl}</strong></p>
    </hgroup>
    <a href={status.metadataUrl} target="_blank" rel="noopener" role="button" class="outline secondary">
      Ver metadata JSON
    </a>
  </header>

  <div class="grid">
    <StatCard
      title="Arquivos brutos (ZIP)"
      value={formatInteger(status.rawZipCount)}
      hint={`cobertura ${formatPeriod(status.firstPeriod)} a ${formatPeriod(status.latestPeriod)}`}
    />
    <StatCard
      title="Contratos extraídos"
      value={status.contractsCount === null ? '—' : formatInteger(status.contractsCount)}
      tone="success"
      hint={status.contractsCountCoverage > 0
        ? `somando contratos_p1.json de ${formatInteger(status.contractsCountCoverage)} ZIPs`
        : 'contagem indisponível via view_archive.php'}
    />
    <StatCard
      title="Volume total"
      value={formatBytes(status.rawBytes)}
      hint={`${formatInteger(status.filesCount)} arquivos no item`}
    />
    <StatCard
      title="Último mês"
      value={formatPeriod(status.latestPeriod)}
      tone="default"
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

  <dl>
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

