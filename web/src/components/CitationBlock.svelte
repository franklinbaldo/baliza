<script lang="ts">
  import { onMount } from 'svelte';
  import { getLatestParquetUrl, getLatestParquetInfo } from '../lib/ia-manifest';

  let snapshotDate = $state<string | null>(null);
  let archiveUrl = $state<string | null>(null);
  let loading = $state(true);
  let visible = $state(false);
  let copied = $state(false);

  onMount(async () => {
    try {
      const info = await getLatestParquetInfo();
      if (info) {
        snapshotDate = info.dataParticao;
        archiveUrl = info.url;
      } else {
        // Fallback: use whatever URL the explorer resolves for the default
        // contratos table. The date may be absent from older manifests.
        archiveUrl = await getLatestParquetUrl();
      }
    } catch {
      archiveUrl = null;
    } finally {
      loading = false;
    }
  });

  // BibTeX block the researcher pastes into their paper. Anchoring on the
  // item URL (not the file URL) keeps the citation valid even if the
  // specific Parquet partition gets rotated, since the IA item page lists
  // every historical snapshot.
  function buildBibtex(): string {
    const year = snapshotDate ? snapshotDate.slice(0, 4) : new Date().getFullYear().toString();
    const urlTarget = archiveUrl ?? 'https://franklinbaldo.github.io/baliza/';
    const note = snapshotDate
      ? `Snapshot de ${snapshotDate}`
      : 'Snapshot mais recente disponível no Internet Archive';
    return `@misc{baliza${year},
  author       = {Baldo, Franklin},
  title        = {Baliza: Arquivo p\\'ublico das contrata\\c{c}\\~oes nacionais brasileiras (PNCP)},
  year         = {${year}},
  howpublished = {\\url{${urlTarget}}},
  note         = {${note}}
}`;
  }

  const bibtex = $derived(buildBibtex());

  async function copyBibtex() {
    try {
      await navigator.clipboard.writeText(bibtex);
      copied = true;
      setTimeout(() => {
        copied = false;
      }, 2500);
    } catch {
      // Clipboard blocked — the pre block is already visible; user can
      // manually select and copy.
    }
  }
</script>

<section class="citation" aria-labelledby="citation-title">
  <h2 id="citation-title">Como citar academicamente</h2>
  <p>
    Gere um bloco BibTeX ancorado no snapshot Parquet mais recente do Internet Archive.
    O URL do item permanece estável mesmo quando novas partições são adicionadas.
  </p>

  {#if !visible}
    <button
      type="button"
      class="btn btn-outline"
      data-testid="open-citation"
      onclick={() => { visible = true; }}
      disabled={loading}
    >
      {loading ? 'Carregando snapshot…' : 'Gerar citação acadêmica'}
    </button>
  {:else}
    <div class="citation-meta" data-testid="citation-meta">
      {#if snapshotDate}
        Snapshot: <code>{snapshotDate}</code>
      {:else}
        Snapshot: <em>não identificado no manifesto — verifique o /status</em>
      {/if}
      {#if archiveUrl}
        <span class="sep">·</span>
        <a href={archiveUrl} target="_blank" rel="noopener" class="ia-link">Arquivo no Internet Archive</a>
      {/if}
    </div>
    <pre class="bibtex" data-testid="bibtex-block"><code>{bibtex}</code></pre>
    <button
      type="button"
      class="btn btn-outline btn-sm"
      data-testid="copy-bibtex"
      onclick={copyBibtex}
    >
      {copied ? 'Copiado ✓' : 'Copiar BibTeX'}
    </button>
  {/if}
</section>

<style>
  .citation {
    margin-top: var(--space-xl);
    padding: var(--space-md);
    background: var(--color-base-200);
    border-left: 4px solid var(--color-primary);
    border-radius: var(--radius-box);
  }
  .citation h2 {
    margin: 0 0 var(--space-sm);
    font-size: var(--font-size-xl);
  }
  .citation p {
    color: var(--color-secondary);
    line-height: 1.6;
    margin-bottom: var(--space-sm);
  }
  .citation-meta {
    font-size: var(--font-size-sm);
    color: var(--color-secondary);
    margin-bottom: var(--space-sm);
  }
  .sep {
    margin: 0 var(--space-xs);
    opacity: 0.6;
  }
  .ia-link {
    color: var(--color-primary);
    font-family: var(--font-mono, monospace);
    font-size: var(--font-size-xs);
    word-break: break-all;
  }
  .bibtex {
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    padding: var(--space-sm);
    border-radius: var(--radius-sm);
    overflow-x: auto;
    font-family: var(--font-mono, monospace);
    font-size: var(--font-size-xs);
    line-height: 1.5;
    margin-bottom: var(--space-sm);
  }
</style>
