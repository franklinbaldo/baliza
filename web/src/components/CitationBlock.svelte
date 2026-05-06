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

<section aria-labelledby="citation-title">
  <header>
    <hgroup>
      <h2 id="citation-title">Como citar academicamente</h2>
      <p>
        Gere um bloco BibTeX ancorado no snapshot Parquet mais recente do Internet Archive.
        O URL do item permanece estável mesmo quando novas partições são adicionadas.
      </p>
    </hgroup>
  </header>

  {#if !visible}
    <button
      type="button"
      class="outline"
      data-testid="open-citation"
      onclick={() => { visible = true; }}
      disabled={loading}
      aria-busy={loading}
    >
      {loading ? 'Carregando snapshot…' : 'Gerar citação acadêmica'}
    </button>
  {:else}
    <blockquote>
      <small data-testid="citation-meta">
        {#if snapshotDate}
          Snapshot: <code>{snapshotDate}</code>
        {:else}
          Snapshot: <em>não identificado no manifesto — verifique o /status</em>
        {/if}
        {#if archiveUrl}
          ·
          <a href={archiveUrl} target="_blank" rel="noopener">Arquivo no Internet Archive</a>
        {/if}
      </small>
      <pre data-testid="bibtex-block"><code>{bibtex}</code></pre>
      <footer>
        <button type="button" class="outline" data-testid="copy-bibtex" onclick={copyBibtex}>
          {copied ? 'Copiado ✓' : 'Copiar BibTeX'}
        </button>
      </footer>
    </blockquote>
  {/if}
</section>

