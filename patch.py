import re

with open("web/src/components/ContractDetailView.svelte") as f:
    content = f.read()

# Add EntityDetailLayout import and remove EntityNotFound/AlertBanner
content = re.sub(
    r"import EntityNotFound from './EntityNotFound.svelte';\n  import AlertBanner from './AlertBanner.svelte';",
    "import EntityDetailLayout from './EntityDetailLayout.svelte';",
    content,
)

search_block = """<div class="contract-view container">
  {#if !id}
    <EntityNotFound id="ausente" type="contratação" />
  {:else if !parsedId}
    <EntityNotFound
      id={id}
      type="contratação"
      error={`ID fora do padrão PNCP. Formato esperado: ${PNCP_ID_EXAMPLE}.`}
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Carregando contratação">
      <div class="skeleton skeleton-title"></div>
      <div class="skeleton skeleton-meta"></div>
      <div class="skeleton-grid">
        <div class="skeleton skeleton-card"></div>
        <div class="skeleton skeleton-card"></div>
        <div class="skeleton skeleton-card"></div>
        <div class="skeleton skeleton-card"></div>
      </div>
    </div>
  {:else if error}
    <div class="error-wrap">
      <AlertBanner title="Contratação não encontrada" message={error.message} level="error" />
      <div class="back-row">
        <a href={resolve('')} class="btn btn-outline">Voltar à busca</a>
      </div>
    </div>
  {:else if data}
    {#if data.archived}
      <AlertBanner
        title="Dados arquivados"
        message={`PNCP indisponível — exibindo dados arquivados (última consolidação: ${formatParticao(data.archived.dataParticao)}).`}
        level="info"
      />
    {/if}
    <header class="hub-header">
      <span class="kicker">Contratação pública</span>
      <div style="display:flex; align-items:center;">
        <svg width="32" height="32" aria-hidden="true" style="margin-right: 12px; flex-shrink: 0; fill: currentColor;"><use href="#t1"/></svg>
        <h1>{data.objetoContratacao}</h1>
      </div>
      <div class="meta-row">
        <span class="meta-item">ID: {data.numeroControlePNCP || id}</span>
        <span class="meta-item">Publicação: {formatDate(data.dataPublicacaoPncp)}</span>
        {#if effectiveSnapshot}
          <span class="meta-item snapshot-badge" data-testid="snapshot-date">
            Snapshot: <time datetime={effectiveSnapshot}>{formatParticao(effectiveSnapshot)}</time>
          </span>
        {/if}
      </div>

      {#snippet plainLanguageSummary()}
        <div style="display:flex; justify-content:space-between; align-items:flex-start; width: 100%; gap: var(--space-4);">
          <p class="plain-summary" data-testid="plain-language-summary" style="margin:0;">
            {#if data.orgaoEntidade?.razaoSocial}
              <strong>{data.orgaoEntidade.razaoSocial}</strong>
            {:else}
              Um órgão público
            {/if}
            contratou
            {#if data.supplierName || data.supplierCnpj}
              <strong>{data.supplierName ?? data.supplierCnpj}</strong>
            {:else}
              um fornecedor (ainda não identificado no snapshot)
            {/if}
            {#if data.valorTotalEstimado != null}
              pelo valor de <strong>{formatBRL(data.valorTotalEstimado)}</strong>
            {:else}
              com valor não informado
            {/if}
            para <strong>{data.objetoContratacao || 'um objeto não descrito'}</strong>{data.modalidadeNome ? `, via ${data.modalidadeNome}` : ''}.
          </p>
          {#if data.supplierCnpj}
            <button
              class="btn btn-outline"
              style="flex-shrink: 0;"
              onclick={() => addWatch('supplier', data.supplierCnpj!, data.supplierName ?? data.supplierCnpj!)}
              disabled={isSupplierWatched}
            >
              {isSupplierWatched ? '✓ Acompanhando' : 'Acompanhar este fornecedor'}
            </button>
          {/if}
        </div>
      {/snippet}
      {@render plainLanguageSummary()}
    </header>"""

replace_block = """<EntityDetailLayout
  id={id}
  idValid={!!parsedId}
  entityType="contratação"
  idFormatError={`ID fora do padrão PNCP. Formato esperado: ${PNCP_ID_EXAMPLE}.`}
  {loading}
  {error}
  dataReady={!!data}
  archivedParticao={data?.archived?.dataParticao}
  archiveMessage={`PNCP indisponível — exibindo dados arquivados (última consolidação: ${data?.archived?.dataParticao ? formatParticao(data.archived.dataParticao) : ''}).`}
  kicker="CONTRATAÇÃO PÚBLICA"
  iconId="t1"
  title={data?.objetoContratacao || ""}
>
  {#snippet metaRow()}
    {#if data}
      <span class="meta-item">ID: {data.numeroControlePNCP || id}</span>
      <span class="meta-item">Publicação: {formatDate(data.dataPublicacaoPncp)}</span>
      {#if effectiveSnapshot}
        <span class="meta-item snapshot-badge" data-testid="snapshot-date">
          Snapshot: <time datetime={effectiveSnapshot}>{formatParticao(effectiveSnapshot)}</time>
        </span>
      {/if}
    {/if}
  {/snippet}

  {#if data}
    <div style="display:flex; justify-content:space-between; align-items:flex-start; width: 100%; gap: var(--space-lg); margin-bottom: var(--space-xl);">
      <p class="plain-summary" data-testid="plain-language-summary" style="margin:0;">
        {#if data.orgaoEntidade?.razaoSocial}
          <strong>{data.orgaoEntidade.razaoSocial}</strong>
        {:else}
          Um órgão público
        {/if}
        contratou
        {#if data.supplierName || data.supplierCnpj}
          <strong>{data.supplierName ?? data.supplierCnpj}</strong>
        {:else}
          um fornecedor (ainda não identificado no snapshot)
        {/if}
        {#if data.valorTotalEstimado != null}
          pelo valor de <strong>{formatBRL(data.valorTotalEstimado)}</strong>
        {:else}
          com valor não informado
        {/if}
        para <strong>{data.objetoContratacao || 'um objeto não descrito'}</strong>{data.modalidadeNome ? `, via ${data.modalidadeNome}` : ''}.
      </p>
      {#if data.supplierCnpj}
        <button
          class="btn btn-outline"
          style="flex-shrink: 0;"
          onclick={() => addWatch('supplier', data.supplierCnpj!, data.supplierName ?? data.supplierCnpj!)}
          disabled={isSupplierWatched}
        >
          {isSupplierWatched ? '✓ Acompanhando' : 'Acompanhar este fornecedor'}
        </button>
      {/if}
    </div>"""

content = content.replace(search_block, replace_block)
content = content.replace("  {/if}\n</div>", "</EntityDetailLayout>")

# Cleanup CSS
css_search = """  .contract-view {
    padding: var(--space-2xl) 0;
  }

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-title { height: 2.5rem; width: 75%; border-radius: var(--radius-sm); }
  .skeleton-meta  { height: 1rem; width: 50%; border-radius: var(--radius-sm); }
  .skeleton-grid  { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: var(--space-lg); }
  .skeleton-card  { height: 12rem; border-radius: var(--radius-sm); }

  .error-wrap { display: grid; gap: var(--space-md); }
  .back-row { display: flex; }

  .hub-header {
    margin-bottom: var(--space-2xl);
    padding: var(--space-lg);
    border: 1px solid var(--color-border);
    border-bottom-width: 5px;
    border-bottom-color: var(--color-azul);
    border-radius: var(--radius-lg) var(--radius-arch) var(--radius-lg) 0;
    background:
      linear-gradient(color-mix(in srgb, var(--color-bg) 92%, transparent), color-mix(in srgb, var(--color-bg) 92%, transparent)),
      radial-gradient(circle at 100% 0, color-mix(in srgb, var(--color-ouro) 22%, transparent) 0 8rem, transparent 8.2rem),
      var(--color-surface);
    box-shadow: var(--shadow-pool-sm);
  }
  .plain-summary {
    margin-top: var(--space-md);
    padding: var(--space-md);
    background: color-mix(in srgb, var(--color-surface-sunk) 64%, transparent);
    border-left: 5px solid var(--color-primary);
    border-radius: var(--radius-0);
    line-height: 1.6;
    color: var(--color-base-content);
    font-size: var(--font-size-md);
  }
  .plain-summary strong { color: var(--color-text); }
  h1 {
    font-size: clamp(1.75rem, 3.2vw, 3rem);
    margin-top: var(--space-sm);
    line-height: 1.12;
    letter-spacing: 0;
  }
  .meta-row { display: flex; gap: var(--space-md); color: var(--color-secondary); font-size: var(--font-size-sm); margin-top: 8px; flex-wrap: wrap; }
  .snapshot-badge {"""

css_replace = """  .plain-summary {
    padding: var(--space-md);
    background: color-mix(in srgb, var(--color-surface-sunk) 64%, transparent);
    border-left: 5px solid var(--color-primary);
    border-radius: var(--radius-0);
    line-height: 1.6;
    color: var(--color-base-content);
    font-size: var(--font-size-md);
  }
  .plain-summary strong { color: var(--color-text); }

  :global(.snapshot-badge) {"""
content = content.replace(css_search, css_replace)

css_media_search = """  @media (max-width: 720px) {
    .hub-header {
      padding: var(--space-md);
      border-radius: var(--radius-lg) var(--radius-lg) var(--radius-lg) 0;
    }
    .plain-summary {
      font-size: var(--font-size-base);
    }"""
css_media_replace = """  @media (max-width: 720px) {
    .plain-summary {
      font-size: var(--font-size-base);
    }"""
content = content.replace(css_media_search, css_media_replace)

with open("web/src/components/ContractDetailView.svelte", "w") as f:
    f.write(content)
