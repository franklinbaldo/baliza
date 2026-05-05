<script lang="ts">
  import type { Snippet } from 'svelte';
  import { resolve } from '../lib/baseUrl';
  import EntityNotFound from './EntityNotFound.svelte';
  import AlertBanner from './AlertBanner.svelte';

  interface Props {
    id?: string;
    idValid: boolean;
    entityType: "fornecedor" | "contratação" | "município" | "órgão";
    idFormatError: string;

    loading: boolean;
    error: Error | null;
    errorTitle?: string;

    dataReady: boolean;
    archivedParticao?: string | null;
    archiveMessage?: string;

    kicker: string;
    iconId: string;
    title: string;
    headerStyle?: string;
    metaTestId?: string;

    hasStatSkeleton?: boolean;

    metaRow?: Snippet;
    headerActions?: Snippet;
    children?: Snippet;
  }

  const {
    id = "",
    idValid,
    entityType,
    idFormatError,

    loading,
    error,
    errorTitle,

    dataReady,
    archivedParticao,
    archiveMessage,

    kicker,
    iconId,
    title,
    headerStyle = "",
    metaTestId,

    hasStatSkeleton = false,

    metaRow,
    headerActions,
    children
  }: Props = $props();

  const defaultErrorTitle = $derived(`${entityType.charAt(0).toUpperCase() + entityType.slice(1)} não encontrado`);
</script>

<div class="entity-detail container">
  {#if !id}
    <EntityNotFound id="ausente" type={entityType} />
  {:else if !idValid}
    <EntityNotFound
      id={id}
      type={entityType}
      error={idFormatError}
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label={`Carregando dados do ${entityType}`}>
      <div class="skeleton skeleton-title"></div>
      <div class="skeleton skeleton-meta"></div>
      {#if hasStatSkeleton}
        <div class="skeleton skeleton-stat"></div>
      {/if}
      {#each [1, 2, 3] as _, i (i)}
        <div class="skeleton skeleton-bid"></div>
      {/each}
    </div>
  {:else if error}
    <div class="error-wrap">
      <AlertBanner title={errorTitle || defaultErrorTitle} message={error.message} level="error" />
      <div class="back-row">
        <a href={resolve('')} class="btn btn-outline">Voltar à busca</a>
      </div>
    </div>
  {:else if dataReady}
    {#if archivedParticao && archiveMessage}
      <AlertBanner
        title="Dados arquivados"
        message={archiveMessage}
        level="info"
      />
    {/if}
    <header class="hub-header" style={headerStyle}>
      <div style="display:flex; justify-content:space-between; align-items:flex-start; width: 100%;">
        <div>
          <span class="kicker">{kicker}</span>
          <div style="display:flex; align-items:center;">
            <svg width="32" height="32" aria-hidden="true" style="margin-right: 12px; flex-shrink: 0; fill: currentColor;"><use href={`#${iconId}`}/></svg>
            <h1>{title}</h1>
          </div>
          {#if metaRow}
            <div class="meta-row" data-testid={metaTestId}>
              {@render metaRow()}
            </div>
          {/if}
        </div>
        {#if headerActions}
          {@render headerActions()}
        {/if}
      </div>
    </header>

    {#if children}
      {@render children()}
    {/if}
  {/if}
</div>

<style>
  .entity-detail { padding: var(--space-2xl) 0; }

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-title { height: 2rem; width: 60%; border-radius: var(--radius-sm); }
  .skeleton-meta  { height: 1rem; width: 40%; border-radius: var(--radius-sm); }
  .skeleton-stat  { height: 5rem; width: 200px; border-radius: var(--radius-sm); }
  .skeleton-bid   { height: 5rem; border-radius: var(--radius-sm); }

  .error-wrap { display: grid; gap: var(--space-md); }
  .back-row { display: flex; }

  .hub-header { margin-bottom: var(--space-2xl); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  h1 { font-size: var(--font-size-2xl); margin-top: var(--space-sm); }
  .meta-row { display: flex; gap: var(--space-md); color: var(--color-secondary); font-size: var(--font-size-sm); margin-top: 4px; }
</style>
