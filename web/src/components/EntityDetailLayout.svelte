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

<div>
  {#if !id}
    <EntityNotFound id="ausente" type={entityType} />
  {:else if !idValid}
    <EntityNotFound
      id={id}
      type={entityType}
      error={idFormatError}
    />
  {:else if loading}
    <div aria-busy="true" aria-label={`Carregando dados do ${entityType}`}>
      <div></div>
      <div></div>
      {#if hasStatSkeleton}
        <div></div>
      {/if}
      {#each [1, 2, 3] as _, i (i)}
        <div></div>
      {/each}
    </div>
  {:else if error}
    <div>
      <AlertBanner title={errorTitle || defaultErrorTitle} message={error.message} level="error" />
      <div>
        <a href={resolve('')}>Voltar à busca</a>
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
    <header>
      <div>
        <div>
          <span>{kicker}</span>
          <div>
            <svg width="32" height="32" aria-hidden="true"><use href={`#${iconId}`}/></svg>
            <h1>{title}</h1>
          </div>
          {#if metaRow}
            <div data-testid={metaTestId}>
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

