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
    metaTestId,

    hasStatSkeleton = false,

    metaRow,
    headerActions,
    children
  }: Props = $props();

  const defaultErrorTitle = $derived(`${entityType.charAt(0).toUpperCase() + entityType.slice(1)} não encontrado`);
</script>

<article>
  {#if !id}
    <EntityNotFound id="ausente" type={entityType} />
  {:else if !idValid}
    <EntityNotFound id={id} type={entityType} error={idFormatError} />
  {:else if loading}
    <article aria-busy="true" class="is-skeleton" aria-label={`Carregando dados do ${entityType}`}>
      <p>&nbsp;</p>
      <p>&nbsp;</p>
      {#if hasStatSkeleton}<p>&nbsp;</p>{/if}
      {#each [1, 2, 3] as _, i (i)}<p>&nbsp;</p>{/each}
    </article>
  {:else if error}
    <article data-invalid="true">
      <AlertBanner title={errorTitle || defaultErrorTitle} message={error.message} level="error" />
      <footer><a href={resolve('')} role="button" class="outline">Voltar à busca</a></footer>
    </article>
  {:else if dataReady}
    {#if archivedParticao && archiveMessage}
      <AlertBanner title="Dados arquivados" message={archiveMessage} level="info" />
    {/if}
    <header>
      <hgroup>
        <small><mark>{kicker}</mark></small>
        <h1>
          <svg width="32" height="32" aria-hidden="true"><use href={`#${iconId}`}/></svg>
          {title}
        </h1>
        {#if metaRow}
          <p data-testid={metaTestId}>{@render metaRow()}</p>
        {/if}
      </hgroup>
      {#if headerActions}
        <div class="actions">{@render headerActions()}</div>
      {/if}
    </header>

    {#if children}{@render children()}{/if}
  {/if}
</article>

