<script lang="ts">
  import type { Snippet } from 'svelte';
  import { resolve } from '../lib/baseUrl';
  import EntityNotFound from './EntityNotFound.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import Skeleton from './Skeleton.svelte';

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
    title: string;
    metaTestId?: string;

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
    title,
    metaTestId,

    metaRow,
    headerActions,
    children
  }: Props = $props();

  const defaultErrorTitle = $derived(`${entityType.charAt(0).toUpperCase() + entityType.slice(1)} não encontrado`);
</script>

{#if !id}
  <EntityNotFound id="ausente" type={entityType} />
{:else if !idValid}
  <EntityNotFound id={id} type={entityType} error={idFormatError} />
{:else if loading}
  <Skeleton label={`Carregando dados do ${entityType}`} rows={5} />
{:else if error}
  <article data-invalid="true">
    <AlertBanner title={errorTitle || defaultErrorTitle} message={error.message} level="error" />
    <footer><a href={resolve('')} role="button" class="outline">← Voltar à busca</a></footer>
  </article>
{:else if dataReady}
    {#if archivedParticao && archiveMessage}
      <AlertBanner title="Dados arquivados" message={archiveMessage} level="info" />
    {/if}
    <header>
      <hgroup>
        <p class="eyebrow">{kicker}</p>
        <h1>{title}</h1>
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

