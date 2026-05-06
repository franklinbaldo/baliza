<script lang="ts">
  import { fade } from 'svelte/transition';
  import { resolve } from '../lib/baseUrl';

  interface Props {
    id: string;
    type: 'contratação' | 'município' | 'órgão' | 'fornecedor';
    error?: string;
  }

  let { id, type, error = "Não encontramos registros para este identificador nas APIs oficiais." }: Props = $props();

  // "contratação" is the only feminine noun among the supported types. The
  // heading used to read "ÓRGÃO não encontrada", which reads as broken
  // Portuguese; agree the participle with the gender of the noun.
  const headingSuffix = $derived(type === 'contratação' ? 'não encontrada' : 'não encontrado');
</script>

<article in:fade data-invalid="true">
  <hgroup>
    <small>🔍</small>
    <h2>{type.toUpperCase()} {headingSuffix}</h2>
  </hgroup>
  <p>Identificador: <code>{id}</code></p>
  <p>{error}</p>
  <footer class="actions">
    <a href={resolve('')} role="button">← Voltar para a Busca</a>
    <a href={resolve('sobre')} role="button" class="outline">📚 Metodologia e Dados</a>
  </footer>
</article>

