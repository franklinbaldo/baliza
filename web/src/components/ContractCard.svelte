<script lang="ts">
  import { resolve } from '../lib/baseUrl';
  import { formatBRL, formatDate, truncate } from '../lib/format';
  import CopyButton from './CopyButton.svelte';

  interface Props {
    id: string;
    date: string | null;
    obj: string;
    valor: number | null | undefined;
    buyer?: string;
  }

  const { id, date, obj, valor, buyer }: Props = $props();
</script>

<article>
  <header>
    <code>{id}</code>
    <CopyButton text={id} label="Copiar ID PNCP" />
    <small>{date ? formatDate(date) : ''}</small>
  </header>
  <a href={resolve(`contratacao?id=${id}`)}>
    <p>{truncate(obj, 150)}</p>
    <footer>
      {#if buyer}<strong>{buyer}</strong>{/if}
      <strong>{formatBRL(valor)}</strong>
    </footer>
  </a>
</article>

