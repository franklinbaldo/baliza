<script lang="ts">
  import { resolve } from '../lib/baseUrl';
  import { formatBRL, formatDate, truncate } from '../lib/format';

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
  <a href={resolve(`contratacao?id=${id}`)}>
    <header>
      <code>{id}</code>
      <small>{date ? formatDate(date) : ''}</small>
    </header>
    <p>{truncate(obj, 150)}</p>
    <footer>
      {#if buyer}<strong>{buyer}</strong>{/if}
      <strong>{formatBRL(valor)}</strong>
    </footer>
  </a>
</article>

