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

<a href={resolve(`contratacao?id=${id}`)}>
  <div>
    <span>{id}</span>
    <span>{date ? formatDate(date) : ''}</span>
  </div>
  <p>{truncate(obj, 150)}</p>
  <div>
    {#if buyer}
      <span>{buyer}</span>
    {/if}
    <span>{formatBRL(valor)}</span>
  </div>
</a>

