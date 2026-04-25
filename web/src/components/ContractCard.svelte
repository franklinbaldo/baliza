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

<a href={resolve(`contratacao?id=${id}`)} class="bid-link-card">
  <div class="bid-header">
    <span class="bid-id">{id}</span>
    <span class="bid-date">{date ? formatDate(date) : ''}</span>
  </div>
  <p class="bid-obj">{truncate(obj, 150)}</p>
  <div class="bid-footer">
    {#if buyer}
      <span class="buyer">{buyer}</span>
    {/if}
    <span class="valor">{formatBRL(valor)}</span>
  </div>
</a>

<style>
  .bid-link-card {
    display: block; text-decoration: none; color: inherit; background: var(--color-base-100);
    padding: var(--space-md); border-radius: 0; border: 1px solid var(--color-base-300);
    transition: transform 0.2s, border-color 0.2s, box-shadow 0.2s;
  }
  .bid-link-card:hover, .bid-link-card:focus-visible { transform: translate(-2px, -2px); border-color: var(--color-primary); box-shadow: 4px 4px 0 var(--color-primary); }
  .bid-header { display: flex; justify-content: space-between; margin-bottom: var(--space-sm); font-size: 0.75rem; font-family: var(--font-mono); color: var(--color-secondary); }
  .bid-obj { font-size: var(--font-size-sm); line-height: 1.5; margin-bottom: var(--space-sm); }
  .bid-footer { text-align: right; font-weight: 800; color: var(--color-primary); }
  .bid-footer:has(.buyer) { display: flex; justify-content: space-between; align-items: baseline; font-size: var(--font-size-sm); text-align: left; }
  .bid-footer .buyer { color: var(--color-secondary); overflow-wrap: anywhere; margin-right: var(--space-sm); font-weight: normal; }
</style>
