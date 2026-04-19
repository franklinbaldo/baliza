<script lang="ts">
  interface Props {
    value: number;
    onchange: (days: number) => void;
    label?: string;
  }

  const { value, onchange, label = 'Período' }: Props = $props();

  const OPTIONS = [30, 90, 180, 365] as const;

  function handleChange(e: Event) {
    onchange(Number((e.target as HTMLSelectElement).value));
  }
</script>

<label class="lookback-window">
  <span class="lookback-label">{label}:</span>
  <select class="lookback-select" value={String(value)} onchange={handleChange}>
    {#each OPTIONS as days (days)}
      <option value={String(days)}>Últimos {days} dias</option>
    {/each}
  </select>
</label>

<style>
  .lookback-window {
    display: inline-flex;
    align-items: center;
    gap: var(--space-sm);
    font-size: var(--font-size-sm);
  }
  .lookback-label { color: var(--color-secondary); }
  .lookback-select {
    padding: 4px 8px;
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    background: var(--color-base-100);
    color: inherit;
    font: inherit;
  }
</style>
