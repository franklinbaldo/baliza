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

<label>
  <span>{label}:</span>
  <select value={String(value)} onchange={handleChange}>
    {#each OPTIONS as days (days)}
      <option value={String(days)}>Últimos {days} dias</option>
    {/each}
  </select>
</label>

