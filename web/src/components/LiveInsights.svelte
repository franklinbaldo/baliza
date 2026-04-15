<script lang="ts">
  const { statsQuery = null } = $props();

  const data = $derived($statsQuery?.data || []);
  const loading = $derived($statsQuery?.isFetching || false);
</script>

<section class="insights-row">
  <div class="container">
    {#if loading}
      <div class="loading-state">
        <div class="skeleton s-insights"></div>
      </div>
    {:else}
      <div class="insights-grid">
        {#each data as item (item.label)}
          <div class="insight-card">
            <span class="insight-label">{item.label}</span>
            <span class="insight-value">{item.value}</span>
          </div>
        {/each}
      </div>
    {/if}
  </div>
</section>

<style>
  .insights-row {
    margin-top: calc(var(--space-md) * -1);
    position: relative;
    z-index: 20;
  }
  .insights-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: var(--space-md);
  }
  .insight-card {
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    padding: var(--space-md);
    border-radius: var(--radius-sm);
    display: flex;
    flex-direction: column;
    gap: 4px;
    box-shadow: 0 4px 15px rgba(0,0,0,0.05);
  }
  .insight-label {
    font-size: var(--font-size-xs);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--color-secondary);
    font-weight: 700;
  }
  .insight-value {
    font-size: var(--font-size-xl);
    font-weight: 900;
    color: var(--color-primary);
  }
  
  .loading-state {
    height: 100px;
    display: flex;
    align-items: center;
  }
  .s-insights {
    width: 100%;
    height: 60px;
  }
</style>
