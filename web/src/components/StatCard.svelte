<script lang="ts">
  let { title, value } = $props<{ title: string, value: number | string }>();
</script>

<div class="card">
  <h3 class="stat-label">{title}</h3>
  <p class="stat-value">{value}</p>
</div>

<style>
  .card {
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-box);
    padding: var(--space-md);
    flex: 1;
    min-width: 250px;
  }
</style>
