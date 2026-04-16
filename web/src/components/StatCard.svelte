<script lang="ts">
  let { title, value } = $props<{ title: string, value: number | string }>();
</script>

<div class="card stat-card">
  <h3 class="stat-label">{title}</h3>
  <p class="stat-value">{value}</p>
</div>

<style>
  /* The global .card styles handle the background, border, and hover effects */
  .stat-card {
    flex: 1;
    min-width: 250px;
  }
  .stat-value {
    text-shadow: 0 0 12px color-mix(in srgb, var(--color-primary) 20%, transparent);
  }
</style>
