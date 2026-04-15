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
    position: relative;
    overflow: hidden;
  }
  .card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 4px;
    height: 100%;
    background: var(--color-primary);
    opacity: 0.5;
    transition: opacity var(--transition-base);
  }
  .card:hover::before {
    opacity: 1;
  }
  .stat-value {
    color: var(--color-primary);
    text-shadow: 0 0 10px rgba(61, 165, 104, 0.2);
  }
</style>
