<script lang="ts">
  import { fade } from 'svelte/transition';

  interface Props {
    id: string;
    type: 'contratação' | 'município' | 'órgão';
    error?: string;
  }

  let { id, type, error = "Não encontramos registros para este identificador nas APIs oficiais." }: Props = $props();
</script>

<div class="not-found-container" in:fade>
  <div class="container">
    <div class="error-card">
      <div class="icon">🔍</div>
      <h2>{type.toUpperCase()} não encontrada</h2>
      <p class="id-display">Identificador: <code>{id}</code></p>
      <p class="message">{error}</p>
      
      <div class="actions">
        <a href="/baliza/" class="btn btn-primary">Voltar para a Busca</a>
        <a href="/baliza/sobre" class="btn btn-outline">Metodologia e Dados</a>
      </div>
    </div>
  </div>
</div>

<style>
  .not-found-container {
    padding: var(--space-2xl) 0;
    text-align: center;
  }
  .error-card {
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    padding: var(--space-2xl) var(--space-xl);
    border-radius: var(--radius-box);
    max-width: 600px;
    margin: 0 auto;
  }
  .icon { font-size: 3rem; margin-bottom: var(--space-md); }
  h2 { font-size: var(--font-size-xl); margin-bottom: var(--space-sm); color: var(--color-primary); }
  .id-display { font-family: var(--font-mono); font-size: var(--font-size-xs); opacity: 0.6; margin-bottom: var(--space-md); }
  .message { margin-bottom: var(--space-xl); opacity: 0.8; line-height: 1.6; }
  
  .actions { display: flex; gap: var(--space-md); justify-content: center; flex-wrap: wrap; }
  .btn { 
    display: inline-block; padding: var(--space-sm) var(--space-lg); 
    border-radius: var(--radius-sm); text-decoration: none; font-weight: 700;
    transition: var(--transition-base);
  }
  .btn-primary { background: var(--color-primary); color: white; border: 1px solid var(--color-primary); }
  .btn-outline { border: 1px solid var(--color-primary); color: var(--color-primary); }
  .btn:hover { opacity: 0.9; transform: translateY(-2px); }
</style>
