<script lang="ts">
  import { cityState, hydrateCityContext } from '../lib/cityContext.svelte';
  import { resolve } from '../lib/baseUrl';

  hydrateCityContext();

  const cityHref = $derived(resolve(`municipio?ibge=${cityState.ibge}`));

  const ITEMS = [
    {
      title: 'Contratos recentes',
      question: 'O que a prefeitura está comprando esta semana?',
      cta: 'Ver contratos',
      href: 'municipio',
      cityScoped: true,
      icon: '🛒',
    },
    {
      title: 'Concentração de fornecedores',
      question: 'Um CNPJ reaparece com frequência em diferentes órgãos?',
      cta: 'Investigar',
      href: 'explorador',
      cityScoped: false,
      icon: '🏢',
    },
    {
      title: 'Contratações por secretaria',
      question: 'Qual secretaria publicou mais no mês corrente?',
      cta: 'Ver por órgão',
      href: 'municipio',
      cityScoped: true,
      icon: '🏛️',
    },
    {
      title: 'Atas de registro de preços',
      question: 'Quais compromissos vigentes a cidade mantém com fornecedores?',
      cta: 'Consultar atas',
      href: 'atas',
      cityScoped: false,
      icon: '📒',
    },
    {
      title: 'Dispensas e inexigibilidades',
      question: 'Houve pico de contratação sem concorrência?',
      cta: 'Verificar dispensas',
      href: 'dispensas',
      cityScoped: false,
      icon: '⚖️',
    },
    {
      title: 'Variações atípicas no valor',
      question: 'Um fornecedor cobra muito mais caro que a média do mercado?',
      cta: 'Analisar preços',
      href: 'explorador',
      cityScoped: false,
      icon: '📈',
    },
  ] as const;
</script>

<section aria-labelledby="monitor-title">
  <hgroup>
    <small>O que você pode monitorar agora</small>
    <h2 id="monitor-title">Perguntas com resposta, não com planilha</h2>
    <p>
      Os mesmos dados públicos do PNCP — organizados para que você chegue
      à resposta em vez de ter que construir a pergunta em SQL.
    </p>
  </hgroup>

  <div class="grid">
    {#each ITEMS as item (item.title)}
      <article>
        <a href={item.cityScoped ? cityHref : resolve(item.href)}>
          <figure aria-hidden="true" style="font-size: 2.5rem; margin: 0">{item.icon}</figure>
          <strong>{item.title}</strong>
          <p><small>{item.question}</small></p>
          <small>{item.cta} →</small>
        </a>
      </article>
    {/each}
  </div>
</section>
