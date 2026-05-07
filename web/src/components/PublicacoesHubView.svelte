<script lang="ts">
  import { onMount } from 'svelte';
  import { SyncStatsSchema } from '../schema';
  import { getLatestParquetInfo } from '../lib/ia-manifest';
  import { getDuckDB } from '../lib/duckdb';
  import { resolve } from '../lib/baseUrl';
  import { formatInteger, formatBRL } from '../lib/format';
  import AlertBanner from './AlertBanner.svelte';
  import Skeleton from './Skeleton.svelte';
  import StatCard from './StatCard.svelte';

  interface ModalidadeRow {
    modalidade_nome: string;
    n: number;
    valor_total: number;
  }

  let stats = $state<{ row_count: number; partition_count: number } | null>(null);
  let modalidades = $state<ModalidadeRow[]>([]);
  let parquetParticao = $state<string | null>(null);
  let loading = $state(true);
  let error = $state<string | null>(null);
  let noData = $state(false);

  onMount(async () => {
    // Load aggregate stats from sync_stats.json
    try {
      const res = await fetch(resolve('data/sync_stats.json'));
      if (res.ok) {
        const raw = await res.json();
        const parsed = SyncStatsSchema.parse(raw);
        const r = parsed.resources?.['publicacoes'];
        if (r) stats = { row_count: r.row_count, partition_count: r.partition_count };
      }
    } catch { /* non-fatal */ }

    // Load modalidade breakdown from archived parquet
    try {
      const info = await getLatestParquetInfo('publicacoes');
      if (!info) { noData = true; loading = false; return; }
      parquetParticao = info.dataParticao;

      const { conn } = await getDuckDB();
      const url = info.url;
      const sql = `SELECT
        COALESCE(modalidade_nome, 'Desconhecida') AS modalidade_nome,
        COUNT(*) AS n,
        COALESCE(SUM(valor_total_estimado), 0) AS valor_total
      FROM read_parquet('${url.replace(/'/g, "''")}')
      GROUP BY modalidade_nome
      ORDER BY n DESC
      LIMIT 20`;

      const result = await conn.query(sql);
      const rows: ModalidadeRow[] = [];
      for (const batch of result.batches) {
        for (let i = 0; i < batch.numRows; i++) {
          rows.push({
            modalidade_nome: String(batch.getChildAt(0)?.get(i) ?? ''),
            n: Number(batch.getChildAt(1)?.get(i) ?? 0),
            valor_total: Number(batch.getChildAt(2)?.get(i) ?? 0),
          });
        }
      }
      modalidades = rows;
    } catch (e) {
      error = (e as Error).message;
    } finally {
      loading = false;
    }
  });
</script>

<main class="hub container">
  <header class="hub-header">
    <p class="eyebrow">Recurso PNCP</p>
    <h1>Publicações</h1>
    <p class="lead">
      Contratações abertas no PNCP — o ponto de entrada para oportunidades em curso.
      Cada publicação representa uma licitação ou contratação cujo prazo de proposta ainda não encerrou.
      Baliza espelha o endpoint <code>/v1/contratacoes/publicacao</code> com fan-out por modalidade (IDs 1–14),
      gerando partições mensais <code>raw-publicacoes-{'{'}YYYY-MM{'}'}.zip</code>.
    </p>
    <div class="role-tag">
      Papel no atlas: <strong>oportunidade em aberto</strong> → posição <em>antes</em> de contratos e atas
    </div>
  </header>

  <!-- Stats -->
  {#if stats && stats.row_count > 0}
    <div class="grid stats-grid">
      <StatCard
        title="Publicações arquivadas"
        value={formatInteger(stats.row_count)}
        hint={`${stats.partition_count} partições mensais no Internet Archive`}
      />
      <StatCard
        title="Partições"
        value={formatInteger(stats.partition_count)}
        hint="cada partição = um mês de publicações do PNCP"
      />
    </div>
  {:else}
    <div class="empty-state" role="status">
      <p>
        Ainda sem partições publicadas — o pipeline <code>pncp-mirror-publicacoes</code>
        executa diariamente às 05:00 UTC. Volte em breve ou
        <a href="https://github.com/franklinbaldo/baliza/actions" target="_blank" rel="noopener">acompanhe o CI ↗</a>.
      </p>
    </div>
  {/if}

  <!-- Modalidade breakdown (Task 5c) -->
  <section class="breakdown" aria-labelledby="modalidade-title">
    <h2 id="modalidade-title">Contratações por modalidade</h2>
    <p class="section-desc">
      Publicações agrupadas por <code>modalidade_id</code> no snapshot mais recente.
      A modalidade determina o rito: dispensa, pregão, concorrência etc.
    </p>

    {#if loading}
      <div class="skeleton-rows">
        {#each [1,2,3,4,5] as _, i (i)}<Skeleton />{/each}
      </div>
    {:else if error}
      <AlertBanner title="Erro ao consultar parquet" message={error} level="error" />
    {:else if noData || modalidades.length === 0}
      <p class="no-data">
        Sem dados de modalidade ainda.
        {parquetParticao
          ? `Snapshot mais recente: ${parquetParticao}`
          : 'Nenhum parquet disponível no manifesto.'}
      </p>
    {:else}
      {#if parquetParticao}
        <p class="snapshot-note">Snapshot: <code>{parquetParticao}</code></p>
      {/if}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Modalidade</th>
              <th class="num">Publicações</th>
              <th class="num">Valor estimado total</th>
            </tr>
          </thead>
          <tbody>
            {#each modalidades as row (row.modalidade_nome)}
              <tr>
                <td>{row.modalidade_nome}</td>
                <td class="num">{formatInteger(row.n)}</td>
                <td class="num">{formatBRL(row.valor_total)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <!-- Schema reference -->
  <section class="schema-panel" aria-labelledby="schema-title">
    <h2 id="schema-title">Esquema do dataset</h2>
    <p class="section-desc">
      Colunas disponíveis no Parquet arquivado. Chave primária: <code>numero_controle_pncp</code>.
    </p>
    <div class="schema-cols">
      {#each [
        ['numero_controle_pncp','VARCHAR','PK — identificador único'],
        ['cnpj_orgao','VARCHAR','CNPJ do órgão contratante'],
        ['razao_social_orgao','VARCHAR','Nome do órgão'],
        ['modalidade_id','INTEGER','Código da modalidade (1–14)'],
        ['modalidade_nome','VARCHAR','Nome da modalidade'],
        ['objeto_compra','VARCHAR','Objeto da contratação'],
        ['valor_total_estimado','DOUBLE','Valor estimado (R$)'],
        ['data_publicacao_pncp','VARCHAR','Data de publicação no PNCP'],
        ['data_abertura_proposta','VARCHAR','Abertura de propostas'],
        ['data_encerramento_proposta','VARCHAR','Encerramento de propostas'],
        ['uf_sigla','VARCHAR','UF do órgão'],
        ['data_particao','VARCHAR','Partição YYYY-MM'],
      ] as col}
        <div class="col-row">
          <code class="col-name">{col[0]}</code>
          <span class="col-type">{col[1]}</span>
          <span class="col-desc">{col[2]}</span>
        </div>
      {/each}
    </div>
  </section>

  <!-- Archive links -->
  <section class="archive-links" aria-labelledby="archive-title">
    <h2 id="archive-title">Acesso ao arquivo</h2>
    <ul>
      <li>
        <a href="https://archive.org/search?query=subject%3Abaliza-pncp+identifier%3Abaliza-pncp-*" target="_blank" rel="noopener">
          Itens mensais no Internet Archive ↗
        </a>
      </li>
      <li>
        <a href={resolve('arquivo')}>Catálogo completo da coleção Baliza</a>
      </li>
      <li>
        <a href={resolve('explorador')}>Consultar via SQL no navegador (DuckDB)</a>
      </li>
      <li>
        <a href={resolve('desenvolvedores')}>URLs estáveis para Python, R, JS</a>
      </li>
    </ul>
  </section>
</main>

<style>
  .hub { max-width: 860px; margin: 0 auto; padding: var(--space-8) var(--space-4); }
  .hub-header { margin-bottom: var(--space-8); }
  .eyebrow { font-size: var(--text-sm); text-transform: uppercase; letter-spacing: .08em; color: var(--color-text-dim); margin: 0 0 var(--space-2); }
  h1 { font-size: clamp(28px,3vw,42px); font-weight: 300; margin: 0 0 var(--space-3); }
  .lead { font-size: var(--text-md); color: var(--color-text-dim); line-height: 1.6; margin: 0 0 var(--space-3); }
  .role-tag { font-size: var(--text-sm); background: var(--color-surface); border: 1px solid var(--color-border); border-left: 4px solid var(--color-verde); padding: var(--space-2) var(--space-3); border-radius: 0 4px 4px 0; }
  .stats-grid { margin-bottom: var(--space-6); }
  .empty-state { background: var(--color-surface); border: 1px dashed var(--color-border); padding: var(--space-6); text-align: center; border-radius: 4px; margin-bottom: var(--space-6); }
  section { margin-bottom: var(--space-8); padding-top: var(--space-6); border-top: 1px solid var(--color-border); }
  h2 { font-size: var(--text-xl); font-weight: 400; margin: 0 0 var(--space-2); }
  .section-desc { font-size: var(--text-sm); color: var(--color-text-dim); margin: 0 0 var(--space-4); }
  .snapshot-note { font-size: var(--text-xs); color: var(--color-text-dim); margin: 0 0 var(--space-3); }
  .no-data { color: var(--color-text-dim); font-style: italic; }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: var(--text-sm); }
  th, td { text-align: left; padding: var(--space-2) var(--space-3); border-bottom: 1px solid var(--color-border); }
  th { font-weight: 600; color: var(--color-text-dim); font-size: var(--text-xs); text-transform: uppercase; letter-spacing: .06em; }
  .num { text-align: right; }
  .schema-cols { display: grid; gap: var(--space-1); }
  .col-row { display: grid; grid-template-columns: 220px 90px 1fr; gap: var(--space-2); padding: var(--space-1) 0; font-size: var(--text-sm); border-bottom: 1px solid var(--color-border); align-items: baseline; }
  .col-name { font-size: var(--text-xs); }
  .col-type { color: var(--color-text-dim); font-size: var(--text-xs); }
  .col-desc { color: var(--color-text-dim); font-size: var(--text-xs); }
  .archive-links ul { list-style: none; padding: 0; margin: 0; display: grid; gap: var(--space-2); }
  .archive-links li { font-size: var(--text-sm); }
  .skeleton-rows { display: grid; gap: var(--space-2); }
</style>
