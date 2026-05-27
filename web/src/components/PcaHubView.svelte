<script lang="ts">
  import { onMount } from 'svelte';
  import { SyncStatsSchema } from '../schema';
  import { getLatestParquetInfo } from '../lib/ia-manifest';
  import { getDuckDB } from '../lib/duckdb';
  import { escapeSqlLiteral } from '../lib/parquetFallback';
  import { resolve } from '../lib/baseUrl';
  import { formatInteger, formatBRL } from '../lib/format';
  import AlertBanner from './AlertBanner.svelte';
  import Skeleton from './Skeleton.svelte';
  import StatCard from './StatCard.svelte';

  interface Props {
    codigo?: string;
  }
  const { codigo }: Props = $props();

  interface CatalogoRow {
    classificacao_superior_nome: string;
    n: number;
    valor_total: number;
  }

  interface AnoPcaRow {
    ano_pca: number;
    n: number;
    valor_total: number;
  }

  let stats = $state<{ row_count: number; partition_count: number } | null>(null);
  let catalogo = $state<CatalogoRow[]>([]);
  let anoRows = $state<AnoPcaRow[]>([]);
  let parquetParticao = $state<string | null>(null);
  let loading = $state(true);
  let error = $state<string | null>(null);
  let noData = $state(false);

  onMount(async () => {
    try {
      const res = await fetch(resolve('data/sync_stats.json'));
      if (res.ok) {
        const raw = await res.json();
        const parsed = SyncStatsSchema.parse(raw);
        const r = parsed.resources?.['pca'];
        if (r) stats = { row_count: r.row_count, partition_count: r.partition_count };
      }
    } catch { /* non-fatal */ }

    try {
      const info = await getLatestParquetInfo('pca');
      if (!info) { noData = true; loading = false; return; }
      parquetParticao = info.dataParticao;

      const { conn } = await getDuckDB();
      const url = info.url;

      if (codigo) {
        const safeUrl = escapeSqlLiteral(url);
        const safeCodigo = escapeSqlLiteral(codigo);
        const sql = `SELECT
          ano_pca,
          COUNT(*) AS n,
          COALESCE(SUM(valor_total), 0) AS valor_total
        FROM read_parquet('${safeUrl}')
        WHERE classificacao_superior_codigo = '${safeCodigo}'
           OR pdm_codigo LIKE '${safeCodigo}%'
        GROUP BY ano_pca
        ORDER BY ano_pca DESC`;

        const result = await conn.query(sql);
        const rows: AnoPcaRow[] = [];
        for (const batch of result.batches) {
          for (let i = 0; i < batch.numRows; i++) {
            rows.push({
              ano_pca: Number(batch.getChildAt(0)?.get(i) ?? 0),
              n: Number(batch.getChildAt(1)?.get(i) ?? 0),
              valor_total: Number(batch.getChildAt(2)?.get(i) ?? 0),
            });
          }
        }
        anoRows = rows;
      } else {
        const sql = `SELECT
          COALESCE(classificacao_superior_nome, 'Sem classificação') AS classificacao_superior_nome,
          COUNT(*) AS n,
          COALESCE(SUM(valor_total), 0) AS valor_total
        FROM read_parquet('${escapeSqlLiteral(url)}')
        GROUP BY classificacao_superior_nome
        ORDER BY valor_total DESC
        LIMIT 25`;

        const result = await conn.query(sql);
        const rows: CatalogoRow[] = [];
        for (const batch of result.batches) {
          for (let i = 0; i < batch.numRows; i++) {
            rows.push({
              classificacao_superior_nome: String(batch.getChildAt(0)?.get(i) ?? ''),
              n: Number(batch.getChildAt(1)?.get(i) ?? 0),
              valor_total: Number(batch.getChildAt(2)?.get(i) ?? 0),
            });
          }
        }
        catalogo = rows;
      }
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
    <h1>Plano de Contratações Anuais (PCA)</h1>
    <p class="lead">
      O PCA consolida o que cada órgão pretende comprar no exercício. É a demanda
      <em>antes</em> da licitação — o estágio mais cedo da cadeia de contratações públicas.
      Baliza espelha o endpoint <code>/v1/pca/atualizacao</code> com partição anual,
      gerando <code>raw-pca-{'{'}YYYY}.zip</code> e o canônico <code>annual_canonical</code>.
    </p>
    <div class="role-tag">
      Papel no atlas: <strong>intenção de demanda</strong> → primeiro elo da cadeia
      PCA → Publicações → Atas → Contratos
    </div>
  </header>

  <!-- Intent disclaimer (BDD: pca-demand scenario 1) -->
  <AlertBanner
    title="Atenção: dados de planejamento, não de contratação"
    message="Esses dados indicam intenção/planejamento informado ao PNCP, não contratação realizada. Todos os valores são estimados."
    level="warning"
  />

  {#if codigo}
    <div class="filter-note">
      Filtrando por código CATMAT/CATSER: <code>{codigo}</code>
      <a href={resolve('pca')} class="clear-filter">× limpar filtro</a>
    </div>
  {/if}

  <!-- Stats -->
  {#if stats && stats.row_count > 0}
    <div class="grid stats-grid">
      <StatCard
        title="Itens de PCA arquivados"
        value={formatInteger(stats.row_count)}
        hint={`${stats.partition_count} anos no Internet Archive`}
      />
      <StatCard
        title="Anos arquivados"
        value={formatInteger(stats.partition_count)}
        hint="cada partição = um ano calendário do PCA federal"
      />
    </div>
  {:else}
    <div class="empty-state" role="status">
      <p>
        Ainda sem partições publicadas — o pipeline <code>pncp-mirror-pca</code>
        executa semanalmente às 06:00 UTC (segunda-feira). Volte em breve ou
        <a href="https://github.com/franklinbaldo/baliza/actions" target="_blank" rel="noopener">acompanhe o CI ↗</a>.
      </p>
    </div>
  {/if}

  {#if codigo}
    <!-- Filtered view: grouped by ano_pca (BDD: pca-demand scenario 3) -->
    <section class="breakdown" aria-labelledby="ano-title">
      <h2 id="ano-title">Demanda planejada por ano — código {codigo}</h2>
      <p class="section-desc">
        Itens do PCA com <code>classificacao_superior_codigo = '{codigo}'</code>
        ou <code>pdm_codigo</code> começando com <code>{codigo}</code>,
        agrupados por <code>ano_pca</code>.
      </p>

      {#if loading}
        <div class="skeleton-rows">
          {#each [1,2,3] as _, i (i)}<Skeleton />{/each}
        </div>
      {:else if error}
        <AlertBanner title="Erro ao consultar parquet" message={error} level="error" />
      {:else if anoRows.length === 0}
        <p class="no-data">
          Nenhum item encontrado para o código <code>{codigo}</code>.
          {parquetParticao ? `Snapshot: ${parquetParticao}` : ''}
        </p>
      {:else}
        {#if parquetParticao}
          <p class="snapshot-note">Snapshot: <code>{parquetParticao}</code></p>
        {/if}
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Ano PCA</th>
                <th class="num">Itens (estimado)</th>
                <th class="num">Valor total planejado (estimado)</th>
              </tr>
            </thead>
            <tbody>
              {#each anoRows as row (row.ano_pca)}
                <tr>
                  <td>
                    <a
                      data-testid="pncp-portal-link"
                      href={`https://pncp.gov.br/app/pca?anoPca=${row.ano_pca}`}
                      target="_blank"
                      rel="noopener noreferrer"
                    >{row.ano_pca}<span aria-hidden="true"> ↗</span></a>
                  </td>
                  <td class="num">{formatInteger(row.n)}</td>
                  <td class="num">{formatBRL(row.valor_total)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </section>
  {:else}
    <!-- Default view: grouped by classificacao_superior_nome -->
    <section class="breakdown" aria-labelledby="catalogo-title">
      <h2 id="catalogo-title">Demanda por classificação de material/serviço</h2>
      <p class="section-desc">
        Itens do PCA agrupados por <code>classificacao_superior_nome</code> (hierarquia CATMAT/CATSER),
        ordenados por valor total planejado. Útil para pesquisadores projetando gasto público por categoria.
      </p>

      {#if loading}
        <div class="skeleton-rows">
          {#each [1,2,3,4,5] as _, i (i)}<Skeleton />{/each}
        </div>
      {:else if error}
        <AlertBanner title="Erro ao consultar parquet" message={error} level="error" />
      {:else if noData || catalogo.length === 0}
        <p class="no-data">
          Sem dados de catálogo ainda.
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
                <th>Classificação</th>
                <th class="num">Itens (estimado)</th>
                <th class="num">Valor total planejado (estimado)</th>
              </tr>
            </thead>
            <tbody>
              {#each catalogo as row (row.classificacao_superior_nome)}
                <tr>
                  <td>{row.classificacao_superior_nome}</td>
                  <td class="num">{formatInteger(row.n)}</td>
                  <td class="num">{formatBRL(row.valor_total)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </section>
  {/if}

  <!-- Schema reference -->
  <section class="schema-panel" aria-labelledby="schema-title">
    <h2 id="schema-title">Esquema do dataset</h2>
    <p class="section-desc">
      Chave primária composta: <code>(id_pca_pncp, numero_item)</code>.
      Partição anual: <code>data_particao = YYYY</code>.
    </p>
    <div class="schema-cols">
      {#each [
        ['id_pca_pncp','VARCHAR','ID do PCA no PNCP'],
        ['numero_item','INTEGER','Número do item dentro do PCA'],
        ['ano_pca','INTEGER','Ano do plano'],
        ['cnpj_orgao','VARCHAR','CNPJ do órgão'],
        ['razao_social_orgao','VARCHAR','Nome do órgão'],
        ['descricao_item','VARCHAR','Descrição do item planejado'],
        ['valor_total','DOUBLE','Valor total planejado (estimado, R$)'],
        ['quantidade_estimada','DOUBLE','Quantidade estimada'],
        ['classificacao_superior_codigo','VARCHAR','Código CATMAT/CATSER'],
        ['classificacao_superior_nome','VARCHAR','Descrição da classificação'],
        ['pdm_codigo','VARCHAR','PDM (código)'],
        ['pdm_descricao','VARCHAR','PDM (descrição)'],
        ['data_desejada','VARCHAR','Data desejada de contratação'],
        ['data_particao','VARCHAR','Partição YYYY (ano)'],
      ] as col (col[0])}
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
        <a href="https://archive.org/details/baliza-pncp-2024" target="_blank" rel="noopener">
          Item PCA 2024 no Internet Archive ↗
        </a>
      </li>
      <li><a href={resolve('arquivo')}>Catálogo completo da coleção Baliza</a></li>
      <li><a href={resolve('explorador')}>Consultar via SQL no navegador (DuckDB)</a></li>
      <li><a href={resolve('desenvolvedores')}>URLs estáveis para Python, R, JS</a></li>
    </ul>
  </section>
</main>

<style>
  .hub { max-width: 860px; margin: 0 auto; padding: var(--space-8) var(--space-4); }
  .hub-header { margin-bottom: var(--space-8); }
  .eyebrow { font-size: var(--text-sm); text-transform: uppercase; letter-spacing: .08em; color: var(--color-text-dim); margin: 0 0 var(--space-2); }
  h1 { font-size: clamp(28px,3vw,42px); font-weight: 300; margin: 0 0 var(--space-3); }
  .lead { font-size: var(--text-md); color: var(--color-text-dim); line-height: 1.6; margin: 0 0 var(--space-3); }
  .role-tag { font-size: var(--text-sm); background: var(--color-surface); border: 1px solid var(--color-border); border-left: 4px solid var(--color-azul); padding: var(--space-2) var(--space-3); border-radius: 0 4px 4px 0; }
  .filter-note { font-size: var(--text-sm); background: var(--color-surface); border: 1px solid var(--color-border); padding: var(--space-2) var(--space-3); border-radius: 4px; margin-bottom: var(--space-4); }
  .clear-filter { margin-left: var(--space-3); color: var(--color-text-dim); text-decoration: none; }
  .clear-filter:hover { color: var(--color-text); }
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
