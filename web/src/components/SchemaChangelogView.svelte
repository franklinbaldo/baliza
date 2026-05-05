<script lang="ts">
  interface ChangeEntry {
    date: string;
    kind: 'addition' | 'removal' | 'rename';
    column: string;
    table: string;
    note: string;
  }

  const ENTRIES: ChangeEntry[] = [
    {
      date: '2025-04',
      kind: 'addition',
      column: 'sha256',
      table: 'manifest',
      note: 'Content-addressed hash for Parquet file integrity verification.',
    },
    {
      date: '2025-04',
      kind: 'addition',
      column: 'row_group_size',
      table: 'manifest',
      note: 'Row-group size used during Parquet write — useful for read optimization.',
    },
    {
      date: '2025-03',
      kind: 'addition',
      column: 'file_type',
      table: 'manifest',
      note: 'Distinguishes monthly_canonical from daily_delta partitions.',
    },
    {
      date: '2025-02',
      kind: 'addition',
      column: 'nome_razao_social_fornecedor',
      table: 'contratos',
      note: 'Supplier legal name sourced from PNCP publicacao endpoint.',
    },
    {
      date: '2025-02',
      kind: 'addition',
      column: 'ni_fornecedor',
      table: 'contratos',
      note: 'Supplier tax ID (CNPJ/CPF) from PNCP publicacao endpoint.',
    },
    {
      date: '2025-01',
      kind: 'addition',
      column: 'uf_sigla',
      table: 'contratos',
      note: 'Two-letter UF code derived from unidadeOrgao for geographic filtering.',
    },
    {
      date: '2024-12',
      kind: 'addition',
      column: 'modalidade_nome',
      table: 'contratos',
      note: 'Human-readable modality name (e.g. Pregão Eletrônico) aligned with PNCP enumeration.',
    },
    {
      date: '2024-11',
      kind: 'rename',
      column: 'data_publicacao → data_publicacao_pncp',
      table: 'contratos',
      note: 'Renamed for clarity: distinguishes PNCP publication date from DOU publication date.',
    },
    {
      date: '2024-10',
      kind: 'addition',
      column: 'valor_inicial',
      table: 'contratos',
      note: 'Initial contracted value before amendments; valorGlobal may reflect amendments.',
    },
    {
      date: '2024-09',
      kind: 'addition',
      column: 'cnpj_orgao',
      table: 'contratos',
      note: 'Contracting agency CNPJ extracted from orgaoEntidade for join performance.',
    },
  ];

  const kindLabel: Record<ChangeEntry['kind'], string> = {
    addition: 'adição',
    removal: 'remoção',
    rename: 'renomeação',
  };
</script>

<div class="changelog">
  <header class="changelog-header">
    <h1>Schema changelog</h1>
    <p class="lead">Adições, remoções e renomeações de colunas nas tabelas do Baliza, em ordem cronológica inversa.</p>
  </header>

  <ol class="changelog-list" data-testid="schema-changelog-list">
    {#each ENTRIES as entry (entry.date + entry.column)}
      <li class="changelog-entry" data-testid="schema-changelog-entry" data-kind={entry.kind}>
        <time class="entry-date">{entry.date}</time>
        <span class="entry-badge entry-badge--{entry.kind}">{kindLabel[entry.kind]}</span>
        <span class="entry-table">{entry.table}</span>
        <code class="entry-column">{entry.column}</code>
        <p class="entry-note">{entry.note}</p>
      </li>
    {/each}
  </ol>
</div>

<style>
  .changelog { padding: var(--space-2xl) 0; }
  .changelog-header { margin-bottom: var(--space-xl); }
  h1 { font-size: var(--font-size-2xl); margin-bottom: var(--space-sm); }
  .lead { color: var(--color-secondary); font-size: var(--font-size-md); line-height: 1.6; }

  .changelog-list {
    list-style: none;
    padding: 0;
    display: grid;
    gap: var(--space-md);
  }

  .changelog-entry {
    display: grid;
    grid-template-columns: 5rem 6rem 6rem 1fr;
    gap: var(--space-sm);
    align-items: start;
    padding: var(--space-md);
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
  }

  .entry-note {
    grid-column: 1 / -1;
    margin: 0;
    color: var(--color-secondary);
    font-size: var(--font-size-sm);
  }

  .entry-date { font-family: var(--font-mono); font-size: var(--font-size-sm); color: var(--color-secondary); }
  .entry-table { font-family: var(--font-mono); font-size: var(--font-size-sm); color: var(--color-secondary); }
  .entry-column { font-family: var(--font-mono); font-size: var(--font-size-sm); font-weight: 600; }

  .entry-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    font-family: var(--font-mono);
  }
  .entry-badge--addition  { background: #d1fae5; color: #065f46; }
  .entry-badge--removal   { background: #fee2e2; color: #991b1b; }
  .entry-badge--rename    { background: #dbeafe; color: #1e40af; }
</style>
