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
    addition: '➕ adição',
    removal: '➖ remoção',
    rename: '✏️ renomeação',
  };
</script>

<section>
  <header>
    <hgroup>
      <h1>📝 Schema changelog</h1>
      <p>Adições, remoções e renomeações de colunas nas tabelas do Baliza, em ordem cronológica inversa.</p>
    </hgroup>
  </header>

  <ol data-testid="schema-changelog-list">
    {#each ENTRIES as entry (entry.date + entry.column)}
      <li data-testid="schema-changelog-entry" data-kind={entry.kind}>
        <article>
          <header>
            <time>{entry.date}</time>
            <small data-badge>{kindLabel[entry.kind]}</small>
            <small><code>{entry.table}</code></small>
          </header>
          <p><code>{entry.column}</code></p>
          <footer><small>{entry.note}</small></footer>
        </article>
      </li>
    {/each}
  </ol>
</section>

