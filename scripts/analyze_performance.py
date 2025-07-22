import duckdb
from pathlib import Path
import pandas as pd

def get_db_connection(db_path: Path):
    """Estabelece conexão com o banco de dados DuckDB."""
    try:
        return duckdb.connect(database=str(db_path), read_only=True)
    except duckdb.IOException as e:
        print(f"Erro ao conectar ao banco de dados em {db_path}: {e}")
        print("Certifique-se de que o caminho para o banco de dados está correto e que ele existe.")
        return None

def get_all_tables(con) -> list:
    """Lista todas as tabelas no banco de dados."""
    try:
        tables = con.execute("SHOW TABLES;").fetchall()
        return [table[0] for table in tables]
    except duckdb.Error as e:
        print(f"Erro ao listar as tabelas: {e}")
        return []

def analyze_storage(con, tables: list) -> pd.DataFrame:
    """Executa PRAGMA storage_info para cada tabela e retorna um DataFrame."""
    all_storage_info = []
    for table in tables:
        try:
            storage_info = con.execute(f"PRAGMA storage_info('{table}');").df()
            storage_info['table_name'] = table
            all_storage_info.append(storage_info)
        except duckdb.Error as e:
            print(f"Não foi possível analisar a tabela '{table}': {e}")
            continue

    if not all_storage_info:
        return pd.DataFrame()

    return pd.concat(all_storage_info, ignore_index=True)

def generate_performance_report(storage_df: pd.DataFrame, output_path: Path):
    """Gera um relatório de baseline de performance em markdown."""
    with output_path.open('w', encoding='utf-8') as f:
        f.write("# Análise de Performance e Armazenamento (Baseline)\n\n")
        f.write("Este documento serve como um baseline do estado atual do banco de dados antes das otimizações.\n\n")

        if storage_df.empty:
            f.write("## Análise de Armazenamento\n\n")
            f.write("Nenhuma informação de armazenamento pôde ser coletada. O banco de dados pode estar vazio ou inacessível.\n")
            return

        total_size_bytes = storage_df['size'].sum()
        total_size_mb = total_size_bytes / (1024 * 1024)

        f.write(f"## Resumo Geral\n\n")
        f.write(f"- **Tamanho Total do Banco (estimado)**: {total_size_mb:.2f} MB\n")
        f.write(f"- **Total de Tabelas Analisadas**: {len(storage_df['table_name'].unique())}\n\n")

        f.write("## Análise de Armazenamento por Tabela\n\n")

        # Agrega os dados por tabela
        summary = storage_df.groupby('table_name').agg(
            total_size_mb=('size', lambda x: x.sum() / (1024*1024)),
            row_count=('row_count', 'first'), # Pega o primeiro valor, já que é o mesmo para todas as colunas da tabela
            compression=('compression', lambda x: x.mode()[0] if not x.mode().empty else 'N/A')
        ).sort_values(by='total_size_mb', ascending=False).reset_index()

        f.write(summary.to_markdown(index=False))

        f.write("\n\n## Detalhes de Compressão por Coluna (Top 10 Maiores Colunas)\n\n")

        # Detalhes das colunas, ordenado por tamanho
        column_details = storage_df.sort_values(by='size', ascending=False).head(10)

        f.write(column_details[['table_name', 'column_name', 'compression', 'size']].to_markdown(index=False))

def main():
    """Função principal para analisar a performance e gerar o relatório."""
    repo_root = Path(__file__).parent.parent
    # O caminho do banco de dados pode precisar ser ajustado dependendo da configuração do projeto
    db_path = repo_root / "baliza.db"
    report_path = repo_root / "docs/performance-baseline.md"

    print(f"Conectando ao banco de dados em `{db_path}`...")
    con = get_db_connection(db_path)

    if not con:
        print("Não foi possível conectar ao banco de dados. Abortando a análise.")
        # Gera um relatório vazio para indicar que a etapa foi executada
        generate_performance_report(pd.DataFrame(), report_path)
        return

    print("Listando tabelas...")
    tables = get_all_tables(con)
    print(f"   Encontradas {len(tables)} tabelas.")

    if not tables:
        print("Nenhuma tabela encontrada no banco de dados.")
        generate_performance_report(pd.DataFrame(), report_path)
        con.close()
        return

    print("Analisando o armazenamento de cada tabela...")
    storage_df = analyze_storage(con, tables)

    if not storage_df.empty:
        print(f"   Análise de armazenamento concluída. {len(storage_df)} registros de metadados de colunas coletados.")
    else:
        print("   Nenhuma informação de armazenamento foi coletada.")

    print(f"Gerando relatório de performance em `{report_path}`...")
    generate_performance_report(storage_df, report_path)
    print("   Relatório gerado com sucesso.")

    con.close()

if __name__ == "__main__":
    main()
