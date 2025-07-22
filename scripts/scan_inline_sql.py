import re
import os
from pathlib import Path

def scan_for_inline_sql(start_dir: Path) -> dict:
    """Escaneia arquivos Python em busca de strings SQL inline."""
    sql_inventory = {}

    # Padrão de regex para encontrar strings SQL.
    # Procura por SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, PRAGMA, WITH.
    # Tenta capturar strings multilinhas e ignora strings vazias.
    sql_pattern = re.compile(
        r"""
        (['"])
        (
            \s*(?:SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|PRAGMA|WITH)\s+
            (?:.|\n)*?
        )
        \1
        """,
        re.VERBOSE | re.IGNORECASE
    )

    for root, _, files in os.walk(start_dir):
        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        matches = sql_pattern.finditer(content)

                        for match in matches:
                            sql_string = match.group(2).strip()

                            # Classifica a query
                            if sql_string.upper().startswith("SELECT"):
                                query_type = "SELECT"
                            elif sql_string.upper().startswith("INSERT"):
                                query_type = "INSERT"
                            elif sql_string.upper().startswith("UPDATE"):
                                query_type = "UPDATE"
                            elif sql_string.upper().startswith("DELETE"):
                                query_type = "DELETE"
                            elif sql_string.upper().startswith("CREATE"):
                                query_type = "DDL"
                            else:
                                query_type = "OTHER"

                            # Mede a complexidade (simples vs. join)
                            complexity = "JOIN" if "JOIN" in sql_string.upper() else "SIMPLE"

                            if str(file_path) not in sql_inventory:
                                sql_inventory[str(file_path)] = []

                            sql_inventory[str(file_path)].append({
                                "type": query_type,
                                "complexity": complexity,
                                "sql": sql_string
                            })
                except Exception as e:
                    print(f"Erro ao ler o arquivo {file_path}: {e}")

    return sql_inventory

def generate_inventory_report(inventory: dict, output_path: Path):
    """Gera um relatório em markdown do inventário de SQL."""
    with output_path.open('w', encoding='utf-8') as f:
        f.write("# Inventário de SQL Inline\n\n")
        f.write("Este documento mapeia todo o SQL inline encontrado no código Python da aplicação.\n\n")

        if not inventory:
            f.write("Nenhum SQL inline encontrado.\n")
            return

        for file_path, queries in inventory.items():
            f.write(f"## Arquivo: `{file_path}`\n\n")
            f.write(f"Encontradas **{len(queries)}** queries inline.\n\n")

            for i, query in enumerate(queries, 1):
                f.write(f"### Query {i}\n\n")
                f.write(f"- **Tipo**: `{query['type']}`\n")
                f.write(f"- **Complexidade**: `{query['complexity']}`\n")
                f.write("- **SQL Encontrado**:\n")
                f.write("```sql\n")
                f.write(query['sql'] + "\n")
                f.write("```\n\n")

def main():
    """Função principal para escanear e gerar o relatório."""
    repo_root = Path(__file__).parent.parent
    src_dir = repo_root / "src/baliza"
    report_path = repo_root / "docs/sql-inventory.md"

    print(f"Escaneando o diretório `{src_dir}` em busca de SQL inline...")
    inventory = scan_for_inline_sql(src_dir)
    print(f"   Encontrados {sum(len(q) for q in inventory.values())} SQLs inline em {len(inventory)} arquivos.")

    print(f"Gerando relatório de inventário em `{report_path}`...")
    generate_inventory_report(inventory, report_path)
    print("   Relatório gerado com sucesso.")

if __name__ == "__main__":
    main()
