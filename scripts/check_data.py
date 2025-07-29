#!/usr/bin/env python3
"""
Verifica os dados armazenados no DuckDB após execução do pipeline.
"""

import duckdb


def check_database(db_path):
    print(f"\n🔍 Verificando: {db_path}")
    con = duckdb.connect(db_path)

    try:
        # Lista todas as tabelas
        result = con.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """).fetchall()

        print(f"📋 Tabelas encontradas: {len(result)}")

        for schema, table in result:
            try:
                count = con.execute(
                    f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                ).fetchone()[0]
                print(f"   📊 {schema}.{table}: {count} registros")

                # Mostra alguns dados de exemplo se existirem
                if count > 0:
                    sample = con.execute(
                        f'SELECT * FROM "{schema}"."{table}" LIMIT 2'
                    ).fetchall()
                    if sample:
                        print(f"      Exemplo: {sample[0]}")

            except Exception as e:
                print(f"   ❌ Erro ao consultar {schema}.{table}: {e}")

    except Exception as e:
        print(f"❌ Erro ao listar tabelas: {e}")

    finally:
        con.close()


def main():
    print("🔍 VERIFICANDO DADOS DO PIPELINE")
    print("=" * 50)

    databases = [
        "baliza_test_sync.duckdb",
        "baliza_test_backfill.duckdb",
        "baliza_test_specialized.duckdb",
    ]

    for db in databases:
        try:
            check_database(db)
        except Exception as e:
            print(f"❌ Erro ao verificar {db}: {e}")

    print("\n✅ Verificação concluída!")


if __name__ == "__main__":
    main()
