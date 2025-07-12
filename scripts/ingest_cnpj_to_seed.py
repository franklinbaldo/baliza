import duckdb
import os
import zipfile
# import requests # Commenting out requests as download is manual for now
from io import BytesIO

# --- Configuration ---
# Placeholder for CNPJ data URL. User needs to ensure data is manually downloaded for now.
# CNPJ_DATA_URL = "http://200.152.38.155/CNPJ/DADOS_ABERTOS_CNPJ.zip"

DATA_DIR = "data/cnpj"  # Directory where raw CNPJ data (CSVs or ZIP) should be placed
SEED_DIR = "dbt_baliza/seeds" # dbt seeds directory
OUTPUT_SEED_CSV_FILE = os.path.join(SEED_DIR, "ref_entidades_seed.csv")

# Column definitions for reading raw CSVs
# These are based on the typical layout from Receita Federal. Files have no headers.
ESTABELECIMENTOS_COLS = {
    'cnpj_basico': 'VARCHAR', 'cnpj_ordem': 'VARCHAR', 'cnpj_dv': 'VARCHAR',
    'identificador_matriz_filial': 'VARCHAR', 'nome_fantasia': 'VARCHAR',
    'situacao_cadastral': 'VARCHAR', 'data_situacao_cadastral': 'VARCHAR',
    'motivo_situacao_cadastral': 'VARCHAR', 'nome_cidade_exterior': 'VARCHAR',
    'pais': 'VARCHAR', 'data_inicio_atividade': 'VARCHAR',
    'cnae_fiscal_principal': 'VARCHAR', 'cnae_fiscal_secundaria': 'VARCHAR',
    'tipo_logradouro': 'VARCHAR', 'logradouro': 'VARCHAR', 'numero': 'VARCHAR',
    'complemento': 'VARCHAR', 'bairro': 'VARCHAR', 'cep': 'VARCHAR', 'uf': 'VARCHAR',
    'municipio': 'VARCHAR', 'ddd_1': 'VARCHAR', 'telefone_1': 'VARCHAR',
    'ddd_2': 'VARCHAR', 'telefone_2': 'VARCHAR', 'ddd_fax': 'VARCHAR', 'fax': 'VARCHAR',
    'correio_eletronico': 'VARCHAR', 'situacao_especial': 'VARCHAR',
    'data_situacao_especial': 'VARCHAR'
}

EMPRESAS_COLS = {
    'cnpj_basico': 'VARCHAR', 'razao_social': 'VARCHAR',
    'natureza_juridica': 'VARCHAR', 'qualificacao_do_responsavel': 'VARCHAR',
    'capital_social_da_empresa': 'VARCHAR', 'porte_da_empresa': 'VARCHAR',
    'ente_federativo_responsavel': 'VARCHAR'
}

# Patterns to identify the relevant CSV files (case-insensitive)
ESTABELECIMENTOS_CSV_PATTERN = "ESTABELE"
EMPRESAS_CSV_PATTERN = "EMPRESAS"

def find_csv_files(directory, pattern):
    """Finds files matching a pattern in the given directory."""
    return [os.path.join(directory, f) for f in os.listdir(directory)
            if pattern.upper() in f.upper() and f.upper().endswith(".CSV")]

def process_cnpj_data_to_seed_csv(estab_csv_paths, emp_csv_paths):
    """
    Processes raw CNPJ CSVs using DuckDB and saves the filtered data
    as a CSV file in the dbt seeds directory.
    """
    if not estab_csv_paths or not emp_csv_paths:
        print("Error: Missing Estabelecimentos or Empresas CSV file paths.")
        return False

    print("Processing CNPJ data with DuckDB to create seed file...")
    con = duckdb.connect(database=':memory:', read_only=False)

    # Read Estabelecimentos CSVs
    print(f"Reading Estabelecimento CSVs: {estab_csv_paths}")
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE stg_estabelecimentos AS
            SELECT *
            FROM read_csv_auto({estab_csv_paths},
                                HEADER=FALSE,
                                DELIM=';',
                                NAMES={list(ESTABELECIMENTOS_COLS.keys())},
                                COLUMNS={ESTABELECIMENTOS_COLS},
                                ENCODING='LATIN1',
                                ALL_VARCHAR=TRUE, -- Read all as VARCHAR initially
                                PARALLEL=TRUE);
        """)
    except Exception as e:
        print(f"Error reading Estabelecimento CSVs: {e}")
        con.close()
        return False

    # Read Empresas CSVs
    print(f"Reading Empresas CSVs: {emp_csv_paths}")
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE stg_empresas AS
            SELECT *
            FROM read_csv_auto({emp_csv_paths},
                                HEADER=FALSE,
                                DELIM=';',
                                NAMES={list(EMPRESAS_COLS.keys())},
                                COLUMNS={EMPRESAS_COLS},
                                ENCODING='LATIN1',
                                ALL_VARCHAR=TRUE, -- Read all as VARCHAR initially
                                PARALLEL=TRUE);
        """)
    except Exception as e:
        print(f"Error reading Empresas CSVs: {e}")
        con.close()
        return False

    # Create the filtered and transformed data for the seed
    print("Filtering and transforming data for ref_entidades_seed...")
    # Ensure natureza_juridica is treated as string for range, or cast.
    # RFB codes are strings. The problem description implies numeric comparison.
    # Let's assume they can be safely cast or compared as strings if padded.
    # For safety, we'll cast to INTEGER for a numeric range.
    # situacao_cadastral = '02' (ATIVA)
    # natureza_juridica BETWEEN 1011 AND 2399
    sql_query = f"""
    SELECT
        SUBSTR(est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv, 1, 8) AS cnpj_normalizado,
        emp.razao_social AS razao_social,
        est.uf AS uf
    FROM stg_estabelecimentos est
    JOIN stg_empresas emp ON est.cnpj_basico = emp.cnpj_basico
    WHERE
        (CASE
            WHEN emp.natureza_juridica ~ '^[0-9]+$' THEN CAST(emp.natureza_juridica AS INTEGER)
            ELSE NULL
         END) BETWEEN 1011 AND 2399
    AND est.situacao_cadastral = '02' -- ATIVA
    AND est.uf IS NOT NULL AND TRIM(est.uf) <> '' -- Ensure UF is present
    AND emp.razao_social IS NOT NULL AND TRIM(emp.razao_social) <> ''; -- Ensure razao_social is present
    """

    # Export to CSV for dbt seed
    os.makedirs(SEED_DIR, exist_ok=True)
    print(f"Exporting to CSV seed file: {OUTPUT_SEED_CSV_FILE}")
    try:
        con.execute(f"""
        COPY ({sql_query})
        TO '{OUTPUT_SEED_CSV_FILE}' (HEADER, DELIMITER ',');
        """)
    except Exception as e:
        print(f"Error exporting seed CSV: {e}")
        con.close()
        return False

    count_result = con.execute(f"SELECT COUNT(*) FROM ({sql_query}) sq").fetchone()
    if count_result:
        print(f"Number of entities selected for seed: {count_result[0]}")
        if count_result[0] == 0:
            print("Warning: No entities were selected. Check filters, data, and paths to CSVs.")

    print("Seed CSV generation complete.")
    con.close()
    return True

def main():
    print("--- CNPJ Ingestion to dbt Seed Script ---")

    # Create necessary directories if they don't exist
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SEED_DIR, exist_ok=True)

    # User Action: Ensure raw CNPJ CSV files (Estabelecimentos, Empresas) are in DATA_DIR
    print(f"This script expects raw CNPJ CSV files (e.g., *{ESTABELECIMENTOS_CSV_PATTERN}*.CSV, *{EMPRESAS_CSV_PATTERN}*.CSV)")
    print(f"to be present in the '{DATA_DIR}' directory.")
    print("These files should be extracted from the official ZIPs from Receita Federal.")

    estab_files = find_csv_files(DATA_DIR, ESTABELECIMENTOS_CSV_PATTERN)
    emp_files = find_csv_files(DATA_DIR, EMPRESAS_CSV_PATTERN)

    if not estab_files:
        print(f"ERROR: No Estabelecimento CSV files (containing '{ESTABELECIMENTOS_CSV_PATTERN}') found in '{DATA_DIR}'.")
        print("Please download and extract them from the Receita Federal website.")
        return
    if not emp_files:
        print(f"ERROR: No Empresas CSV files (containing '{EMPRESAS_CSV_PATTERN}') found in '{DATA_DIR}'.")
        print("Please download and extract them from the Receita Federal website.")
        return

    print(f"Found Estabelecimento files: {estab_files}")
    print(f"Found Empresas files: {emp_files}")

    if process_cnpj_data_to_seed_csv(estab_files, emp_files):
        print(f"Successfully created dbt seed file: {OUTPUT_SEED_CSV_FILE}")
    else:
        print("Failed to create dbt seed file. Check errors above.")

if __name__ == "__main__":
    main()
    print("\n--- Reminder ---")
    print(f"1. Ensure your raw, unzipped CNPJ CSV files for 'Estabelecimentos' and 'Empresas' are in '{DATA_DIR}'.")
    print(f"2. The script will generate '{OUTPUT_SEED_CSV_FILE}'.")
    print(f"3. After running this script, you can run 'dbt seed' in your dbt project.")
    print(f"4. Check 'natureza_juridica' and 'situacao_cadastral' filters if no data is selected.")

"""
Manual Steps for User before running this script:
1.  Download the full CNPJ data ZIPs from the official Receita Federal website.
    The data is typically split into multiple ZIP files (e.g., Estabelecimentos0.zip to Estabelecimentos9.zip,
    Empresas0.zip to Empresas9.zip, etc.).
2.  Extract ALL relevant CSV files from these ZIPs.
    For example, each `EstabelecimentosX.zip` will contain a CSV like `K3241.K03200XY.D40111.ESTABELE.CSV` (name might vary).
    Do the same for `EmpresasX.zip`.
3.  Place all these extracted CSV files (e.g., all 10 Estabelecimento CSVs, all 10 Empresa CSVs)
    into the `data/cnpj/` directory in this project.
4.  Run this script: `python scripts/ingest_cnpj_to_seed.py`
5.  After successful execution, a file `dbt_baliza/seeds/ref_entidades_seed.csv` will be created.
6.  You can then run `dbt seed` in your dbt project to load this data into your data warehouse.

The script uses `read_csv_auto` with a list of files, so it can handle multiple
Estabelecimento and Empresa CSVs if they are all placed in the `data/cnpj/` directory.
"""
