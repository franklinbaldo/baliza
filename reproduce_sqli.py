
import duckdb
from pathlib import Path

def test_exploit():
    dataset = "baliza_raw"
    table = "contratos"

    # Malicious path containing a single quote
    malicious_output = Path("vulnerable'path")
    parquet_file = malicious_output / f"{table}.parquet"

    print(f"Target path: {parquet_file}")

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE SCHEMA {dataset}")
        con.execute(f"CREATE TABLE {dataset}.{table} (id INTEGER)")

        # This is the vulnerable line from src/baliza/cli_simple.py
        sql = f"COPY {dataset}.{table} TO '{parquet_file}' (FORMAT PARQUET)"
        print(f"Executing SQL: {sql}")

        con.execute(sql)
        print("SQL executed successfully (unexpectedly!)")
    except Exception as e:
        print(f"Caught expected exception: {e}")

if __name__ == "__main__":
    test_exploit()
