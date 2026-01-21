
import duckdb
from pathlib import Path

def test_parameterized():
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

        # Try using parameters
        print("Attempting parameterized query...")
        con.execute(f"COPY {dataset}.{table} TO ? (FORMAT PARQUET)", [str(parquet_file)])
        print("Parameterized SQL executed successfully!")
    except Exception as e:
        print(f"Caught exception with parameters: {e}")

if __name__ == "__main__":
    test_parameterized()
