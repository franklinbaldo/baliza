"""
Examples of how current raw SQL operations should be converted to Ibis
"""

import ibis


def example_current_vs_ibis_approach():
    """
    Example showing current raw SQL approach vs recommended Ibis approach
    """
    con = ibis.duckdb.connect("data/baliza.duckdb")

    # ===== CURRENT APPROACH (raw SQL) =====

    # 1. Counting records (current)
    raw_count = con.raw_sql(
        "SELECT COUNT(*) as cnt FROM staging.contratacoes"
    ).fetchone()[0]

    # 2. Getting table stats (current)
    raw_stats = con.raw_sql(
        """
        SELECT
            table_name,
            COUNT(*) as row_count,
            SUM(payload_size) as total_bytes
        FROM raw.api_requests
        GROUP BY table_name
    """
    ).fetchall()

    # 3. Creating aggregated table (current)
    con.raw_sql(
        """
        CREATE OR REPLACE TABLE marts.extraction_summary AS
        SELECT
            ingestion_date,
            endpoint,
            COUNT(*) as request_count,
            SUM(payload_size) as total_bytes
        FROM raw.api_requests
        WHERE http_status = 200
        GROUP BY ingestion_date, endpoint
    """
    )

    # ===== RECOMMENDED IBIS APPROACH =====

    # 1. Counting records (Ibis)
    staging_contratacoes = con.table("staging.contratacoes")
    ibis_count = staging_contratacoes.count().execute()

    # 2. Getting table stats (Ibis)
    api_requests = con.table("raw.api_requests")
    ibis_stats = (
        api_requests.group_by("endpoint")
        .aggregate(
            [
                ibis._.count().name("row_count"),
                ibis._.payload_size.sum().name("total_bytes"),
                ibis._.payload_size.mean().name("avg_bytes"),
            ]
        )
        .execute()
    )

    # 3. Creating aggregated table (Ibis)
    extraction_summary = (
        api_requests.filter(api_requests.http_status == 200)
        .group_by(["ingestion_date", "endpoint"])
        .aggregate(
            [
                ibis._.count().name("request_count"),
                ibis._.payload_size.sum().name("total_bytes"),
                (ibis._.payload_size.sum() / 1024 / 1024).round(2).name("total_mb"),
                ibis._.collected_at.min().name("first_extraction"),
                ibis._.collected_at.max().name("last_extraction"),
                ibis._.payload_sha256.nunique().name("unique_payloads"),
            ]
        )
        .order_by([ibis.desc("ingestion_date"), "endpoint"])
    )

    # Create table from Ibis expression
    con.create_table(
        "marts.extraction_summary_ibis", extraction_summary, overwrite=True
    )

    # 4. JSON processing with Ibis (DuckDB JSON functions)
    # Current: json.loads(zlib.decompress(payload).decode())
    # Ibis: Use DuckDB's native JSON functions
    (
        api_requests.filter(api_requests.endpoint == "contratacoes_publicacao")
        .mutate(
            [
                # Extract JSON fields using DuckDB JSON functions
                ibis.literal(
                    "json_extract(payload_decompressed, '$.totalRegistros')"
                ).name("total_registros"),
                ibis.literal(
                    "json_array_length(json_extract(payload_decompressed, '$.data'))"
                ).name("data_count"),
            ]
        )
        .select(["request_id", "endpoint", "total_registros", "data_count"])
    )

    return {
        "raw_count": raw_count,
        "ibis_count": ibis_count,
        "raw_stats": raw_stats,
        "ibis_stats": ibis_stats,
        "extraction_summary": extraction_summary.execute(),
    }


def example_staging_views_with_ibis():
    """
    Example of how staging views should be created with Ibis
    """
    con = ibis.duckdb.connect("data/baliza.duckdb")

    # Current approach: External SQL files
    # staging_contratacoes.sql: CREATE OR REPLACE VIEW staging.contratacoes AS SELECT ...

    # Recommended Ibis approach:
    api_requests = con.table("raw.api_requests")

    # Create staging view for contratacoes
    staging_contratacoes = api_requests.filter(
        [
            api_requests.endpoint == "contratacoes_publicacao",
            api_requests.http_status == 200,
        ]
    ).select(
        [
            "request_id",
            "ingestion_date",
            "endpoint",
            "http_status",
            "payload_sha256",
            "payload_size",
            "collected_at",
            ibis.literal("json_extract(payload_compressed, '$.totalRegistros')").name(
                "total_registros"
            ),
            ibis.literal(
                "json_array_length(json_extract(payload_compressed, '$.data'))"
            ).name("data_count"),
        ]
    )

    # Create the view
    con.create_view("staging.contratacoes_ibis", staging_contratacoes, overwrite=True)

    # Create staging view for contratos
    staging_contratos = api_requests.filter(
        [api_requests.endpoint == "contratos", api_requests.http_status == 200]
    ).select(
        [
            "request_id",
            "ingestion_date",
            "endpoint",
            "payload_sha256",
            "payload_size",
            "collected_at",
        ]
    )

    con.create_view("staging.contratos_ibis", staging_contratos, overwrite=True)

    return {
        "staging_contratacoes": staging_contratacoes.execute(),
        "staging_contratos": staging_contratos.execute(),
    }


def example_data_quality_with_ibis():
    """
    Example of data quality metrics using Ibis statistical functions
    """
    con = ibis.duckdb.connect("data/baliza.duckdb")
    api_requests = con.table("raw.api_requests")

    # Current: Raw SQL with manual calculations
    # Ibis: Use built-in statistical functions
    data_quality = (
        api_requests.group_by(["ingestion_date", "endpoint"])
        .aggregate(
            [
                # Basic metrics
                ibis._.count().name("total_requests"),
                ibis._.payload_size.sum().name("total_bytes"),
                # Statistical metrics (Ibis built-ins)
                ibis._.payload_size.mean().round(2).name("avg_payload_size"),
                ibis._.payload_size.std().round(2).name("std_payload_size"),
                ibis._.payload_size.min().name("min_payload_size"),
                ibis._.payload_size.max().name("max_payload_size"),
                ibis._.payload_size.quantile(0.5).name("median_payload_size"),
                ibis._.payload_size.quantile(0.95).name("p95_payload_size"),
                # Quality indicators
                (ibis._.http_status == 200).sum().name("successful_requests"),
                (ibis._.http_status != 200).sum().name("failed_requests"),
                ibis._.payload_sha256.nunique().name("unique_payloads"),
                # Completeness checks
                ibis._.etag.count().name("records_with_etag"),
                (ibis._.payload_size > 0).sum().name("non_empty_payloads"),
                # Time-based metrics
                ibis._.collected_at.min().name("first_collection"),
                ibis._.collected_at.max().name("last_collection"),
                (ibis._.collected_at.max() - ibis._.collected_at.min()).name(
                    "collection_duration"
                ),
            ]
        )
        .order_by([ibis.desc("ingestion_date"), "endpoint"])
    )

    # Create data quality table
    con.create_table("marts.data_quality_ibis", data_quality, overwrite=True)

    return data_quality.execute()


def example_bulk_operations_with_ibis():
    """
    Example of bulk insert operations using Ibis + pandas
    """
    import pandas as pd

    con = ibis.duckdb.connect("data/baliza.duckdb")

    # Current: Individual INSERT statements in loop
    # for api_request in api_requests:
    #     con.raw_sql(insert_sql, params)

    # Recommended: Bulk operations with pandas
    api_requests_data = [
        {
            "request_id": "uuid1",
            "ingestion_date": "2024-01-01",
            "endpoint": "contratacoes_publicacao",
            "http_status": 200,
            "payload_size": 1024,
        },
        # ... more records
    ]

    # Convert to pandas DataFrame
    df = pd.DataFrame(api_requests_data)

    # Insert using Ibis + pandas (much faster for bulk operations)
    con.table("raw.api_requests")

    # Method 1: Create temporary table and insert
    ibis.memtable(df)
    # If needed for processing

    # Method 2: Direct pandas to DuckDB (fastest)
    # con.register(df, "temp_api_requests")
    # con.raw_sql("INSERT INTO raw.api_requests SELECT * FROM temp_api_requests")

    return df


if __name__ == "__main__":
    # Run examples
    results = example_current_vs_ibis_approach()
    print("Current vs Ibis comparison completed")

    staging_results = example_staging_views_with_ibis()
    print("Staging views with Ibis created")

    quality_results = example_data_quality_with_ibis()
    print("Data quality metrics with Ibis computed")

    bulk_results = example_bulk_operations_with_ibis()
    print("Bulk operations example completed")
