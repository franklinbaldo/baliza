"""
Internet Archive Federation for DuckDB.

This module implements archive-first data access, where DuckDB federated queries
read directly from Internet Archive Parquet files instead of local storage.
"""

import hashlib
import json
from pathlib import Path

import duckdb
import requests
from internetarchive import search_items


class InternetArchiveFederation:
    """Manages DuckDB federation with Internet Archive stored data."""

    def __init__(self, baliza_db_path: str | Path = None):
        # Use standard XDG directories
        import os
        if not os.getenv("BALIZA_PRODUCTION"):
            # Development mode
            data_dir = Path.cwd() / "data"
            cache_dir = Path.cwd() / ".cache"
        else:
            # Production mode
            data_dir = Path.home() / ".local" / "share" / "baliza"
            cache_dir = Path.home() / ".cache" / "baliza"
        
        if baliza_db_path is None:
            self.baliza_db_path = str(data_dir / "baliza.duckdb")
        else:
            self.baliza_db_path = str(baliza_db_path)
            
        self.cache_dir = cache_dir / "ia_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # IA collection identifier pattern
        self.ia_collection = "baliza-pncp"

    def discover_ia_items(self, limit: int = 1000) -> list[dict]:
        """
        Discover all Baliza items in Internet Archive.

        Returns list of items with metadata for federation.
        """
        print("🔍 Discovering Baliza items in Internet Archive...")

        items = []
        try:
            # Search for Baliza items in IA
            search_query = f"collection:{self.ia_collection} AND title:baliza-*"

            for item in search_items(search_query).iter_as_items():
                item_metadata = {
                    "identifier": item.identifier,
                    "title": item.metadata.get("title", ""),
                    "date": item.metadata.get("date", ""),
                    "size": item.metadata.get("size", 0),
                    "files": [],
                }

                # Get Parquet files from this item
                for file_info in item.get_files():
                    if file_info.name.endswith(".parquet"):
                        item_metadata["files"].append(
                            {
                                "name": file_info.name,
                                "size": file_info.size,
                                "url": f"https://archive.org/download/{item.identifier}/{file_info.name}",
                                "sha1": file_info.sha1,
                                "md5": file_info.md5,
                            }
                        )

                if item_metadata["files"]:  # Only include items with Parquet files
                    items.append(item_metadata)

        except Exception as e:
            print(f"❌ Error discovering IA items: {e}")
            return []
        else:
            print(f"✅ Found {len(items)} Baliza items with Parquet files")
            return items

    def create_ia_catalog(self) -> str:
        """
        Create a catalog of all available IA data for federation.

        Returns path to catalog database.
        """
        # Use same data directory logic
        import os
        if not os.getenv("BALIZA_PRODUCTION"):
            data_dir = Path.cwd() / "data"
        else:
            data_dir = Path.home() / ".local" / "share" / "baliza"
            
        catalog_db = str(data_dir / "ia_catalog.duckdb")

        print("📋 Creating Internet Archive data catalog...")

        items = self.discover_ia_items()

        conn = duckdb.connect(catalog_db)

        # Create catalog schema
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS catalog
        """)

        # IA items catalog
        conn.execute("""
            CREATE OR REPLACE TABLE catalog.ia_items (
                identifier VARCHAR PRIMARY KEY,
                title VARCHAR,
                date DATE,
                total_size_bytes BIGINT,
                parquet_files_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # IA files catalog
        conn.execute("""
            CREATE OR REPLACE TABLE catalog.ia_files (
                identifier VARCHAR,
                file_name VARCHAR,
                file_size_bytes BIGINT,
                download_url VARCHAR,
                sha1_checksum VARCHAR,
                md5_checksum VARCHAR,
                file_type VARCHAR,
                data_date DATE,
                endpoint_type VARCHAR,
                PRIMARY KEY (identifier, file_name)
            )
        """)

        # Insert discovered items
        for item in items:
            conn.execute(
                """
                INSERT INTO catalog.ia_items (identifier, title, date, total_size_bytes, parquet_files_count)
                VALUES (?, ?, ?, ?, ?)
            """,
                [
                    item["identifier"],
                    item["title"],
                    item.get("date"),
                    item.get("size", 0),
                    len(item["files"]),
                ],
            )

            # Insert files
            for file_info in item["files"]:
                # Parse data date and endpoint from filename
                # Expected format: contratos-2024-01.parquet
                filename = file_info["name"]
                data_date = None
                endpoint_type = "contratos"  # default

                if "-" in filename:
                    parts = filename.replace(".parquet", "").split("-")
                    if len(parts) >= 3:
                        endpoint_type = parts[0]
                        try:
                            # Try to parse YYYY-MM format
                            year_month = f"{parts[1]}-{parts[2]}"
                            data_date = f"{year_month}-01"  # First day of month
                        except (IndexError, ValueError):
                            # Skip if filename doesn't match expected format
                            pass

                conn.execute(
                    """
                    INSERT INTO catalog.ia_files
                    (identifier, file_name, file_size_bytes, download_url, sha1_checksum,
                     md5_checksum, file_type, data_date, endpoint_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    [
                        item["identifier"],
                        file_info["name"],
                        file_info.get("size", 0),
                        file_info["url"],
                        file_info.get("sha1"),
                        file_info.get("md5"),
                        "parquet",
                        data_date,
                        endpoint_type,
                    ],
                )

        conn.close()

        print(f"✅ Created IA catalog with {len(items)} items")
        return catalog_db

    def get_cached_file_path(self, url: str) -> Path:
        """Get local cache path for a remote file."""
        url_hash = hashlib.sha256(url.encode()).hexdigest()[
            :16
        ]  # Use first 16 chars for shorter filenames
        filename = url.split("/")[-1]
        return self.cache_dir / f"{url_hash}_{filename}"

    def download_with_cache(self, url: str, expected_size: int | None = None) -> Path:
        """
        Download file with local caching.

        Returns path to local cached file.
        """
        cache_path = self.get_cached_file_path(url)

        # Check if already cached and valid
        if cache_path.exists():
            if expected_size is None or cache_path.stat().st_size == expected_size:
                print(f"📦 Using cached file: {cache_path.name}")
                return cache_path

        print(f"⬇️ Downloading: {url}")

        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with cache_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        except Exception as e:
            print(f"❌ Error downloading {url}: {e}")
            raise
        else:
            print(f"✅ Downloaded and cached: {cache_path.name}")
            return cache_path

    def _setup_federated_db(self) -> tuple[str, any]:
        """Set up database connection and catalog for federation."""
        catalog_db = self.create_ia_catalog()
        conn = duckdb.connect(self.baliza_db_path)
        conn.execute(f"ATTACH '{catalog_db}' AS ia_catalog")
        conn.execute("CREATE SCHEMA IF NOT EXISTS federated")
        return catalog_db, conn

    def _get_ia_files(self, conn) -> list[tuple]:
        """Get available IA files from catalog."""
        return conn.execute("""
            SELECT identifier, file_name, download_url, data_date, endpoint_type, file_size_bytes
            FROM ia_catalog.catalog.ia_files
            WHERE file_type = 'parquet'
            ORDER BY data_date DESC
        """).fetchall()

    def _group_files_by_endpoint(self, ia_files: list[tuple]) -> dict:
        """Group IA files by endpoint type."""
        endpoints = {}
        for (
            identifier,
            file_name,
            download_url,
            data_date,
            endpoint_type,
            file_size,
        ) in ia_files:
            if endpoint_type not in endpoints:
                endpoints[endpoint_type] = []
            endpoints[endpoint_type].append(
                {
                    "identifier": identifier,
                    "file_name": file_name,
                    "url": download_url,
                    "data_date": data_date,
                    "size": file_size,
                }
            )
        return endpoints

    def _create_endpoint_view(
        self, conn, endpoint_type: str, files: list[dict]
    ) -> None:
        """Create federated view for a specific endpoint type."""
        print(f"📋 Creating federated view for {endpoint_type} ({len(files)} files)")

        # Download and cache first few files for schema detection
        sample_files = files[:3]
        cached_paths = []

        for file_info in sample_files:
            try:
                cached_path = self.download_with_cache(
                    file_info["url"], file_info.get("size")
                )
                cached_paths.append(str(cached_path))
            except Exception as e:
                print(f"⚠️ Skipping file {file_info['file_name']}: {e}")

        if not cached_paths:
            print(f"❌ No valid files for {endpoint_type}")
            return

        # Create union view
        union_clauses = []
        for i, file_info in enumerate(files[: len(cached_paths)]):
            file_path = cached_paths[i]
            union_clauses.append(f"""
                SELECT
                    '{file_info["identifier"]}' AS ia_identifier,
                    '{file_info["file_name"]}' AS ia_file_name,
                    '{file_info["data_date"]}' AS ia_data_date,
                    *
                FROM '{file_path}'
            """)

        if union_clauses:
            federated_view = f"federated.{endpoint_type}_ia"
            union_sql = " UNION ALL ".join(union_clauses)
            conn.execute(f"CREATE OR REPLACE VIEW {federated_view} AS {union_sql}")
            print(f"✅ Created {federated_view} with {len(union_clauses)} files")

    def _create_unified_view(self, conn) -> None:
        """Create unified view combining IA and local data."""
        conn.execute("""
            CREATE OR REPLACE VIEW federated.contratos_unified AS
            SELECT
                'ia' AS data_source,
                ia_identifier,
                ia_file_name,
                ia_data_date::DATE AS source_date,
                *
            FROM federated.contratos_ia
            UNION ALL
            SELECT
                'local' AS data_source,
                run_id AS ia_identifier,
                NULL AS ia_file_name,
                baliza_data_date AS source_date,
                *
            FROM psa.contratos_raw
            WHERE baliza_data_date NOT IN (
                SELECT DISTINCT ia_data_date::DATE
                FROM federated.contratos_ia
                WHERE ia_data_date IS NOT NULL
            )
        """)

    def create_federated_views(self) -> None:
        """
        Create DuckDB views that federate with Internet Archive data.
        """
        print("🔗 Creating federated views for IA data...")

        catalog_db, conn = self._setup_federated_db()

        try:
            ia_files = self._get_ia_files(conn)
            print(f"📊 Found {len(ia_files)} Parquet files in IA")

            if not ia_files:
                print("⚠️ No IA files found, creating empty federated views")
                conn.execute("""
                    CREATE OR REPLACE VIEW federated.contratos_ia AS
                    SELECT * FROM psa.contratos_raw WHERE 1=0
                """)
                return

            endpoints = self._group_files_by_endpoint(ia_files)

            # Create federated views for each endpoint
            for endpoint_type, files in endpoints.items():
                self._create_endpoint_view(conn, endpoint_type, files)

            self._create_unified_view(conn)

        finally:
            conn.close()

        print("✅ Federated views created successfully")

    def get_data_availability(self) -> dict:
        """Get overview of data availability from IA vs local."""
        conn = duckdb.connect(self.baliza_db_path)

        try:
            # Try to get stats from federated views
            ia_stats = conn.execute("""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT ia_data_date) as unique_dates,
                    MIN(ia_data_date) as earliest_date,
                    MAX(ia_data_date) as latest_date
                FROM federated.contratos_ia
            """).fetchone()

            local_stats = conn.execute("""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT baliza_data_date) as unique_dates,
                    MIN(baliza_data_date) as earliest_date,
                    MAX(baliza_data_date) as latest_date
                FROM psa.contratos_raw
            """).fetchone()

            return {
                "internet_archive": {
                    "total_records": ia_stats[0] if ia_stats else 0,
                    "unique_dates": ia_stats[1] if ia_stats else 0,
                    "earliest_date": ia_stats[2] if ia_stats else None,
                    "latest_date": ia_stats[3] if ia_stats else None,
                },
                "local_storage": {
                    "total_records": local_stats[0] if local_stats else 0,
                    "unique_dates": local_stats[1] if local_stats else 0,
                    "earliest_date": local_stats[2] if local_stats else None,
                    "latest_date": local_stats[3] if local_stats else None,
                },
            }

        except Exception as e:
            print(f"Error getting data availability: {e}")
            return {}
        finally:
            conn.close()

    def update_federation(self) -> None:
        """Update federated views with latest IA data."""
        print("🔄 Updating Internet Archive federation...")

        try:
            self.create_federated_views()

            availability = self.get_data_availability()

            print(f"""
📊 Data Availability Summary:
Internet Archive: {availability.get("internet_archive", {}).get("total_records", 0):,} records
Local Storage: {availability.get("local_storage", {}).get("total_records", 0):,} records

✅ Federation updated successfully
            """)

        except Exception as e:
            print(f"❌ Error updating federation: {e}")
            raise


def main():
    """CLI entry point for IA federation management."""
    import typer

    app = typer.Typer(help="Internet Archive Federation for Baliza")

    @app.command()
    def discover():
        """Discover available data in Internet Archive."""
        federation = InternetArchiveFederation()
        items = federation.discover_ia_items()

        print(f"\n📋 Found {len(items)} items:")
        for item in items[:10]:  # Show first 10
            print(f"  🗂️ {item['identifier']} ({len(item['files'])} Parquet files)")

    @app.command()
    def federate():
        """Create federated views with Internet Archive data."""
        federation = InternetArchiveFederation()
        federation.update_federation()

    @app.command()
    def status():
        """Show federation status and data availability."""
        federation = InternetArchiveFederation()
        availability = federation.get_data_availability()

        print(json.dumps(availability, indent=2, default=str))

    app()


if __name__ == "__main__":
    main()
