#!/usr/bin/env python3
"""
Migration Script: Monolithic to Split Table Architecture

Migrates existing psa.pncp_raw_responses data to the new split table
architecture (psa.pncp_content + psa.pncp_requests) with content deduplication.

Usage:
    uv run python scripts/migrate_to_split_tables.py [--dry-run] [--batch-size 1000]
"""

import argparse
import logging
import time
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)

from baliza.content_utils import analyze_content, estimate_deduplication_savings
from baliza.pncp_writer import connect_utf8

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

console = Console()


class SplitTableMigrator:
    """Migrates monolithic pncp_raw_responses to split table architecture."""

    def __init__(self, db_path: str, dry_run: bool = False, batch_size: int = 1000):
        self.db_path = db_path
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.conn = None

        # Migration statistics
        self.stats = {
            "total_legacy_records": 0,
            "unique_content_pieces": 0,
            "total_content_size": 0,
            "deduplicated_size": 0,
            "migration_time": 0,
            "records_migrated": 0,
            "content_records_created": 0,
            "request_records_created": 0,
            "deduplication_rate": 0.0,
            "storage_savings_bytes": 0,
            "storage_savings_percent": 0.0,
        }

    def connect_database(self):
        """Connect to database and verify tables exist."""
        try:
            self.conn = connect_utf8(self.db_path)

            # Check if legacy table exists and has data
            legacy_count = self.conn.execute(
                "SELECT COUNT(*) FROM psa.pncp_raw_responses"
            ).fetchone()[0]

            self.stats["total_legacy_records"] = legacy_count

            if legacy_count == 0:
                console.print(
                    "⚠️ [yellow]No records in psa.pncp_raw_responses to migrate[/yellow]"
                )
                return False

            console.print(f"📊 Found {legacy_count:,} records to migrate")
            return True

        except Exception as e:
            console.print(f"❌ [red]Database connection failed: {e}[/red]")
            return False

    def analyze_existing_data(self):
        """Analyze existing data to estimate deduplication potential."""
        console.print(
            "🔍 [bold blue]Analyzing existing data for deduplication potential[/bold blue]"
        )

        # Sample analysis for large datasets
        sample_size = min(10000, self.stats["total_legacy_records"])

        # Get content size distribution
        size_stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT response_content) as unique_content,
                SUM(LENGTH(response_content)) as total_content_size,
                AVG(LENGTH(response_content)) as avg_content_size,
                COUNT(CASE WHEN response_content = '' OR response_content IS NULL THEN 1 END) as empty_responses
            FROM (
                SELECT response_content
                FROM psa.pncp_raw_responses
                LIMIT {sample_size}
            )
        """).fetchone()

        total, unique, total_size, avg_size, empty = size_stats

        estimated_dedup_rate = ((total - unique) / total) * 100 if total > 0 else 0
        estimated_savings = estimate_deduplication_savings(
            unique * avg_size, total_size
        )

        console.print(
            f"📈 [bold]Sample Analysis Results[/bold] (from {sample_size:,} records):"
        )
        console.print(f"  Total responses: {total:,}")
        console.print(f"  Unique content: {unique:,}")
        console.print(f"  Empty responses: {empty:,}")
        console.print(f"  Estimated deduplication rate: {estimated_dedup_rate:.1f}%")
        console.print(
            f"  Estimated storage savings: {estimated_savings['savings_percentage']:.1f}%"
        )
        console.print(f"  Average content size: {avg_size:.0f} bytes")

        return estimated_savings

    def create_migration_tables(self):
        """Create temporary migration tables if they don't exist."""
        if self.dry_run:
            console.print("🔧 [dim]DRY RUN: Would create migration tables[/dim]")
            return

        # The split tables should already exist from the new schema
        # Just verify they exist
        tables_exist = self.conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'psa'
            AND table_name IN ('pncp_content', 'pncp_requests')
        """).fetchone()[0]

        if tables_exist != 2:
            console.print(
                "❌ [red]Split tables don't exist. Run extraction first to create schema.[/red]"
            )
            return False

        console.print("✅ Split tables verified")
        return True

    def migrate_data_batch(self, offset: int, batch_size: int) -> dict:
        """Migrate a batch of records from legacy to split tables."""
        batch_stats = {
            "processed": 0,
            "content_created": 0,
            "requests_created": 0,
            "duplicates_found": 0,
        }

        # Get batch of legacy records
        legacy_records = self.conn.execute(f"""
            SELECT
                id, extracted_at, endpoint_url, endpoint_name, request_parameters,
                response_code, response_content, response_headers, data_date, run_id,
                total_records, total_pages, current_page, page_size
            FROM psa.pncp_raw_responses
            ORDER BY extracted_at
            LIMIT {batch_size} OFFSET {offset}
        """).fetchall()

        if not legacy_records:
            return batch_stats

        # Track content we've seen in this batch
        content_cache: dict[str, str] = {}  # hash -> content_id

        for record in legacy_records:
            (
                legacy_id,
                extracted_at,
                endpoint_url,
                endpoint_name,
                request_parameters,
                response_code,
                response_content,
                response_headers,
                data_date,
                run_id,
                total_records,
                total_pages,
                current_page,
                page_size,
            ) = record

            # Analyze content
            content = response_content or ""
            content_id, content_hash, content_size = analyze_content(content)

            # Check if content already exists (in DB or batch cache)
            if content_hash in content_cache:
                # Already processed in this batch
                existing_content_id = content_cache[content_hash]
                batch_stats["duplicates_found"] += 1
            else:
                # Check database
                existing = self.conn.execute(
                    "SELECT id FROM psa.pncp_content WHERE content_sha256 = ?",
                    [content_hash],
                ).fetchone()

                if existing:
                    existing_content_id = existing[0]
                    # Update reference count
                    if not self.dry_run:
                        self.conn.execute(
                            """
                            UPDATE psa.pncp_content
                            SET reference_count = reference_count + 1,
                                last_seen_at = CURRENT_TIMESTAMP
                            WHERE content_sha256 = ?
                            """,
                            [content_hash],
                        )
                    batch_stats["duplicates_found"] += 1
                else:
                    # New content - create record
                    if not self.dry_run:
                        self.conn.execute(
                            """
                            INSERT INTO psa.pncp_content
                            (id, response_content, content_sha256, content_size_bytes, reference_count)
                            VALUES (?, ?, ?, ?, 1)
                            """,
                            [content_id, content, content_hash, content_size],
                        )
                    existing_content_id = content_id
                    batch_stats["content_created"] += 1

                content_cache[content_hash] = existing_content_id

            # Create request record
            if not self.dry_run:
                self.conn.execute(
                    """
                    INSERT INTO psa.pncp_requests (
                        extracted_at, endpoint_url, endpoint_name, request_parameters,
                        response_code, response_headers, data_date, run_id,
                        total_records, total_pages, current_page, page_size, content_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        extracted_at,
                        endpoint_url,
                        endpoint_name,
                        request_parameters,
                        response_code,
                        response_headers,
                        data_date,
                        run_id,
                        total_records,
                        total_pages,
                        current_page,
                        page_size,
                        existing_content_id,
                    ],
                )

            batch_stats["requests_created"] += 1
            batch_stats["processed"] += 1

        return batch_stats

    def run_migration(self):
        """Execute the complete migration process."""
        start_time = time.time()

        console.print(
            "🚀 [bold blue]Starting Migration to Split Table Architecture[/bold blue]"
        )
        console.print(f"Database: {self.db_path}")
        console.print(f"Batch size: {self.batch_size:,}")
        console.print(f"Dry run: {self.dry_run}")
        console.print()

        # Connect and validate
        if not self.connect_database():
            return False

        # Analyze existing data
        self.analyze_existing_data()
        console.print()

        # Create/verify tables
        if not self.create_migration_tables():
            return False
        console.print()

        # Migration loop
        total_records = self.stats["total_legacy_records"]

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Migrating records", total=total_records)

            offset = 0
            total_content_created = 0
            total_requests_created = 0
            total_duplicates_found = 0

            while offset < total_records:
                # Process batch
                batch_stats = self.migrate_data_batch(offset, self.batch_size)

                if batch_stats["processed"] == 0:
                    break

                # Update statistics
                total_content_created += batch_stats["content_created"]
                total_requests_created += batch_stats["requests_created"]
                total_duplicates_found += batch_stats["duplicates_found"]

                # Commit batch (if not dry run)
                if not self.dry_run:
                    self.conn.commit()

                # Update progress
                progress.advance(task, batch_stats["processed"])
                offset += batch_stats["processed"]

                # Brief pause to avoid overwhelming database
                time.sleep(0.01)

        migration_time = time.time() - start_time

        # Final statistics
        self.stats.update(
            {
                "migration_time": migration_time,
                "records_migrated": total_requests_created,
                "content_records_created": total_content_created,
                "request_records_created": total_requests_created,
                "duplicates_found": total_duplicates_found,
            }
        )

        if total_requests_created > 0:
            self.stats["deduplication_rate"] = (
                total_duplicates_found / total_requests_created
            ) * 100

        self.print_migration_summary()
        return True

    def print_migration_summary(self):
        """Print comprehensive migration summary."""
        console.print()
        console.print("📊 [bold green]Migration Complete![/bold green]")
        console.print("=" * 50)

        stats = self.stats

        console.print(f"⏱️  **Migration Time**: {stats['migration_time']:.1f} seconds")
        console.print(f"📄 **Records Processed**: {stats['records_migrated']:,}")
        console.print(
            f"💾 **Content Records Created**: {stats['content_records_created']:,}"
        )
        console.print(
            f"📨 **Request Records Created**: {stats['request_records_created']:,}"
        )
        console.print(f"🔄 **Duplicates Found**: {stats['duplicates_found']:,}")
        console.print(f"📈 **Deduplication Rate**: {stats['deduplication_rate']:.1f}%")

        if not self.dry_run:
            # Calculate actual storage metrics
            try:
                final_stats = self.conn.execute("""
                    SELECT
                        COUNT(*) as content_count,
                        SUM(content_size_bytes) as total_content_size,
                        SUM(content_size_bytes * reference_count) as theoretical_size
                    FROM psa.pncp_content
                """).fetchone()

                content_count, actual_size, theoretical_size = final_stats

                if theoretical_size and actual_size:
                    savings = theoretical_size - actual_size
                    savings_pct = (savings / theoretical_size) * 100

                    console.print()
                    console.print("💾 [bold]Storage Optimization Results[/bold]:")
                    console.print(f"  Unique content records: {content_count:,}")
                    console.print(f"  Actual storage: {actual_size:,} bytes")
                    console.print(
                        f"  Without deduplication: {theoretical_size:,} bytes"
                    )
                    console.print(
                        f"  Storage savings: {savings:,} bytes ({savings_pct:.1f}%)"
                    )
                    console.print(
                        f"  Compression ratio: {actual_size / theoretical_size:.3f}"
                    )

            except Exception as e:
                console.print(f"⚠️ Could not calculate final storage metrics: {e}")

        if self.dry_run:
            console.print()
            console.print(
                "ℹ️ [dim]This was a dry run - no data was actually migrated[/dim]"
            )
            console.print("Run without --dry-run to perform actual migration")


def main():
    """Main migration script entry point."""
    parser = argparse.ArgumentParser(
        description="Migrate BALIZA database to split table architecture"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze migration without making changes",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of records to process per batch",
    )
    parser.add_argument(
        "--db-path", default="data/baliza.duckdb", help="Path to DuckDB database file"
    )

    args = parser.parse_args()

    # Verify database exists
    db_path = Path(args.db_path)
    if not db_path.exists():
        console.print(f"❌ [red]Database not found: {db_path}[/red]")
        return 1

    # Run migration
    migrator = SplitTableMigrator(
        db_path=str(db_path), dry_run=args.dry_run, batch_size=args.batch_size
    )

    success = migrator.run_migration()
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
