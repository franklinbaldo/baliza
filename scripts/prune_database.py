#!/usr/bin/env python3
"""
Database Pruning Script for BALIZA

Cleans up old data, optimizes storage, and maintains database health.
Supports multiple pruning strategies and safety checks.

Usage:
    uv run python scripts/prune_database.py [options]
"""

import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from baliza.pncp_writer import connect_utf8

console = Console()


class DatabasePruner:
    """Comprehensive database pruning and optimization."""

    def __init__(self, db_path: str = "data/baliza.duckdb", dry_run: bool = False):
        self.db_path = Path(db_path)
        self.data_dir = self.db_path.parent
        self.dry_run = dry_run
        self.conn = None

        # Pruning statistics
        self.stats = {
            "initial_db_size": 0,
            "final_db_size": 0,
            "files_removed": 0,
            "files_size_removed": 0,
            "records_pruned": 0,
            "content_deduplicated": 0,
            "orphaned_content_removed": 0,
            "space_saved": 0,
        }

    def analyze_current_state(self) -> dict:
        """Analyze current database and file system state."""
        console.print("🔍 [bold blue]Analyzing Current Database State[/bold blue]")

        analysis = {
            "db_exists": self.db_path.exists(),
            "db_size": 0,
            "json_files": [],
            "backup_files": [],
            "other_files": [],
            "total_file_size": 0,
        }

        if analysis["db_exists"]:
            analysis["db_size"] = self.db_path.stat().st_size
            self.stats["initial_db_size"] = analysis["db_size"]

        # Scan data directory
        for file_path in self.data_dir.glob("*"):
            if file_path.is_file():
                file_size = file_path.stat().st_size
                analysis["total_file_size"] += file_size

                if file_path.name.startswith("async_extraction_results_"):
                    analysis["json_files"].append((file_path, file_size))
                elif "backup" in file_path.name.lower() or file_path.name.endswith(
                    ".bak"
                ):
                    analysis["backup_files"].append((file_path, file_size))
                elif file_path.name.endswith((".lock", ".wal", ".tmp")):
                    analysis["other_files"].append((file_path, file_size))

        # Display analysis
        table = Table(title="Current Database State")
        table.add_column("Item", style="cyan")
        table.add_column("Count/Size", style="yellow")
        table.add_column("Details", style="white")

        table.add_row(
            "Main Database",
            f"{analysis['db_size'] / 1024 / 1024:.1f} MB",
            str(self.db_path),
        )
        table.add_row(
            "JSON Result Files",
            f"{len(analysis['json_files'])}",
            f"{sum(size for _, size in analysis['json_files']) / 1024 / 1024:.1f} MB",
        )
        table.add_row(
            "Backup Files",
            f"{len(analysis['backup_files'])}",
            f"{sum(size for _, size in analysis['backup_files']) / 1024 / 1024:.1f} MB",
        )
        table.add_row(
            "Temp/Lock Files",
            f"{len(analysis['other_files'])}",
            f"{sum(size for _, size in analysis['other_files']) / 1024:.1f} KB",
        )
        table.add_row(
            "Total Directory Size",
            f"{analysis['total_file_size'] / 1024 / 1024:.1f} MB",
            "All files",
        )

        console.print(table)
        return analysis

    def analyze_database_content(self) -> dict:
        """Analyze database content for pruning opportunities."""
        if not self.db_path.exists():
            return {}

        console.print("🔍 [bold blue]Analyzing Database Content[/bold blue]")

        try:
            self.conn = connect_utf8(str(self.db_path))

            # Get table information
            tables = self.conn.execute("""
                SELECT table_schema, table_name, 
                       (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name AND table_schema = t.table_schema) as column_count
                FROM information_schema.tables t 
                WHERE table_schema = 'psa'
                ORDER BY table_name
            """).fetchall()

            content_analysis = {}

            for schema, table_name, col_count in tables:
                try:
                    count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {schema}.{table_name}"
                    ).fetchone()[0]
                    content_analysis[table_name] = {
                        "record_count": count,
                        "column_count": col_count,
                    }

                    # Special analysis for split tables
                    if table_name == "pncp_content":
                        # Analyze content deduplication
                        stats = self.conn.execute("""
                            SELECT 
                                COUNT(*) as total_content,
                                SUM(reference_count) as total_references,
                                COUNT(CASE WHEN reference_count = 1 THEN 1 END) as unique_content,
                                COUNT(CASE WHEN reference_count > 1 THEN 1 END) as deduplicated_content,
                                SUM(content_size_bytes) as total_size,
                                MAX(last_seen_at) as latest_access,
                                MIN(first_seen_at) as earliest_access
                            FROM psa.pncp_content
                        """).fetchone()

                        content_analysis[table_name].update(
                            {
                                "total_content": stats[0],
                                "total_references": stats[1],
                                "unique_content": stats[2],
                                "deduplicated_content": stats[3],
                                "total_size_bytes": stats[4],
                                "latest_access": stats[5],
                                "earliest_access": stats[6],
                            }
                        )

                        # Find potentially orphaned content (no references)
                        orphaned = self.conn.execute("""
                            SELECT COUNT(*) 
                            FROM psa.pncp_content c
                            LEFT JOIN psa.pncp_requests r ON c.id = r.content_id
                            WHERE r.content_id IS NULL
                        """).fetchone()[0]

                        content_analysis[table_name]["orphaned_content"] = orphaned

                    elif table_name == "pncp_requests":
                        # Analyze request age distribution
                        age_stats = self.conn.execute("""
                            SELECT 
                                COUNT(CASE WHEN extracted_at > (CURRENT_TIMESTAMP - INTERVAL '7 days') THEN 1 END) as last_week,
                                COUNT(CASE WHEN extracted_at > (CURRENT_TIMESTAMP - INTERVAL '30 days') THEN 1 END) as last_month,
                                COUNT(CASE WHEN extracted_at > (CURRENT_TIMESTAMP - INTERVAL '90 days') THEN 1 END) as last_quarter,
                                MIN(extracted_at) as oldest_record,
                                MAX(extracted_at) as newest_record
                            FROM psa.pncp_requests
                        """).fetchone()

                        content_analysis[table_name].update(
                            {
                                "last_week": age_stats[0],
                                "last_month": age_stats[1],
                                "last_quarter": age_stats[2],
                                "oldest_record": age_stats[3],
                                "newest_record": age_stats[4],
                            }
                        )

                except Exception as e:
                    console.print(f"⚠️ Could not analyze table {table_name}: {e}")
                    content_analysis[table_name] = {"error": str(e)}

            # Display content analysis
            content_table = Table(title="Database Content Analysis")
            content_table.add_column("Table", style="cyan")
            content_table.add_column("Records", style="yellow")
            content_table.add_column("Details", style="white")

            for table_name, data in content_analysis.items():
                if "error" in data:
                    content_table.add_row(table_name, "ERROR", data["error"])
                else:
                    details = []
                    if "total_references" in data:
                        dedup_rate = (
                            (data["deduplicated_content"] / data["total_content"]) * 100
                            if data["total_content"] > 0
                            else 0
                        )
                        details.append(f"Dedup: {dedup_rate:.1f}%")
                        if data["orphaned_content"] > 0:
                            details.append(f"Orphaned: {data['orphaned_content']}")
                    if "last_week" in data:
                        details.append(f"Recent: {data['last_week']} (7d)")
                        if data["oldest_record"]:
                            details.append(f"Age: {str(data['oldest_record'])[:10]}")

                    content_table.add_row(
                        table_name,
                        f"{data['record_count']:,}",
                        " | ".join(details) if details else "No special analysis",
                    )

            console.print(content_table)
            return content_analysis

        except Exception as e:
            console.print(f"❌ Database analysis failed: {e}")
            return {}
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

    def prune_old_files(self, keep_days: int = 7) -> None:
        """Remove old JSON result files and temporary files."""
        console.print(
            f"🧹 [bold yellow]Pruning Old Files (older than {keep_days} days)[/bold yellow]"
        )

        cutoff_time = time.time() - (keep_days * 24 * 60 * 60)
        files_to_remove = []

        # Find old JSON result files
        for pattern in ["async_extraction_results_*.json", "extraction_results_*.json"]:
            for file_path in self.data_dir.glob(pattern):
                if file_path.stat().st_mtime < cutoff_time:
                    files_to_remove.append(file_path)

        # Find temp/lock files
        for pattern in ["*.lock", "*.wal", "*.tmp"]:
            for file_path in self.data_dir.glob(pattern):
                # Always remove lock files (they shouldn't persist)
                if pattern == "*.lock" or file_path.stat().st_mtime < cutoff_time:
                    files_to_remove.append(file_path)

        # Remove files
        for file_path in files_to_remove:
            try:
                file_size = file_path.stat().st_size

                if self.dry_run:
                    console.print(
                        f"🔧 [dim]DRY RUN: Would remove {file_path.name} ({file_size / 1024:.1f} KB)[/dim]"
                    )
                else:
                    file_path.unlink()
                    console.print(
                        f"🗑️ Removed {file_path.name} ({file_size / 1024:.1f} KB)"
                    )

                self.stats["files_removed"] += 1
                self.stats["files_size_removed"] += file_size

            except Exception as e:
                console.print(f"⚠️ Could not remove {file_path.name}: {e}")

    def prune_old_records(self, keep_days: int = 90) -> None:
        """Remove old records from database tables."""
        if not self.db_path.exists():
            return

        console.print(
            f"🧹 [bold yellow]Pruning Old Records (older than {keep_days} days)[/bold yellow]"
        )

        try:
            self.conn = connect_utf8(str(self.db_path))
            cutoff_date = datetime.now() - timedelta(days=keep_days)

            # Remove old extraction tasks (they're just for tracking)
            old_tasks = self.conn.execute(
                "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE created_at < ?",
                [cutoff_date],
            ).fetchone()[0]

            if old_tasks > 0:
                if self.dry_run:
                    console.print(
                        f"🔧 [dim]DRY RUN: Would remove {old_tasks:,} old extraction tasks[/dim]"
                    )
                else:
                    self.conn.execute(
                        "DELETE FROM psa.pncp_extraction_tasks WHERE created_at < ?",
                        [cutoff_date],
                    )
                    console.print(f"🗑️ Removed {old_tasks:,} old extraction tasks")
                    self.stats["records_pruned"] += old_tasks

            # Remove old requests and their content references (more aggressive pruning)
            if keep_days < 30:  # Only for very aggressive pruning
                old_requests = self.conn.execute(
                    "SELECT COUNT(*) FROM psa.pncp_requests WHERE extracted_at < ?",
                    [cutoff_date],
                ).fetchone()[0]

                if old_requests > 0:
                    if self.dry_run:
                        console.print(
                            f"🔧 [dim]DRY RUN: Would remove {old_requests:,} old request records[/dim]"
                        )
                    else:
                        # This would require careful content reference cleanup
                        console.print(
                            "⚠️ [yellow]Skipping aggressive request pruning - use migration instead[/yellow]"
                        )

            if not self.dry_run:
                self.conn.commit()

        except Exception as e:
            console.print(f"❌ Record pruning failed: {e}")
            if self.conn:
                self.conn.rollback()
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

    def clean_orphaned_content(self) -> None:
        """Remove orphaned content records with no references."""
        if not self.db_path.exists():
            return

        console.print("🧹 [bold yellow]Cleaning Orphaned Content[/bold yellow]")

        try:
            self.conn = connect_utf8(str(self.db_path))

            # Find orphaned content
            orphaned = self.conn.execute("""
                SELECT c.id, c.content_size_bytes
                FROM psa.pncp_content c
                LEFT JOIN psa.pncp_requests r ON c.id = r.content_id
                WHERE r.content_id IS NULL
            """).fetchall()

            if orphaned:
                total_size = sum(size for _, size in orphaned)

                if self.dry_run:
                    console.print(
                        f"🔧 [dim]DRY RUN: Would remove {len(orphaned):,} orphaned content records ({total_size / 1024:.1f} KB)[/dim]"
                    )
                else:
                    for content_id, _ in orphaned:
                        self.conn.execute(
                            "DELETE FROM psa.pncp_content WHERE id = ?", [content_id]
                        )

                    console.print(
                        f"🗑️ Removed {len(orphaned):,} orphaned content records ({total_size / 1024:.1f} KB)"
                    )
                    self.stats["orphaned_content_removed"] = len(orphaned)
                    self.conn.commit()
            else:
                console.print("✅ No orphaned content found")

        except Exception as e:
            console.print(f"❌ Orphaned content cleanup failed: {e}")
            if self.conn:
                self.conn.rollback()
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

    def vacuum_database(self) -> None:
        """Optimize database storage with VACUUM."""
        if not self.db_path.exists():
            return

        console.print("🔧 [bold green]Optimizing Database Storage[/bold green]")

        try:
            if self.dry_run:
                console.print(
                    "🔧 [dim]DRY RUN: Would run VACUUM to optimize storage[/dim]"
                )
                return

            self.conn = connect_utf8(str(self.db_path))

            # Get size before vacuum
            size_before = self.db_path.stat().st_size

            # Run VACUUM to reclaim space
            with Progress(
                SpinnerColumn(), TextColumn("Vacuuming database...")
            ) as progress:
                task = progress.add_task("vacuum")
                self.conn.execute("VACUUM")
                progress.advance(task)

            # Get size after vacuum
            size_after = self.db_path.stat().st_size
            space_saved = size_before - size_after

            console.print("✅ Database optimized:")
            console.print(f"  Before: {size_before / 1024 / 1024:.1f} MB")
            console.print(f"  After: {size_after / 1024 / 1024:.1f} MB")
            console.print(f"  Saved: {space_saved / 1024 / 1024:.1f} MB")

            self.stats["final_db_size"] = size_after
            self.stats["space_saved"] = space_saved

        except Exception as e:
            console.print(f"❌ Database vacuum failed: {e}")
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

    def remove_backup_files(self, confirm: bool = False) -> None:
        """Remove large backup files."""
        backup_files = list(self.data_dir.glob("*backup*.duckdb")) + list(
            self.data_dir.glob("*.bak")
        )

        if not backup_files:
            console.print("✅ No backup files found")
            return

        total_size = sum(f.stat().st_size for f in backup_files)

        console.print("🗑️ [bold red]Backup File Removal[/bold red]")
        console.print(
            f"Found {len(backup_files)} backup files ({total_size / 1024 / 1024:.1f} MB)"
        )

        for backup_file in backup_files:
            size = backup_file.stat().st_size

            if self.dry_run:
                console.print(
                    f"🔧 [dim]DRY RUN: Would remove {backup_file.name} ({size / 1024 / 1024:.1f} MB)[/dim]"
                )
            elif confirm:
                backup_file.unlink()
                console.print(
                    f"🗑️ Removed {backup_file.name} ({size / 1024 / 1024:.1f} MB)"
                )
                self.stats["files_removed"] += 1
                self.stats["files_size_removed"] += size
            else:
                console.print(
                    f"⚠️ Use --remove-backups to actually remove {backup_file.name}"
                )

    def print_summary(self) -> None:
        """Print comprehensive pruning summary."""
        console.print()
        console.print("📊 [bold green]Database Pruning Summary[/bold green]")
        console.print("=" * 50)

        stats = self.stats

        # File operations
        if stats["files_removed"] > 0:
            console.print(
                f"🗑️ **Files Removed**: {stats['files_removed']} ({stats['files_size_removed'] / 1024 / 1024:.1f} MB)"
            )

        # Database operations
        if stats["records_pruned"] > 0:
            console.print(f"📄 **Records Pruned**: {stats['records_pruned']:,}")

        if stats["orphaned_content_removed"] > 0:
            console.print(
                f"🔄 **Orphaned Content Removed**: {stats['orphaned_content_removed']:,}"
            )

        # Storage optimization
        if stats["space_saved"] > 0:
            console.print(
                f"💾 **Database Storage Saved**: {stats['space_saved'] / 1024 / 1024:.1f} MB"
            )

        # Overall impact
        total_saved = stats["files_size_removed"] + stats["space_saved"]
        if total_saved > 0:
            console.print(
                f"🎯 **Total Space Freed**: {total_saved / 1024 / 1024:.1f} MB"
            )

        if stats["initial_db_size"] > 0 and stats["final_db_size"] > 0:
            reduction = (
                (stats["initial_db_size"] - stats["final_db_size"])
                / stats["initial_db_size"]
            ) * 100
            console.print(f"📈 **Database Size Reduction**: {reduction:.1f}%")

        if self.dry_run:
            console.print()
            console.print("ℹ️ [dim]This was a dry run - no changes were made[/dim]")
            console.print("Run without --dry-run to apply changes")

    def run_full_pruning(
        self,
        keep_file_days: int = 7,
        keep_record_days: int = 90,
        remove_backups: bool = False,
    ) -> None:
        """Execute complete database pruning workflow."""

        console.print("🧹 [bold blue]Starting Complete Database Pruning[/bold blue]")
        console.print(
            f"Keep files: {keep_file_days} days, Keep records: {keep_record_days} days"
        )
        console.print(f"Remove backups: {remove_backups}, Dry run: {self.dry_run}")
        console.print()

        # Analysis phase
        file_analysis = self.analyze_current_state()
        content_analysis = self.analyze_database_content()
        console.print()

        # Pruning phase
        self.prune_old_files(keep_file_days)
        self.prune_old_records(keep_record_days)
        self.clean_orphaned_content()

        if remove_backups:
            self.remove_backup_files(confirm=True)
        else:
            self.remove_backup_files(confirm=False)

        # Optimization phase
        self.vacuum_database()

        # Summary
        self.print_summary()


def main():
    """Main pruning script entry point."""
    parser = argparse.ArgumentParser(description="Prune and optimize BALIZA database")
    parser.add_argument(
        "--dry-run", action="store_true", help="Analyze without making changes"
    )
    parser.add_argument(
        "--keep-file-days", type=int, default=7, help="Days to keep JSON result files"
    )
    parser.add_argument(
        "--keep-record-days", type=int, default=90, help="Days to keep old records"
    )
    parser.add_argument(
        "--remove-backups", action="store_true", help="Remove backup files"
    )
    parser.add_argument(
        "--db-path", default="data/baliza.duckdb", help="Path to database file"
    )

    args = parser.parse_args()

    # Create pruner and run
    pruner = DatabasePruner(db_path=args.db_path, dry_run=args.dry_run)

    pruner.run_full_pruning(
        keep_file_days=args.keep_file_days,
        keep_record_days=args.keep_record_days,
        remove_backups=args.remove_backups,
    )


if __name__ == "__main__":
    main()
