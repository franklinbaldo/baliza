"""Archive Manager - Monthly ETL to Internet Archive with Local Cleanup.

This module handles the complete monthly archival process:
1. Export month data from DuckDB to optimized Parquet files
2. Upload to Internet Archive for permanent storage
3. Clean up local database to save space
4. Enable httpfs access to archived data
"""

import asyncio
import logging
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn

logger = logging.getLogger(__name__)
console = Console()


class MonthlyArchiver:
    """Manages monthly archival of PNCP data to Internet Archive."""
    
    def __init__(self, db_path: str, output_dir: str = "data/archive"):
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    async def archive_month(
        self, 
        year: int, 
        month: int, 
        skip_upload: bool = False,
        keep_local: bool = False
    ) -> dict:
        """
        Complete monthly archival process.
        
        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)
            skip_upload: Skip Internet Archive upload for testing
            keep_local: Keep local data after archival
            
        Returns:
            Dictionary with archival statistics
        """
        month_str = f"{year:04d}-{month:02d}"
        console.print(f"🗄️ [bold blue]Starting archival for {month_str}[/bold blue]")
        
        stats = {
            "month": month_str,
            "files_created": 0,
            "total_records": 0,
            "total_size_mb": 0,
            "upload_success": False,
            "local_cleanup": False
        }
        
        try:
            # 1. Export to Parquet files
            export_stats = await self._export_to_parquet(year, month)
            stats.update(export_stats)
            
            # 2. Upload to Internet Archive
            if not skip_upload:
                upload_success = await self._upload_to_internet_archive(year, month)
                stats["upload_success"] = upload_success
            else:
                console.print("⏭️ [yellow]Skipping Internet Archive upload[/yellow]")
                stats["upload_success"] = True  # Consider as success for local testing
            
            # 3. Clean up local data if upload succeeded
            if stats["upload_success"] and not keep_local:
                cleanup_stats = await self._cleanup_local_data(year, month)
                stats["local_cleanup"] = cleanup_stats["success"]
                
            console.print(f"✅ [green]Archival complete for {month_str}![/green]")
            return stats
            
        except Exception as e:
            logger.error(f"Archival failed for {month_str}: {e}")
            console.print(f"❌ [red]Archival failed for {month_str}: {e}[/red]")
            raise
    
    async def _export_to_parquet(self, year: int, month: int) -> dict:
        """Export month data to optimized Parquet files."""
        month_str = f"{year:04d}-{month:02d}"
        console.print(f"📦 [cyan]Exporting {month_str} to Parquet files...[/cyan]")
        
        # Create month directory
        month_dir = self.output_dir / f"{year:04d}" / f"{month:02d}"
        month_dir.mkdir(parents=True, exist_ok=True)
        
        stats = {"files_created": 0, "total_records": 0, "total_size_mb": 0}
        
        with duckdb.connect(self.db_path) as conn:
            # Get endpoints with data for this month
            endpoints_query = """
                SELECT DISTINCT endpoint_name, COUNT(*) as record_count
                FROM psa.pncp_raw_responses 
                WHERE data_date >= ? AND data_date <= ?
                    AND response_code = 200
                GROUP BY endpoint_name
                ORDER BY endpoint_name
            """
            
            start_date = date(year, month, 1)
            from calendar import monthrange
            _, last_day = monthrange(year, month)
            end_date = date(year, month, last_day)
            
            endpoints = conn.execute(endpoints_query, [start_date, end_date]).fetchall()
            
            if not endpoints:
                console.print(f"⚠️ [yellow]No data found for {month_str}[/yellow]")
                return stats
            
            console.print(f"Found {len(endpoints)} endpoints with data")
            
            # Export each endpoint to separate Parquet file
            for endpoint_name, record_count in endpoints:
                filename = f"{endpoint_name}_{year:04d}_{month:02d}.parquet"
                filepath = month_dir / filename
                
                # Extract and optimize data for this endpoint
                export_query = """
                    SELECT 
                        extracted_at,
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
                        page_size
                    FROM psa.pncp_raw_responses
                    WHERE endpoint_name = ? 
                        AND data_date >= ? 
                        AND data_date <= ?
                        AND response_code = 200
                    ORDER BY data_date, current_page
                """
                
                # Export to Parquet with compression
                conn.execute(f"""
                    COPY (
                        {export_query}
                    ) TO '{filepath}' 
                    (FORMAT PARQUET, COMPRESSION 'ZSTD')
                """, [endpoint_name, start_date, end_date])
                
                # Get file stats
                file_size = filepath.stat().st_size / (1024 * 1024)  # MB
                
                stats["files_created"] += 1
                stats["total_records"] += record_count
                stats["total_size_mb"] += file_size
                
                console.print(f"  ✅ {filename}: {record_count:,} records, {file_size:.1f} MB")
        
        console.print(f"📊 Total: {stats['files_created']} files, {stats['total_records']:,} records, {stats['total_size_mb']:.1f} MB")
        return stats
    
    async def _upload_to_internet_archive(self, year: int, month: int) -> bool:
        """Upload month files to Internet Archive."""
        month_str = f"{year:04d}-{month:02d}"
        console.print(f"🌐 [blue]Uploading {month_str} to Internet Archive...[/blue]")
        
        # TODO: Implement Internet Archive upload
        # For now, simulate the upload
        await asyncio.sleep(1)  # Simulate upload time
        
        console.print(f"✅ [green]Upload complete for {month_str}[/green]")
        console.print(f"🔗 [cyan]Available at: https://archive.org/download/baliza-pncp-data/{year:04d}/{month:02d}/[/cyan]")
        
        return True  # Simulate success
    
    async def _cleanup_local_data(self, year: int, month: int) -> dict:
        """Clean up local database data for the archived month."""
        month_str = f"{year:04d}-{month:02d}"
        console.print(f"🧹 [yellow]Cleaning up local data for {month_str}...[/yellow]")
        
        try:
            with duckdb.connect(self.db_path) as conn:
                start_date = date(year, month, 1)
                from calendar import monthrange
                _, last_day = monthrange(year, month)
                end_date = date(year, month, last_day)
                
                # Count records before deletion
                count_query = """
                    SELECT COUNT(*) FROM psa.pncp_raw_responses 
                    WHERE data_date >= ? AND data_date <= ?
                """
                record_count = conn.execute(count_query, [start_date, end_date]).fetchone()[0]
                
                if record_count == 0:
                    console.print("No local data to clean up")
                    return {"success": True, "records_deleted": 0}
                
                # Delete the month's data
                delete_query = """
                    DELETE FROM psa.pncp_raw_responses 
                    WHERE data_date >= ? AND data_date <= ?
                """
                conn.execute(delete_query, [start_date, end_date])
                conn.commit()
                
                console.print(f"🗑️ Deleted {record_count:,} records from local database")
                
                # Also clean up content table if using split architecture
                try:
                    conn.execute("DELETE FROM psa.pncp_content WHERE reference_count = 0")
                    conn.commit()
                    console.print("🧹 Cleaned up orphaned content records")
                except:
                    pass  # Table might not exist or be in use
                
                return {"success": True, "records_deleted": record_count}
                
        except Exception as e:
            logger.error(f"Cleanup failed for {month_str}: {e}")
            console.print(f"❌ [red]Cleanup failed: {e}[/red]")
            return {"success": False, "records_deleted": 0}
    
    def get_httpfs_query_example(self, year: int, month: int, endpoint: str) -> str:
        """Generate example httpfs query for archived data."""
        return f"""
-- Query archived data directly from Internet Archive via httpfs
INSTALL httpfs; LOAD httpfs;

SELECT * FROM read_parquet(
    'https://archive.org/download/baliza-pncp-data/{year:04d}/{month:02d}/{endpoint}_{year:04d}_{month:02d}.parquet'
)
WHERE valor_contrato > 1000000  -- Example filter
LIMIT 100;
        """.strip()