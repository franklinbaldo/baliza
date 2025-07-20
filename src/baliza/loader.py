"""
Handles the 'load' command by exporting data to Parquet and uploading to the Internet Archive.

Enhanced with proper error handling, configurable schemas, and robust exception management.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import typer
from internetarchive import get_item, upload
from internetarchive.exceptions import ItemLocateError, AuthenticationError

from .config import settings

logger = logging.getLogger(__name__)

DATA_DIR = Path("data")
PARQUET_DIR = DATA_DIR / "parquet"

# Configuration for table export - addressing hardcoded schemas issue
DEFAULT_EXPORT_SCHEMAS = ["gold", "silver"]  # Can be made configurable
REQUIRED_CREDENTIALS = ["ia_access_key", "ia_secret_key", "internet_archive_identifier"]


class LoaderError(Exception):
    """Base exception for loader operations."""
    pass


class ExportError(LoaderError):
    """Exception raised during data export operations."""
    pass


class UploadError(LoaderError):
    """Exception raised during Internet Archive upload operations."""
    pass


def validate_credentials() -> None:
    """Validate that required Internet Archive credentials are configured."""
    missing_creds = []
    
    for cred in REQUIRED_CREDENTIALS:
        value = getattr(settings, cred, None)
        if not value or (isinstance(value, str) and value.strip() == ""):
            missing_creds.append(cred)
    
    if missing_creds:
        raise LoaderError(
            f"Missing required Internet Archive credentials: {', '.join(missing_creds)}. "
            "Please set these in environment variables or configuration."
        )


def export_to_parquet(schemas: list[str] | None = None) -> dict[str, Any]:
    """
    Exports tables from DuckDB to Parquet files with proper error handling.
    
    Args:
        schemas: List of schemas to export. Defaults to DEFAULT_EXPORT_SCHEMAS.
        
    Returns:
        dict: Export results with statistics
        
    Raises:
        ExportError: If export fails
    """
    if schemas is None:
        schemas = DEFAULT_EXPORT_SCHEMAS
    
    logger.info(f"Starting export to Parquet for schemas: {schemas}")
    
    try:
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        
        db_path = DATA_DIR / "baliza.duckdb"
        if not db_path.exists():
            raise ExportError(f"Database file not found: {db_path}")
        
        results = {
            "exported_tables": [],
            "total_files": 0,
            "total_rows": 0,
            "schemas_processed": schemas
        }
        
        with duckdb.connect(database=str(db_path), read_only=True) as conn:
            for schema in schemas:
                logger.info(f"Exporting tables from schema: {schema}")
                
                # Get tables for this schema with error handling
                try:
                    tables = conn.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                        [schema]
                    ).fetchall()
                except duckdb.Error as e:
                    logger.warning(f"Failed to list tables in schema '{schema}': {e}")
                    continue
                
                if not tables:
                    logger.warning(f"No tables found in schema '{schema}'")
                    continue
                
                for table_row in tables:
                    table_name = table_row[0]
                    full_table_name = f"{schema}.{table_name}"
                    output_path = PARQUET_DIR / f"{schema}_{table_name}.parquet"
                    
                    try:
                        typer.echo(f"Exporting table: {full_table_name}")
                        logger.info(f"Exporting {full_table_name} to {output_path}")
                        
                        # Get table data with row count
                        df = conn.table(full_table_name).df()
                        row_count = len(df)
                        
                        # Export to Parquet
                        df.to_parquet(output_path, index=False)
                        
                        results["exported_tables"].append({
                            "schema": schema,
                            "table": table_name,
                            "file": str(output_path),
                            "rows": row_count
                        })
                        results["total_files"] += 1
                        results["total_rows"] += row_count
                        
                        typer.echo(f"✅ Exported {row_count:,} rows to {output_path.name}")
                        
                    except Exception as e:
                        logger.error(f"Failed to export table {full_table_name}: {e}")
                        typer.echo(f"❌ Failed to export {full_table_name}: {e}")
                        # Continue with other tables rather than failing completely
                        continue
        
        logger.info(f"Export completed: {results['total_files']} files, {results['total_rows']:,} total rows")
        return results
        
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise ExportError(f"Failed to export data to Parquet: {e}") from e


def upload_to_internet_archive(max_retries: int = 3) -> dict[str, Any]:
    """
    Uploads the Parquet files to the Internet Archive with comprehensive error handling.
    
    Args:
        max_retries: Maximum number of upload attempts
        
    Returns:
        dict: Upload results with metadata
        
    Raises:
        UploadError: If upload fails after all retries
    """
    validate_credentials()
    
    identifier = settings.internet_archive_identifier
    logger.info(f"Starting upload to Internet Archive item: {identifier}")
    
    # Check for files to upload
    parquet_files = list(PARQUET_DIR.glob("*.parquet"))
    if not parquet_files:
        raise UploadError("No Parquet files found to upload")
    
    logger.info(f"Found {len(parquet_files)} Parquet files to upload")
    
    for attempt in range(max_retries):
        try:
            typer.echo(f"Uploading to Internet Archive item: {identifier} (attempt {attempt + 1}/{max_retries})")
            
            # Get item metadata safely
            try:
                item = get_item(identifier)
                update_date = item.metadata.get('updatedate', datetime.now().strftime('%Y-%m-%d'))
            except ItemLocateError:
                logger.warning(f"Item {identifier} not found, will be created")
                update_date = datetime.now().strftime('%Y-%m-%d')
            except Exception as e:
                logger.warning(f"Failed to get item metadata: {e}, using current date")
                update_date = datetime.now().strftime('%Y-%m-%d')
            
            # Prepare metadata
            metadata = {
                "title": f"BALIZA - Dados de Contratações Públicas do Brasil - {update_date}",
                "description": "Backup Aberto de Licitações Zelando pelo Acesso (BALIZA) - Dados extraídos do Portal Nacional de Contratações Públicas (PNCP) e transformados em formato analítico.",
                "creator": "BALIZA",
                "subject": ["Brazil", "government procurement", "public contracts", "PNCP"],
                "date": update_date,
                "language": "Portuguese",
                "collection": "opensource_data"
            }
            
            # Perform upload with proper error handling
            upload_result = upload(
                identifier,
                files=[str(f) for f in parquet_files],
                metadata=metadata,
                access_key=settings.ia_access_key,
                secret_key=settings.ia_secret_key,
                retries=2,  # Internet Archive internal retries
                retries_sleep=30  # Wait between retries
            )
            
            # Success!
            results = {
                "identifier": identifier,
                "files_uploaded": len(parquet_files),
                "upload_result": upload_result,
                "metadata": metadata,
                "attempt": attempt + 1
            }
            
            typer.echo(f"✅ Upload completed successfully: {len(parquet_files)} files uploaded")
            logger.info(f"Upload successful on attempt {attempt + 1}: {results}")
            return results
            
        except AuthenticationError as e:
            logger.error(f"Internet Archive authentication failed: {e}")
            raise UploadError(f"Authentication failed: {e}. Check your IA credentials.") from e
            
        except Exception as e:
            logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
            
            if attempt == max_retries - 1:
                # Final attempt failed
                logger.error(f"Upload failed after {max_retries} attempts")
                raise UploadError(f"Upload failed after {max_retries} attempts. Last error: {e}") from e
            else:
                # Wait before retry (exponential backoff)
                import time
                wait_time = 2 ** attempt
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)


def load(schemas: list[str] | None = None, max_retries: int = 3) -> dict[str, Any]:
    """
    Exports data to Parquet and uploads to the Internet Archive with full error handling.
    
    Args:
        schemas: List of schemas to export. Defaults to DEFAULT_EXPORT_SCHEMAS.
        max_retries: Maximum upload retry attempts
        
    Returns:
        dict: Combined results from export and upload operations
        
    Raises:
        LoaderError: If any step fails
    """
    logger.info("Starting load operation: export + upload")
    
    try:
        # Step 1: Export to Parquet
        typer.echo("📦 Exporting data to Parquet files...")
        export_results = export_to_parquet(schemas)
        typer.echo(f"✅ Export completed: {export_results['total_files']} files, {export_results['total_rows']:,} rows")
        
        # Step 2: Upload to Internet Archive
        typer.echo("☁️ Uploading to Internet Archive...")
        upload_results = upload_to_internet_archive(max_retries)
        typer.echo("✅ Upload completed successfully")
        
        # Combine results
        combined_results = {
            "export": export_results,
            "upload": upload_results,
            "success": True
        }
        
        logger.info("Load operation completed successfully")
        return combined_results
        
    except (ExportError, UploadError) as e:
        logger.error(f"Load operation failed: {e}")
        typer.echo(f"❌ Load failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during load operation: {e}")
        raise LoaderError(f"Load operation failed: {e}") from e
