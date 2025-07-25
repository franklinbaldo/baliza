"""
Raw data extraction flows using Prefect
"""

import json
import zlib
from datetime import datetime
from typing import List, Dict, Any, Optional
from uuid import uuid4

import pandas as pd
from prefect import flow, task, get_run_logger
from pydantic import BaseModel

from ..backend import connect
from ..config import settings
from ..enums import ModalidadeContratacao
from ..utils.http_client import EndpointExtractor, APIRequest
from ..utils.endpoints import DateRangeHelper


class ExtractionResult(BaseModel):
    """Result of extraction operation"""

    endpoint: str
    modalidade: Optional[ModalidadeContratacao] = None
    date_range: str
    total_requests: int
    total_records: int
    total_bytes: int
    duration_seconds: float
    success: bool
    error_message: Optional[str] = None


@task(name="store_dataframe", retries=2)
def store_dataframe(df: pd.DataFrame, table_name: str, overwrite: bool = False) -> bool:
    """Store a pandas DataFrame in a DuckDB table."""
    logger = get_run_logger()

    if df.empty:
        return True

    try:
        con = connect()
        con.insert(table_name, df, overwrite=overwrite)
        logger.info(f"Stored {len(df)} rows in table {table_name}.")
        return True
    except Exception as e:
        logger.error(f"Failed to store dataframe in table {table_name}: {e}")
        raise


@task(name="store_api_requests_batch", retries=2)
def store_api_requests_batch(api_requests: List[APIRequest]) -> bool:
    """Store a batch of API request data in DuckDB"""
    if not api_requests:
        return True

    # Store payloads in hot_payloads
    hot_payloads_df = pd.DataFrame(
        [
            {
                "payload_sha256": req.payload_sha256,
                "payload_compressed": req.payload_compressed,
                "first_seen_at": req.collected_at,
            }
            for req in api_requests
        ]
    )
    store_dataframe.submit(hot_payloads_df, "raw.hot_payloads", overwrite=True)

    # Store request metadata in api_requests
    requests_for_df = [req.dict(exclude={"payload_compressed"}) for req in api_requests]
    api_request_df = pd.DataFrame(requests_for_df)
    store_dataframe.submit(api_request_df, "raw.api_requests")

    return True


@task(name="log_extraction_execution", retries=1)
def log_extraction_execution(
    execution_id: str,
    flow_name: str,
    task_name: str,
    start_time: datetime,
    end_time: datetime,
    status: str,
    records_processed: int,
    bytes_processed: int,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Log extraction execution in meta.execution_log"""
    df = pd.DataFrame(
        [
            {
                "execution_id": execution_id,
                "flow_name": flow_name,
                "task_name": task_name,
                "start_time": start_time,
                "end_time": end_time,
                "status": status,
                "records_processed": records_processed,
                "bytes_processed": bytes_processed,
                "error_message": error_message,
                "metadata": json.dumps(metadata) if metadata else None,
            }
        ]
    )
    store_dataframe.submit(df, "meta.execution_log")
    return True


@task(name="extract_contratacoes_modalidade", timeout_seconds=3600)
async def extract_contratacoes_modalidade(
    data_inicial: str, data_final: str, modalidade: ModalidadeContratacao, **filters
) -> ExtractionResult:
    """Extract contratacoes for specific modalidade"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(
        f"Starting extraction: contratacoes_publicacao "
        f"({data_inicial} to {data_final}, {modalidade.name})"
    )

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_contratacoes_publicacao(
            data_inicial=data_inicial,
            data_final=data_final,
            modalidade=modalidade,
            **filters,
        )

        # Store requests in a batch
        total_records = 0
        total_bytes = 0
        batch_requests = []

        for api_request in api_requests:
            # Count records in this page
            if api_request.payload_compressed:
                payload_json = json.loads(
                    zlib.decompress(api_request.payload_compressed).decode("utf-8")
                )
                if "data" in payload_json:
                    total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            batch_requests.append(api_request)

        # Store the batch of requests
        if batch_requests:
            store_api_requests_batch.submit(batch_requests)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="contratacoes_publicacao",
            modalidade=modalidade,
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: {modalidade.name} - "
            f"{result.total_requests} requests, {result.total_records} records, "
            f"{result.total_bytes / 1024 / 1024:.2f} MB in {duration:.2f}s"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for {modalidade.name}: {e}")

        return ExtractionResult(
            endpoint="contratacoes_publicacao",
            modalidade=modalidade,
            date_range=f"{data_inicial}:{data_final}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )


@task(name="extract_contratos", timeout_seconds=3600)
async def extract_contratos(
    data_inicial: str, data_final: str, **filters
) -> ExtractionResult:
    """Extract contratos for date range"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(f"Starting extraction: contratos ({data_inicial} to {data_final})")

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_contratos(
            data_inicial=data_inicial, data_final=data_final, **filters
        )

        # Store and count data
        total_records = 0
        total_bytes = 0
        batch_requests = []

        for api_request in api_requests:
            if api_request.payload_compressed:
                payload_json = json.loads(
                    zlib.decompress(api_request.payload_compressed).decode("utf-8")
                )
                if "data" in payload_json:
                    total_records += len(payload_json["data"])
            if api_request.payload_compressed:
                payload_json = json.loads(
                    zlib.decompress(api_request.payload_compressed).decode("utf-8")
                )
                if "data" in payload_json:
                    total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            batch_requests.append(api_request)

        if batch_requests:
            store_api_requests_batch.submit(batch_requests)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="contratos",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: contratos - "
            f"{result.total_requests} requests, {result.total_records} records"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for contratos: {e}")

        return ExtractionResult(
            endpoint="contratos",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )


@task(name="extract_atas", timeout_seconds=3600)
async def extract_atas(
    data_inicial: str, data_final: str, **filters
) -> ExtractionResult:
    """Extract atas for date range"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(f"Starting extraction: atas ({data_inicial} to {data_final})")

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_atas(
            data_inicial=data_inicial, data_final=data_final, **filters
        )

        # Store and count data
        total_records = 0
        total_bytes = 0
        batch_requests = []

        for api_request in api_requests:
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            if "data" in payload_json:
                total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            batch_requests.append(api_request)

        if batch_requests:
            store_api_requests_batch.submit(batch_requests)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="atas",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: atas - "
            f"{result.total_requests} requests, {result.total_records} records"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for atas: {e}")

        return ExtractionResult(
            endpoint="atas",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )


@flow(name="extract_phase_2a_concurrent", log_prints=True)
async def extract_phase_2a_concurrent(
    date_range_days: int = 7,
    modalidades: Optional[List[ModalidadeContratacao]] = None,
    concurrent: bool = True,
) -> Dict[str, Any]:
    """
    Extract data from Phase 2A endpoints with concurrent processing
    This is the main extraction flow for MVP
    """
    logger = get_run_logger()
    execution_id = str(uuid4())
    start_time = datetime.now()

    # Generate date range
    data_inicial, data_final = DateRangeHelper.get_last_n_days(date_range_days)

    # Use high priority modalidades if not specified
    if modalidades is None:
        modalidades = settings.HIGH_PRIORITY_MODALIDADES

    logger.info(
        f"Starting Phase 2A extraction: {data_inicial} to {data_final}, "
        f"modalidades: {modalidades}, concurrent: {concurrent}"
    )

    try:
        results = []

        if concurrent:
            # Concurrent extraction for optimal performance
            tasks = []

            # Extract contratacoes for each modalidade in parallel
            for modalidade in modalidades:
                task = extract_contratacoes_modalidade.submit(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    modalidade=modalidade,
                )
                tasks.append(task)

            # Extract contratos and atas in parallel
            contratos_task = extract_contratos.submit(
                data_inicial=data_inicial, data_final=data_final
            )
            atas_task = extract_atas.submit(
                data_inicial=data_inicial, data_final=data_final
            )

            tasks.extend([contratos_task, atas_task])

            # Wait for all tasks to complete
            results = [task.result() for task in tasks]

        else:
            # Sequential extraction (fallback)
            for modalidade in modalidades:
                result = await extract_contratacoes_modalidade(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    modalidade_code=modalidade,
                )
                results.append(result)

            contratos_result = await extract_contratos(
                data_inicial=data_inicial, data_final=data_final
            )
            results.append(contratos_result)

            atas_result = await extract_atas(
                data_inicial=data_inicial, data_final=data_final
            )
            results.append(atas_result)

        # Aggregate results
        total_requests = sum(r.total_requests for r in results)
        total_records = sum(r.total_records for r in results)
        total_bytes = sum(r.total_bytes for r in results)
        successful_extractions = sum(1 for r in results if r.success)
        failed_extractions = len(results) - successful_extractions

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Log execution
        log_extraction_execution.submit(
            execution_id=execution_id,
            flow_name="extract_phase_2a_concurrent",
            task_name="main_flow",
            start_time=start_time,
            end_time=end_time,
            status="success" if failed_extractions == 0 else "partial_success",
            records_processed=total_records,
            bytes_processed=total_bytes,
            metadata={
                "date_range": f"{data_inicial}:{data_final}",
                "modalidades": modalidades,
                "concurrent": concurrent,
                "successful_extractions": successful_extractions,
                "failed_extractions": failed_extractions,
                "results": [r.dict() for r in results],
            },
        )

        summary = {
            "execution_id": execution_id,
            "date_range": f"{data_inicial} to {data_final}",
            "duration_seconds": duration,
            "total_requests": total_requests,
            "total_records": total_records,
            "total_bytes": total_bytes,
            "total_mb": round(total_bytes / 1024 / 1024, 2),
            "successful_extractions": successful_extractions,
            "failed_extractions": failed_extractions,
            "throughput_records_per_second": round(total_records / duration, 2)
            if duration > 0
            else 0,
            "results": results,
        }

        logger.info(
            f"Phase 2A extraction completed: "
            f"{total_records} records ({summary['total_mb']} MB) in {duration:.2f}s "
            f"({summary['throughput_records_per_second']} records/s)"
        )

        return summary

    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.error(f"Phase 2A extraction failed: {e}")

        # Log failure
        log_extraction_execution.submit(
            execution_id=execution_id,
            flow_name="extract_phase_2a_concurrent",
            task_name="main_flow",
            start_time=start_time,
            end_time=end_time,
            status="failed",
            records_processed=0,
            bytes_processed=0,
            error_message=str(e),
            metadata={
                "date_range": f"{data_inicial}:{data_final}",
                "modalidades": modalidades,
                "concurrent": concurrent,
            },
        )

        raise


# MISSING ENDPOINT TASKS FOR 100% COVERAGE


@task(name="extract_contratacoes_atualizacao_modalidade", timeout_seconds=3600)
async def extract_contratacoes_atualizacao_modalidade(
    data_inicial: str, data_final: str, modalidade: ModalidadeContratacao, **filters
) -> ExtractionResult:
    """Extract contratacoes by update date for specific modalidade"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(
        f"Starting extraction: contratacoes_atualizacao "
        f"({data_inicial} to {data_final}, {modalidade.name})"
    )

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_contratacoes_atualizacao(
            data_inicial=data_inicial,
            data_final=data_final,
            modalidade=modalidade,
            **filters,
        )

        # Store each request in database
        total_records = 0
        total_bytes = 0
        batch_requests = []

        for api_request in api_requests:
            # Count records in this page
            if api_request.payload_compressed:
                payload_json = json.loads(
                    zlib.decompress(api_request.payload_compressed).decode("utf-8")
                )
                if "data" in payload_json:
                    total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            batch_requests.append(api_request)

        if batch_requests:
            store_api_requests_batch.submit(batch_requests)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="contratacoes_atualizacao",
            modalidade=modalidade,
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: {modalidade.name} - "
            f"{result.total_requests} requests, {result.total_records} records, "
            f"{result.total_bytes / 1024 / 1024:.2f} MB in {duration:.2f}s"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for {modalidade.name}: {e}")

        return ExtractionResult(
            endpoint="contratacoes_atualizacao",
            modalidade=modalidade,
            date_range=f"{data_inicial}:{data_final}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )


@task(name="extract_contratos_atualizacao", timeout_seconds=3600)
async def extract_contratos_atualizacao(
    data_inicial: str, data_final: str, **filters
) -> ExtractionResult:
    """Extract contratos by update date"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(
        f"Starting extraction: contratos_atualizacao ({data_inicial} to {data_final})"
    )

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_contratos_atualizacao(
            data_inicial=data_inicial, data_final=data_final, **filters
        )

        # Store and count data
        total_records = 0
        total_bytes = 0
        batch_requests = []

        for api_request in api_requests:
            if api_request.payload_compressed:
                payload_json = json.loads(
                    zlib.decompress(api_request.payload_compressed).decode("utf-8")
                )
                if "data" in payload_json:
                    total_records += len(payload_json["data"])
            if api_request.payload_compressed:
                payload_json = json.loads(
                    zlib.decompress(api_request.payload_compressed).decode("utf-8")
                )
                if "data" in payload_json:
                    total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            batch_requests.append(api_request)

        if batch_requests:
            store_api_requests_batch.submit(batch_requests)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="contratos_atualizacao",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: contratos_atualizacao - "
            f"{result.total_requests} requests, {result.total_records} records"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for contratos_atualizacao: {e}")

        return ExtractionResult(
            endpoint="contratos_atualizacao",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )


@task(name="extract_atas_atualizacao", timeout_seconds=3600)
async def extract_atas_atualizacao(
    data_inicial: str, data_final: str, **filters
) -> ExtractionResult:
    """Extract atas by update date"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(
        f"Starting extraction: atas_atualizacao ({data_inicial} to {data_final})"
    )

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_atas_atualizacao(
            data_inicial=data_inicial, data_final=data_final, **filters
        )

        # Store and count data
        total_records = 0
        total_bytes = 0
        batch_requests = []

        for api_request in api_requests:
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            if "data" in payload_json:
                total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            batch_requests.append(api_request)

        if batch_requests:
            store_api_requests_batch.submit(batch_requests)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="atas_atualizacao",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: atas_atualizacao - "
            f"{result.total_requests} requests, {result.total_records} records"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for atas_atualizacao: {e}")

        return ExtractionResult(
            endpoint="atas_atualizacao",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )
