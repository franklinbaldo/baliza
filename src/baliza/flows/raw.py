"""
Raw data extraction flows using Prefect
"""
import asyncio
import json
import zlib
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from uuid import uuid4

from prefect import flow, task, get_run_logger
from pydantic import BaseModel

from ..backend import connect, load_sql_file
from ..config import settings
from ..enums import ModalidadeContratacao, get_enum_by_value
from ..utils.http_client import PNCPClient, EndpointExtractor, APIRequest
from ..utils.endpoints import DateRangeHelper, get_phase_2a_endpoints


class ExtractionResult(BaseModel):
    """Result of extraction operation"""
    endpoint: str
    modalidade: Optional[int] = None
    date_range: str
    total_requests: int
    total_records: int
    total_bytes: int
    duration_seconds: float
    success: bool
    error_message: Optional[str] = None


@task(name="store_api_request", retries=2)
def store_api_request(api_request: APIRequest) -> bool:
    """Store API request data in DuckDB"""
    logger = get_run_logger()
    
    try:
        con = connect()
        
        # Use prepared statement for security
        insert_sql = load_sql_file("insert_api_request.sql")
        
        # Convert UUID to string for DuckDB
        params = [
            str(api_request.request_id),
            api_request.ingestion_date,
            api_request.endpoint,
            api_request.http_status,
            api_request.etag,
            api_request.payload_sha256,
            api_request.payload_size,
            api_request.payload_compressed,
            api_request.collected_at
        ]
        
        con.raw_sql(insert_sql, params)
        
        logger.info(f"Stored API request: {api_request.endpoint} ({api_request.payload_size} bytes)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to store API request: {e}")
        raise


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
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """Log extraction execution in meta.execution_log"""
    logger = get_run_logger()
    
    try:
        con = connect()
        
        log_sql = load_sql_file("log_execution.sql")
        
        params = [
            execution_id,
            flow_name,
            task_name,
            start_time,
            end_time,
            status,
            records_processed,
            bytes_processed,
            error_message,
            json.dumps(metadata) if metadata else None
        ]
        
        con.raw_sql(log_sql, params)
        
        logger.info(f"Logged execution: {flow_name}.{task_name} - {status}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to log execution: {e}")
        raise


@task(name="extract_contratacoes_modalidade", timeout_seconds=3600)
async def extract_contratacoes_modalidade(
    data_inicial: str,
    data_final: str,
    modalidade_code: int,
    **filters
) -> ExtractionResult:
    """Extract contratacoes for specific modalidade"""
    logger = get_run_logger()
    start_time = datetime.now()
    
    # Validate modalidade using enum
    modalidade = get_enum_by_value(ModalidadeContratacao, modalidade_code)
    if not modalidade:
        raise ValueError(f"Invalid modalidade code: {modalidade_code}")
    
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
            **filters
        )
        
        # Store each request in database
        total_records = 0
        total_bytes = 0
        
        for api_request in api_requests:
            # Count records in this page
            payload_json = json.loads(zlib.decompress(api_request.payload_compressed).decode('utf-8'))
            if 'data' in payload_json:
                total_records += len(payload_json['data'])
            
            total_bytes += api_request.payload_size
            
            # Store in database (this will be executed sequentially by Prefect)
            store_api_request.submit(api_request)
        
        await extractor.close()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        result = ExtractionResult(
            endpoint="contratacoes_publicacao",
            modalidade=modalidade_code,
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True
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
            modalidade=modalidade_code,
            date_range=f"{data_inicial}:{data_final}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e)
        )


@task(name="extract_contratos", timeout_seconds=3600)
async def extract_contratos(
    data_inicial: str,
    data_final: str,
    **filters
) -> ExtractionResult:
    """Extract contratos for date range"""
    logger = get_run_logger()
    start_time = datetime.now()
    
    logger.info(f"Starting extraction: contratos ({data_inicial} to {data_final})")
    
    try:
        extractor = EndpointExtractor()
        
        # Extract data from API
        api_requests = await extractor.extract_contratos(
            data_inicial=data_inicial,
            data_final=data_final,
            **filters
        )
        
        # Store and count data
        total_records = 0
        total_bytes = 0
        
        for api_request in api_requests:
            payload_json = json.loads(zlib.decompress(api_request.payload_compressed).decode('utf-8'))
            if 'data' in payload_json:
                total_records += len(payload_json['data'])
            
            total_bytes += api_request.payload_size
            store_api_request.submit(api_request)
        
        await extractor.close()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        result = ExtractionResult(
            endpoint="contratos",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True
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
            error_message=str(e)
        )


@task(name="extract_atas", timeout_seconds=3600)
async def extract_atas(
    data_inicial: str,
    data_final: str,
    **filters
) -> ExtractionResult:
    """Extract atas for date range"""
    logger = get_run_logger()
    start_time = datetime.now()
    
    logger.info(f"Starting extraction: atas ({data_inicial} to {data_final})")
    
    try:
        extractor = EndpointExtractor()
        
        # Extract data from API
        api_requests = await extractor.extract_atas(
            data_inicial=data_inicial,
            data_final=data_final,
            **filters
        )
        
        # Store and count data
        total_records = 0
        total_bytes = 0
        
        for api_request in api_requests:
            payload_json = json.loads(zlib.decompress(api_request.payload_compressed).decode('utf-8'))
            if 'data' in payload_json:
                total_records += len(payload_json['data'])
            
            total_bytes += api_request.payload_size
            store_api_request.submit(api_request)
        
        await extractor.close()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        result = ExtractionResult(
            endpoint="atas",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True
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
            error_message=str(e)
        )


@flow(name="extract_phase_2a_concurrent", log_prints=True)
async def extract_phase_2a_concurrent(
    date_range_days: int = 7,
    modalidades: Optional[List[int]] = None,
    concurrent: bool = True
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
                    modalidade_code=modalidade
                )
                tasks.append(task)
            
            # Extract contratos and atas in parallel
            contratos_task = extract_contratos.submit(
                data_inicial=data_inicial,
                data_final=data_final
            )
            atas_task = extract_atas.submit(
                data_inicial=data_inicial,
                data_final=data_final
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
                    modalidade_code=modalidade
                )
                results.append(result)
            
            contratos_result = await extract_contratos(
                data_inicial=data_inicial,
                data_final=data_final
            )
            results.append(contratos_result)
            
            atas_result = await extract_atas(
                data_inicial=data_inicial,
                data_final=data_final
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
                "results": [r.dict() for r in results]
            }
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
            "throughput_records_per_second": round(total_records / duration, 2) if duration > 0 else 0,
            "results": results
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
                "concurrent": concurrent
            }
        )
        
        raise