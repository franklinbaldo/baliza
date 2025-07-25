"""
Complete PNCP endpoint extraction flows for 100% API coverage
"""

import json
import zlib
from datetime import datetime
from typing import List, Dict, Any, Optional
from uuid import uuid4

from prefect import flow, task, get_run_logger

from ..config import settings
from ..enums import ModalidadeContratacao
from ..utils.http_client import EndpointExtractor
from ..utils.endpoints import DateRangeHelper, get_all_pncp_endpoints
from .raw import (
    ExtractionResult,
    extract_contratacoes_modalidade,
    extract_contratos,
    extract_atas,
    extract_contratacoes_atualizacao_modalidade,
    extract_contratos_atualizacao,
    extract_atas_atualizacao,
    log_extraction_execution,
    store_api_request,
)


@flow
async def extract_contratacoes_subflow(data_inicial, data_final, modalidades):
    """Extracts all data related to contratacoes."""
    tasks = []
    for modalidade in modalidades:
        task = extract_contratacoes_modalidade.submit(
            data_inicial=data_inicial,
            data_final=data_final,
            modalidade_code=modalidade.value,
        )
        tasks.append(task)
        task = extract_contratacoes_atualizacao_modalidade.submit(
            data_inicial=data_inicial,
            data_final=data_final,
            modalidade_code=modalidade.value,
        )
        tasks.append(task)
    proposta_task = extract_contratacoes_proposta.submit(data_final=data_final)
    tasks.append(proposta_task)
    return [task.result() for task in tasks]


@flow
async def extract_contratos_subflow(data_inicial, data_final):
    """Extracts all data related to contratos."""
    tasks = [
        extract_contratos.submit(data_inicial=data_inicial, data_final=data_final),
        extract_contratos_atualizacao.submit(
            data_inicial=data_inicial, data_final=data_final
        ),
    ]
    return [task.result() for task in tasks]


@flow
async def extract_atas_subflow(data_inicial, data_final):
    """Extracts all data related to atas."""
    tasks = [
        extract_atas.submit(data_inicial=data_inicial, data_final=data_final),
        extract_atas_atualizacao.submit(
            data_inicial=data_inicial, data_final=data_final
        ),
    ]
    return [task.result() for task in tasks]


@flow
async def extract_pca_subflow(data_inicial, data_final, codigo_classificacao):
    """Extracts all data related to PCA."""
    current_year = datetime.now().year
    tasks = [
        extract_pca.submit(
            ano_pca=current_year,
            codigo_classificacao=codigo_classificacao,
        ),
        extract_pca_atualizacao.submit(data_inicio=data_inicial, data_fim=data_final),
    ]
    return [task.result() for task in tasks]


@flow(name="extract_all_pncp_endpoints", log_prints=True)
async def extract_all_pncp_endpoints(
    date_range_days: int = 7,
    modalidades: Optional[List[ModalidadeContratacao]] = None,
    include_pca: bool = False,
    concurrent: bool = True,
    codigo_classificacao: str = "01",
) -> Dict[str, Any]:
    """
    Extract data from ALL PNCP endpoints for 100% API coverage

    Args:
        date_range_days: Number of days to extract
        modalidades: List of modalidade codes to extract
        include_pca: Whether to include PCA endpoints (requires additional params)
        concurrent: Whether to run extractions concurrently
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
        f"Starting COMPLETE PNCP extraction: {data_inicial} to {data_final}, "
        f"modalidades: {modalidades}, concurrent: {concurrent}, "
        f"include_pca: {include_pca}"
    )

    try:
        results = []
        if concurrent:
            tasks = []
            tasks.append(
                extract_contratacoes_subflow.submit(
                    data_inicial, data_final, modalidades
                )
            )
            tasks.append(extract_contratos_subflow.submit(data_inicial, data_final))
            tasks.append(extract_atas_subflow.submit(data_inicial, data_final))
            tasks.append(
                extract_instrumentos_cobranca.submit(
                    data_inicial=data_inicial, data_final=data_final
                )
            )
            if include_pca:
                tasks.append(
                    extract_pca_subflow.submit(
                        data_inicial, data_final, codigo_classificacao
                    )
                )

            for task in tasks:
                results.extend(task.result())
        else:
            # Sequential extraction (fallback)
            results.extend(
                await extract_contratacoes_subflow(
                    data_inicial, data_final, modalidades
                )
            )
            results.extend(await extract_contratos_subflow(data_inicial, data_final))
            results.extend(await extract_atas_subflow(data_inicial, data_final))
            results.append(
                await extract_instrumentos_cobranca(
                    data_inicial=data_inicial, data_final=data_final
                )
            )
            if include_pca:
                results.extend(
                    await extract_pca_subflow(
                        data_inicial, data_final, codigo_classificacao
                    )
                )

        # Aggregate results
        total_requests = sum(r.total_requests for r in results)
        total_records = sum(r.total_records for r in results)
        total_bytes = sum(r.total_bytes for r in results)
        successful_extractions = sum(1 for r in results if r.success)
        failed_extractions = len(results) - successful_extractions

        # Count unique endpoints
        unique_endpoints = len(set(r.endpoint for r in results))

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Log execution
        log_extraction_execution.submit(
            execution_id=execution_id,
            flow_name="extract_all_pncp_endpoints",
            task_name="complete_extraction",
            start_time=start_time,
            end_time=end_time,
            status="success" if failed_extractions == 0 else "partial_success",
            records_processed=total_records,
            bytes_processed=total_bytes,
            metadata={
                "date_range": f"{data_inicial}:{data_final}",
                "modalidades": modalidades,
                "concurrent": concurrent,
                "include_pca": include_pca,
                "unique_endpoints": unique_endpoints,
                "successful_extractions": successful_extractions,
                "failed_extractions": failed_extractions,
                "results": [r.dict() for r in results],
            },
        )

        summary = {
            "execution_id": execution_id,
            "date_range": f"{data_inicial} to {data_final}",
            "duration_seconds": duration,
            "unique_endpoints_extracted": unique_endpoints,
            "total_endpoints_available": len(get_all_pncp_endpoints()),
            "coverage_percentage": round(
                (unique_endpoints / len(get_all_pncp_endpoints())) * 100, 1
            ),
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
            f"COMPLETE PNCP extraction completed: "
            f"{unique_endpoints}/{len(get_all_pncp_endpoints())} endpoints "
            f"({summary['coverage_percentage']}% coverage), "
            f"{total_records} records ({summary['total_mb']} MB) in {duration:.2f}s "
            f"({summary['throughput_records_per_second']} records/s)"
        )

        return summary

    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.error(f"Complete PNCP extraction failed: {e}")

        # Log failure
        log_extraction_execution.submit(
            execution_id=execution_id,
            flow_name="extract_all_pncp_endpoints",
            task_name="complete_extraction",
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
                "include_pca": include_pca,
            },
        )

        raise


# Individual missing endpoint tasks
@task(name="extract_contratacoes_proposta", timeout_seconds=3600)
async def extract_contratacoes_proposta(
    data_final: str, modalidade: Optional[ModalidadeContratacao] = None, **filters
) -> ExtractionResult:
    """Extract contratações with open proposal period"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(
        f"Starting extraction: contratacoes_proposta "
        f"(up to {data_final}, modalidade {modalidade.name if modalidade else 'all'})"
    )

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_contratacoes_proposta(
            data_final=data_final,
            modalidade=modalidade,
            **filters,
        )

        # Store each request in database
        total_records = 0
        total_bytes = 0

        for api_request in api_requests:
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            if "data" in payload_json:
                total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            store_api_request.submit(api_request)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="contratacoes_proposta",
            modalidade=modalidade,
            date_range=f"up_to_{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: contratacoes_proposta - "
            f"{result.total_requests} requests, {result.total_records} records"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for contratacoes_proposta: {e}")

        return ExtractionResult(
            endpoint="contratacoes_proposta",
            modalidade=modalidade,
            date_range=f"up_to_{data_final}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )


@task(name="extract_instrumentos_cobranca", timeout_seconds=3600)
async def extract_instrumentos_cobranca(
    data_inicial: str, data_final: str, **filters
) -> ExtractionResult:
    """Extract instrumentos de cobrança by inclusion date"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(
        f"Starting extraction: instrumentos_cobranca ({data_inicial} to {data_final})"
    )

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_instrumentos_cobranca(
            data_inicial=data_inicial, data_final=data_final, **filters
        )

        # Store and count data
        total_records = 0
        total_bytes = 0

        for api_request in api_requests:
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            if "data" in payload_json:
                total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            store_api_request.submit(api_request)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="instrumentoscobranca_inclusao",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: instrumentos_cobranca - "
            f"{result.total_requests} requests, {result.total_records} records"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for instrumentos_cobranca: {e}")

        return ExtractionResult(
            endpoint="instrumentoscobranca_inclusao",
            date_range=f"{data_inicial}:{data_final}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )


@task(name="extract_pca", timeout_seconds=3600)
async def extract_pca(
    ano_pca: int, codigo_classificacao: str, **filters
) -> ExtractionResult:
    """Extract PCA (Plano de Contratações Anuais) data"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(f"Starting extraction: pca (year {ano_pca})")

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_pca(
            ano_pca=ano_pca, codigo_classificacao=codigo_classificacao, **filters
        )

        # Store and count data
        total_records = 0
        total_bytes = 0

        for api_request in api_requests:
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            if "data" in payload_json:
                total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            store_api_request.submit(api_request)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="pca",
            date_range=f"year_{ano_pca}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: pca - "
            f"{result.total_requests} requests, {result.total_records} records"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for pca: {e}")

        return ExtractionResult(
            endpoint="pca",
            date_range=f"year_{ano_pca}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )


@task(name="extract_pca_atualizacao", timeout_seconds=3600)
async def extract_pca_atualizacao(
    data_inicio: str, data_fim: str, **filters
) -> ExtractionResult:
    """Extract PCA data by update date"""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(f"Starting extraction: pca_atualizacao ({data_inicio} to {data_fim})")

    try:
        extractor = EndpointExtractor()

        # Extract data from API
        api_requests = await extractor.extract_pca_atualizacao(
            data_inicio=data_inicio, data_fim=data_fim, **filters
        )

        # Store and count data
        total_records = 0
        total_bytes = 0

        for api_request in api_requests:
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            if "data" in payload_json:
                total_records += len(payload_json["data"])

            total_bytes += api_request.payload_size
            store_api_request.submit(api_request)

        await extractor.close()

        duration = (datetime.now() - start_time).total_seconds()

        result = ExtractionResult(
            endpoint="pca_atualizacao",
            date_range=f"{data_inicio}:{data_fim}",
            total_requests=len(api_requests),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_seconds=duration,
            success=True,
        )

        logger.info(
            f"Completed extraction: pca_atualizacao - "
            f"{result.total_requests} requests, {result.total_records} records"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Extraction failed for pca_atualizacao: {e}")

        return ExtractionResult(
            endpoint="pca_atualizacao",
            date_range=f"{data_inicio}:{data_fim}",
            total_requests=0,
            total_records=0,
            total_bytes=0,
            duration_seconds=duration,
            success=False,
            error_message=str(e),
        )
