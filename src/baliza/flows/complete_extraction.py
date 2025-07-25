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
from ..enums import ModalidadeContratacao, get_enum_by_value
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
    # TODO: This flow is very long and complex. It would be better to
    # break it down into smaller, more manageable sub-flows to improve
    # readability and maintainability.
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
            # Concurrent extraction for optimal performance
            tasks = []

            # 1. CONTRATAÇÕES ENDPOINTS
            # Extract contratacoes_publicacao for each modalidade
            for modalidade in modalidades:
                task = extract_contratacoes_modalidade.submit(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    modalidade_code=modalidade.value,
                )
                tasks.append(task)

            # Extract contratacoes_atualizacao for each modalidade
            for modalidade in modalidades:
                task = extract_contratacoes_atualizacao_modalidade.submit(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    modalidade_code=modalidade.value,
                )
                tasks.append(task)

            # Extract contratacoes_proposta (single endpoint, no modalidade required)
            proposta_task = extract_contratacoes_proposta.submit(data_final=data_final)
            tasks.append(proposta_task)

            # 2. CONTRATOS ENDPOINTS
            contratos_task = extract_contratos.submit(
                data_inicial=data_inicial, data_final=data_final
            )
            tasks.append(contratos_task)

            contratos_atualizacao_task = extract_contratos_atualizacao.submit(
                data_inicial=data_inicial, data_final=data_final
            )
            tasks.append(contratos_atualizacao_task)

            # 3. ATAS ENDPOINTS
            atas_task = extract_atas.submit(
                data_inicial=data_inicial, data_final=data_final
            )
            tasks.append(atas_task)

            atas_atualizacao_task = extract_atas_atualizacao.submit(
                data_inicial=data_inicial, data_final=data_final
            )
            tasks.append(atas_atualizacao_task)

            # 4. INSTRUMENTOS DE COBRANÇA
            instrumentos_task = extract_instrumentos_cobranca.submit(
                data_inicial=data_inicial, data_final=data_final
            )
            tasks.append(instrumentos_task)

            # 5. PCA ENDPOINTS (if requested)
            if include_pca:
                current_year = datetime.now().year

                # Extract PCA for current year (requires classification code)
                pca_task = extract_pca.submit(
                    ano_pca=current_year,
                    codigo_classificacao=codigo_classificacao,
                )
                tasks.append(pca_task)

                # PCA atualizacao
                pca_atualizacao_task = extract_pca_atualizacao.submit(
                    data_inicio=data_inicial, data_fim=data_final
                )
                tasks.append(pca_atualizacao_task)

            # Wait for all tasks to complete
            results = [task.result() for task in tasks]

        else:
            # Sequential extraction (fallback)
            logger.info("Running sequential extraction (fallback mode)")

            # 1. Contratações - publicacao
            for modalidade in modalidades:
                result = await extract_contratacoes_modalidade(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    modalidade_code=modalidade,
                )
                results.append(result)

            # 2. Contratações - atualizacao
            for modalidade in modalidades:
                result = await extract_contratacoes_atualizacao_modalidade(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    modalidade_code=modalidade,
                )
                results.append(result)

            # 3. Contratações - proposta
            result = await extract_contratacoes_proposta(data_final=data_final)
            results.append(result)

            # 4. Contratos
            result = await extract_contratos(
                data_inicial=data_inicial, data_final=data_final
            )
            results.append(result)

            result = await extract_contratos_atualizacao(
                data_inicial=data_inicial, data_final=data_final
            )
            results.append(result)

            # 5. Atas
            result = await extract_atas(
                data_inicial=data_inicial, data_final=data_final
            )
            results.append(result)

            result = await extract_atas_atualizacao(
                data_inicial=data_inicial, data_final=data_final
            )
            results.append(result)

            # 6. Instrumentos de cobrança
            result = await extract_instrumentos_cobranca(
                data_inicial=data_inicial, data_final=data_final
            )
            results.append(result)

            # 7. PCA (if requested)
            if include_pca:
                current_year = datetime.now().year

                result = await extract_pca(
                    ano_pca=current_year, codigo_classificacao="01"
                )
                results.append(result)

                result = await extract_pca_atualizacao(
                    data_inicio=data_inicial, data_fim=data_final
                )
                results.append(result)

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
            modalidade=modalidade_code,
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
            modalidade=modalidade_code,
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
