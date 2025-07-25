import dlt
import json
import zlib # Import zlib for decompression
from baliza.legacy.enums import PncpEndpoint, ModalidadeContratacao
from baliza.legacy.utils.http_client import EndpointExtractor, PNCPResponse # Import PNCPResponse
from baliza.legacy.utils.hash_utils import hash_sha256 # Import hash_sha256
from datetime import date, timedelta

@dlt.source
def pncp_source(
    start_date: str = "20240101",
    end_date: str = "20240101",
    modalidade: int = None, # Optional, for specific endpoints
    extractor_instance: EndpointExtractor = None # New argument for dependency injection
):
    """
    A dlt source for extracting data from the PNCP API.
    This source uses the legacy PncpEndpoint enum to define resources
    and integrates with EndpointExtractor for data fetching.
    """
    extractor = extractor_instance or EndpointExtractor()

    async def _contratacoes_publicacao_resource(
        start_date: str = dlt.sources.incremental("data_inicial", initial_value="20240101"),
        end_date: str = dlt.sources.incremental("data_final", initial_value="20240101"),
        modalidade: int = None
    ):
        modalidade_enum = ModalidadeContratacao(modalidade) if modalidade else None
        api_requests = await extractor.extract_contratacoes_publicacao(
            data_inicial=start_date,
            data_final=end_date,
            modalidade=modalidade_enum
        )
        for api_request in api_requests:
            # Decompress and parse the payload
            payload_json = json.loads(zlib.decompress(api_request.payload_compressed).decode("utf-8"))
            pncp_response = PNCPResponse(**payload_json)
            
            # Iterate over individual records in the response data
            for record in pncp_response.data:
                # Add a unique ID for deduplication
                record["_dlt_id"] = hash_sha256(record)
                yield record

    async def _contratos_resource(
        start_date: str = dlt.sources.incremental("data_inicial", initial_value="20240101"),
        end_date: str = dlt.sources.incremental("data_final", initial_value="20240101")
    ):
        api_requests = await extractor.extract_contratos(
            data_inicial=start_date,
            data_final=end_date
        )
        for api_request in api_requests:
            payload_json = json.loads(zlib.decompress(api_request.payload_compressed).decode("utf-8"))
            pncp_response = PNCPResponse(**payload_json)
            for record in pncp_response.data:
                record["_dlt_id"] = hash_sha256(record)
                yield record

    async def _atas_resource(
        start_date: str = dlt.sources.incremental("data_inicial", initial_value="20240101"),
        end_date: str = dlt.sources.incremental("data_final", initial_value="20240101")
    ):
        api_requests = await extractor.extract_atas(
            data_inicial=start_date,
            data_final=end_date
        )
        for api_request in api_requests:
            payload_json = json.loads(zlib.decompress(api_request.payload_compressed).decode("utf-8"))
            pncp_response = PNCPResponse(**payload_json)
            for record in pncp_response.data:
                record["_dlt_id"] = hash_sha256(record)
                yield record

    return {
        "contratacoes_publicacao": dlt.resource(_contratacoes_publicacao_resource, name="contratacoes_publicacao", primary_key="_dlt_id", write_disposition="merge"),
        "contratos": dlt.resource(_contratos_resource, name="contratos", primary_key="_dlt_id", write_disposition="merge"),
        "atas": dlt.resource(_atas_resource, name="atas", primary_key="_dlt_id", write_disposition="merge"),
    }
