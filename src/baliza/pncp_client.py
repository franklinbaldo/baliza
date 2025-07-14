"""
PNCP API Client Wrapper
Provides a wrapper around the generated OpenAPI client to maintain the current interface
"""

from typing import Any, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .api_pncp_consulta_client.api.contrato_empenho import consultar_contratos_por_data_publicacao
from .api_pncp_consulta_client.client import Client


class PNCPClient:
    """Wrapper class for PNCP API that maintains the current interface while using OpenAPI client."""
    
    def __init__(self, base_url: str = "https://pncp.gov.br/api/consulta", timeout: int = 30):
        """Initialize the PNCP client.
        
        Args:
            base_url: Base URL for the PNCP API
            timeout: Request timeout in seconds
        """
        self.client = Client(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": "BALIZA/1.0 (Backup Aberto de Licitacoes)"}
        )
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(5))
    def fetch_contratos_data(self, data_inicial: str, data_final: str, pagina: int, tamanho_pagina: int) -> dict[str, Any]:
        """Fetch contratos data for a date range.
        
        Args:
            data_inicial: Start date in YYYYMMDD format
            data_final: End date in YYYYMMDD format  
            pagina: Page number (1-based)
            tamanho_pagina: Page size (number of items per page)
            
        Returns:
            Dict with 'data', 'totalRegistros', and 'totalPaginas' keys
            
        Raises:
            httpx.HTTPError: For HTTP errors
            RetryError: If all retries are exhausted
        """
        try:
            # Call the generated OpenAPI client function
            response = consultar_contratos_por_data_publicacao.sync_detailed(
                client=self.client,
                data_inicial=data_inicial,
                data_final=data_final,
                pagina=pagina,
                tamanho_pagina=tamanho_pagina
            )
            
            # Handle different response status codes
            if response.status_code == 200:
                # Success - return the parsed content
                if response.parsed:
                    # The OpenAPI client should have parsed the response
                    return self._format_response(response.parsed)
                else:
                    # Fallback to manual JSON parsing if OpenAPI parsing failed
                    if isinstance(response.content, bytes):
                        return self._parse_raw_response(response.content)
                    elif isinstance(response.content, dict):
                        return response.content
                    else:
                        return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
                    
            elif response.status_code == 204:
                # No Content - return empty structure
                return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
                
            else:
                # Unexpected status code - raise HTTPError
                raise httpx.HTTPError(f"HTTP {response.status_code}: Request failed")
                
        except httpx.HTTPStatusError as e:
            # Re-raise as the expected exception type for retry logic
            raise httpx.HTTPError(f"HTTP {e.response.status_code}: {e.response.reason_phrase}") from e
        except Exception as e:
            # Convert any other exceptions to HTTPError for consistent retry behavior
            raise httpx.HTTPError(f"Request failed: {e}") from e
    
    def _format_response(self, parsed_data: Any) -> dict[str, Any]:
        """Format the OpenAPI client response to match the expected structure.
        
        Args:
            parsed_data: Parsed response from OpenAPI client
            
        Returns:
            Formatted response dict
        """
        # If the parsed data is already in the expected format, return as-is
        if isinstance(parsed_data, dict):
            # Check if it has the expected structure
            if all(key in parsed_data for key in ['data', 'totalRegistros', 'totalPaginas']):
                return parsed_data
                
            # If it's a different structure, try to extract the data
            # This handles cases where the OpenAPI client returns a different object structure
            if hasattr(parsed_data, 'data'):
                return {
                    'data': getattr(parsed_data, 'data', []),
                    'totalRegistros': getattr(parsed_data, 'total_registros', 0),
                    'totalPaginas': getattr(parsed_data, 'total_paginas', 0)
                }
        
        # Fallback - assume it's the data array
        if isinstance(parsed_data, list):
            return {
                'data': parsed_data,
                'totalRegistros': len(parsed_data),
                'totalPaginas': 1
            }
            
        # Last resort - return empty structure
        return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
    
    def _parse_raw_response(self, response_content: bytes) -> dict[str, Any]:
        """Parse raw response content when OpenAPI parsing fails.
        
        Args:
            response_content: Raw response bytes
            
        Returns:
            Parsed response dict
        """
        try:
            import json
            return json.loads(response_content.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
    
    def get_status_message(self, status_code: int, reason: Optional[str] = None) -> str:
        """Get a user-friendly status message for HTTP status codes.
        
        Args:
            status_code: HTTP status code
            reason: Optional reason phrase
            
        Returns:
            Formatted status message with emoji
        """
        if status_code == 200:
            return f"🌐 HTTP {status_code} ✅ Success"
        elif status_code == 204:
            return f"🌐 HTTP {status_code} 📭 No data available"
        else:
            reason_text = f" {reason}" if reason else ""
            return f"🌐 HTTP {status_code} ⚠️{reason_text}"


class PNCPApiError(Exception):
    """Custom exception for PNCP API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[Any] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data