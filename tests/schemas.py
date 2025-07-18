from pydantic import BaseModel, Field
from typing import List, Optional, Any


class PNCPResponse(BaseModel):
    """A generic Pydantic model for a PNCP API response."""
    total_registros: int = Field(..., alias="totalRegistros")
    total_paginas: int = Field(..., alias="totalPaginas")
    pagina: int
    tamanho_pagina: int = Field(..., alias="tamanhoPagina")
    data: List[Any]

class Contrato(BaseModel):
    """A Pydantic model for a contract object in the PNCP API response."""
    numero_controle_pncp: str = Field(..., alias="numeroControlePNCP")
    ano_contrato: int = Field(..., alias="anoContrato")
    data_assinatura: str = Field(..., alias="dataAssinatura")
    valor_global: float = Field(..., alias="valorGlobal")
    objeto_contrato: str = Field(..., alias="objetoContrato")

class ContratosResponse(PNCPResponse):
    """A Pydantic model for a PNCP API response for the /contratos endpoint."""
    data: List[Contrato]
