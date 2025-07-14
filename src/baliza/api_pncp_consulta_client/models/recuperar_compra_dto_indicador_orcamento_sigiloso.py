from enum import Enum


class RecuperarCompraDTOIndicadorOrcamentoSigiloso(str, Enum):
    COMPRA_PARCIALMENTE_SIGILOSA = "COMPRA_PARCIALMENTE_SIGILOSA"
    COMPRA_SEM_SIGILO = "COMPRA_SEM_SIGILO"
    COMPRA_TOTALMENTE_SIGILOSA = "COMPRA_TOTALMENTE_SIGILOSA"

    def __str__(self) -> str:
        return str(self.value)
