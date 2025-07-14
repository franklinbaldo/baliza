from enum import Enum


class RecuperarContratoDTOTipoPessoaSubContratada(str, Enum):
    PE = "PE"
    PF = "PF"
    PJ = "PJ"

    def __str__(self) -> str:
        return str(self.value)
