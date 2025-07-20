from enum import Enum
from . import register_enum

@register_enum
class TipoContrato(Enum):
    CONTRATO = 1
    COMODATO = 2
    ARRENDAMENTO = 3
    CONCESSAO = 4
    TERMO_DE_ADESAO = 5
    CONVENIO = 6
    EMPENHO = 7
    OUTROS = 8
    TERMO_DE_EXECUCAO_DESCENTRALIZADA = 9
    ACORDO_DE_COOPERACAO_TECNICA = 10
    TERMO_DE_COMPROMISSO = 11
    CARTA_CONTRATO = 12

@register_enum
class TipoTermoContrato(Enum):
    TERMO_DE_RESCISAO = 1
    TERMO_ADITIVO = 2
    TERMO_DE_APOSTILamento = 3
