from enum import Enum
from . import register_enum

@register_enum
class InstrumentoConvocatorio(Enum):
    EDITAL = 1
    AVISO_CONTRATACAO_DIRETA = 2
    ATO_QUE_AUTORIZA_CONTRATACAO_DIRETA = 3

@register_enum
class ModalidadeContratacao(Enum):
    LEILAO_ELETRONICO = 1
    DIALOGO_COMPETITIVO = 2
    CONCURSO = 3
    CONCORRENCIA_ELETRONICA = 4
    CONCORRENCIA_PRESENCIAL = 5
    PREGAO_ELETRONICO = 6
    PREGAO_PRESENCIAL = 7
    DISPENSA_DE_LICITACAO = 8
    INEXIGIBILIDADE = 9
    MANIFESTACAO_DE_INTERESSE = 10
    PRE_QUALIFICACAO = 11
    CREDENCIAMENTO = 12
    LEILAO_PRESENCIAL = 13

@register_enum
class ModoDisputa(Enum):
    ABERTO = 1
    FECHADO = 2
    ABERTO_FECHADO = 3
    DISPENSA_COM_DISPUTA = 4
    NAO_SE_APLICA = 5
    FECHADO_ABERTO = 6

@register_enum
class CriterioJulgamento(Enum):
    MENOR_PRECO = 1
    MAIOR_DESCONTO = 2
    TECNICA_E_PRECO = 4
    MAIOR_LANCE = 5
    MAIOR_RETORNO_ECONOMICO = 6
    NAO_SE_APLICA = 7
    MELHOR_TECNICA = 8
    CONTEUDO_ARTISTICO = 9

@register_enum
class SituacaoContratacao(Enum):
    DIVULGADA_NO_PNCP = 1
    REVOGADA = 2
    ANULADA = 3
    SUSPENSA = 4

@register_enum
class SituacaoItemContratacao(Enum):
    EM_ANDAMENTO = 1
    HOMOLOGADO = 2
    ANULADO_REVOGADO_CANCELADO = 3
    DESERTO = 4
    FRACASSADO = 5

@register_enum
class TipoBeneficio(Enum):
    PARTICIPACAO_EXCLUSIVA_ME_EPP = 1
    SUBCONTRATACAO_PARA_ME_EPP = 2
    COTA_RESERVADA_PARA_ME_EPP = 3
    SEM_BENEFICIO = 4
    NAO_SE_APLICA = 5

@register_enum
class SituacaoResultadoItemContratacao(Enum):
    INFORMADO = 1
    CANCELADO = 2
