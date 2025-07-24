"""
PNCP Enum Utilities - Centralized enum management for BALIZA
"""

from enum import Enum


class InstrumentoConvocatorio(Enum):
    EDITAL = 1
    AVISO_CONTRATACAO_DIRETA = 2
    ATO_QUE_AUTORIZA_CONTRATACAO_DIRETA = 3


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


class ModoDisputa(Enum):
    ABERTO = 1
    FECHADO = 2
    ABERTO_FECHADO = 3
    DISPENSA_COM_DISPUTA = 4
    NAO_SE_APLICA = 5
    FECHADO_ABERTO = 6


class CriterioJulgamento(Enum):
    MENOR_PRECO = 1
    MAIOR_DESCONTO = 2
    TECNICA_E_PRECO = 4
    MAIOR_LANCE = 5
    MAIOR_RETORNO_ECONOMICO = 6
    NAO_SE_APLICA = 7
    MELHOR_TECNICA = 8
    CONTEUDO_ARTISTICO = 9


class SituacaoContratacao(Enum):
    DIVULGADA_NO_PNCP = 1
    REVOGADA = 2
    ANULADA = 3
    SUSPENSA = 4


class SituacaoItemContratacao(Enum):
    EM_ANDAMENTO = 1
    HOMOLOGADO = 2
    ANULADO_REVOGADO_CANCELADO = 3
    DESERTO = 4
    FRACASSADO = 5


class TipoBeneficio(Enum):
    PARTICIPACAO_EXCLUSIVA_ME_EPP = 1
    SUBCONTRATACAO_PARA_ME_EPP = 2
    COTA_RESERVADA_PARA_ME_EPP = 3
    SEM_BENEFICIO = 4
    NAO_SE_APLICA = 5


class SituacaoResultadoItemContratacao(Enum):
    INFORMADO = 1
    CANCELADO = 2


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


class TipoTermoContrato(Enum):
    TERMO_DE_RESCISAO = 1
    TERMO_ADITIVO = 2
    TERMO_DE_APOSTILamento = 3


class CategoriaProcesso(Enum):
    CESSAO = 1
    COMPRAS = 2
    INFORMATICA_TIC = 3
    INTERNACIONAL = 4
    LOCACAO_IMOVEIS = 5
    MAO_DE_OBRA = 6
    OBRAS = 7
    SERVICOS = 8
    SERVICOS_DE_ENGENHARIA = 9
    SERVICOS_DE_SAUDE = 10
    ALIENACAO_DE_BENS_MOVEIS_IMOVEIS = 11


class TipoDocumento(Enum):
    AVISO_CONTRATACAO_DIRETA = 1
    EDITAL = 2
    MINUTA_CONTRATO = 3
    TERMO_REFERENCIA = 4
    ANTEPROJETO = 5
    PROJETO_BASICO = 6
    ESTUDO_TECNICO_PRELIMINAR = 7
    PROJETO_EXECUTIVO = 8
    MAPA_RISCOS = 9
    DFD = 10
    ATA_REGISTRO_PRECO = 11
    CONTRATO = 12
    TERMO_RESCISAO = 13
    TERMO_ADITIVO = 14
    TERMO_APOSTILAMENTO = 15
    OUTROS = 16
    NOTA_EMPENHO = 17
    RELATORIO_FINAL_CONTRATO = 18


class NaturezaJuridica(Enum):
    NAO_INFORMADA = 0
    ORGAO_PUBLICO_EXECUTIVO_FEDERAL = 1015
    ORGAO_PUBLICO_EXECUTIVO_ESTADUAL_DF = 1023
    ORGAO_PUBLICO_EXECUTIVO_MUNICIPAL = 1031
    ORGAO_PUBLICO_LEGISLATIVO_FEDERAL = 1040
    ORGAO_PUBLICO_LEGISLATIVO_ESTADUAL_DF = 1058
    ORGAO_PUBLICO_LEGISLATIVO_MUNICIPAL = 1066
    ORGAO_PUBLICO_JUDICIARIO_FEDERAL = 1074
    ORGAO_PUBLICO_JUDICIARIO_ESTADUAL = 1082
    AUTARQUIA_FEDERAL = 1104
    AUTARQUIA_ESTADUAL_DF = 1112
    AUTARQUIA_MUNICIPAL = 1120
    FUNDACAO_PUBLICA_DIREITO_PUBLICO_FEDERAL = 1139
    FUNDACAO_PUBLICA_DIREITO_PUBLICO_ESTADUAL_DF = 1147
    FUNDACAO_PUBLICA_DIREITO_PUBLICO_MUNICIPAL = 1155
    ORGAO_PUBLICO_AUTONOMO_FEDERAL = 1163
    ORGAO_PUBLICO_AUTONOMO_ESTADUAL_DF = 1171
    ORGAO_PUBLICO_AUTONOMO_MUNICIPAL = 1180
    COMISSAO_POLINACIONAL = 1198
    CONSORCIO_PUBLICO_DIREITO_PUBLICO = 1210
    CONSORCIO_PUBLICO_DIREITO_PRIVADO = 1228
    ESTADO_DF = 1236
    MUNICIPIO = 1244
    FUNDACAO_PUBLICA_DIREITO_PRIVADO_FEDERAL = 1252
    FUNDACAO_PUBLICA_DIREITO_PRIVADO_ESTADUAL_DF = 1260
    FUNDACAO_PUBLICA_DIREITO_PRIVADO_MUNICIPAL = 1279
    FUNDO_PUBLICO_ADMINISTRACAO_INDIRETA_FEDERAL = 1287
    FUNDO_PUBLICO_ADMINISTRACAO_INDIRETA_ESTADUAL_DF = 1295
    FUNDO_PUBLICO_ADMINISTRACAO_INDIRETA_MUNICIPAL = 1309
    FUNDO_PUBLICO_ADMINISTRACAO_DIRETA_FEDERAL = 1317
    FUNDO_PUBLICO_ADMINISTRACAO_DIRETA_ESTADUAL_DF = 1325
    FUNDO_PUBLICO_ADMINISTRACAO_DIRETA_MUNICIPAL = 1333
    UNIAO = 1341
    EMPRESA_PUBLICA = 2011
    SOCIEDADE_ECONOMIA_MISTA = 2038
    SOCIEDADE_ANONIMA_ABERTA = 2046
    SOCIEDADE_ANONIMA_FECHADA = 2054


class PorteEmpresa(Enum):
    ME = 1
    EPP = 2
    DEMAIS = 3
    NAO_SE_APLICA = 4
    NAO_INFORMADO = 5


class AmparoLegal(Enum):
    LEI_14133_ART_28_I = 1
    LEI_14133_ART_28_II = 2
    LEI_14133_ART_28_III = 3
    LEI_14133_ART_28_IV = 4
    LEI_14133_ART_28_V = 5
    LEI_14133_ART_74_I = 6
    LEI_14133_ART_74_II = 7
    LEI_14133_ART_74_III_A = 8
    LEI_14133_ART_74_III_B = 9
    LEI_14133_ART_74_III_C = 10
    LEI_14133_ART_74_III_D = 11
    LEI_14133_ART_74_III_E = 12
    LEI_14133_ART_74_III_F = 13
    LEI_14133_ART_74_III_G = 14
    LEI_14133_ART_74_III_H = 15
    LEI_14133_ART_74_IV = 16
    LEI_14133_ART_74_V = 17
    LEI_14133_ART_75_I = 18
    LEI_14133_ART_75_II = 19
    LEI_14133_ART_75_III_A = 20
    LEI_14133_ART_75_III_B = 21
    LEI_14133_ART_75_IV_A = 22
    LEI_14133_ART_75_IV_B = 23
    LEI_14133_ART_75_IV_C = 24
    LEI_14133_ART_75_IV_D = 25
    LEI_14133_ART_75_IV_E = 26
    LEI_14133_ART_75_IV_F = 27
    LEI_14133_ART_75_IV_G = 28


class CategoriaItemPlanoContratacoes(Enum):
    MATERIAL = 1
    SERVICO = 2
    OBRAS = 3
    SERVICOS_DE_ENGENHARIA = 4
    SOLUCOES_DE_TIC = 5
    LOCACAO_DE_IMOVEIS = 6
    ALIENACAO_CONCESSAO_PERMISSAO = 7
    OBRAS_E_SERVICOS_DE_ENGENHARIA = 8


class PoderId(str, Enum):
    LEGISLATIVO = "L"
    EXECUTIVO = "E"
    JUDICIARIO = "J"


class EsferaId(str, Enum):
    FEDERAL = "F"
    ESTADUAL = "E"
    MUNICIPAL = "M"
    DISTRITAL = "D"


class TipoPessoa(str, Enum):
    PESSOA_JURIDICA = "PJ"
    PESSOA_FISICA = "PF"
    PESSOA_ESTRANGEIRA = "PE"


class ClassificacaoCatalogo(int, Enum):
    MATERIAL = 1
    SERVICO = 2


# Enum utilities
def get_enum_by_value(enum_class: type[Enum], value: int | str) -> Enum | None:
    """Get enum member by value, returning None if not found."""
    try:
        return enum_class(value)
    except ValueError:
        return None


def get_enum_name_by_value(enum_class: type[Enum], value: int | str) -> str | None:
    """Get enum member name by value, returning None if not found."""
    enum_member = get_enum_by_value(enum_class, value)
    return enum_member.name if enum_member else None


def validate_enum_value(enum_class: type[Enum], value: int | str) -> bool:
    """Validate that a value exists in the enum."""
    return get_enum_by_value(enum_class, value) is not None


def get_enum_values(enum_class: type[Enum]) -> list[int]:
    """Get all values from an enum class."""
    return [member.value for member in enum_class]


def get_enum_choices(enum_class: type[Enum]) -> dict[int, str]:
    """Get all enum values with their names as a dictionary."""
    return {member.value: member.name for member in enum_class}


def get_enum_description(enum_class: type[Enum], value: int | str) -> str:
    """Get a human-readable description of an enum value."""
    enum_member = get_enum_by_value(enum_class, value)
    if not enum_member:
        return f"Unknown {enum_class.__name__} value: {value}"

    # Convert enum name to human-readable format
    name = enum_member.name.replace("_", " ").title()
    return f"{name} ({enum_member.value})"


# Enum registry for dynamic access
ENUM_REGISTRY = {
    "InstrumentoConvocatorio": InstrumentoConvocatorio,
    "ModalidadeContratacao": ModalidadeContratacao,
    "ModoDisputa": ModoDisputa,
    "CriterioJulgamento": CriterioJulgamento,
    "SituacaoContratacao": SituacaoContratacao,
    "SituacaoItemContratacao": SituacaoItemContratacao,
    "TipoBeneficio": TipoBeneficio,
    "SituacaoResultadoItemContratacao": SituacaoResultadoItemContratacao,
    "TipoContrato": TipoContrato,
    "TipoTermoContrato": TipoTermoContrato,
    "CategoriaProcesso": CategoriaProcesso,
    "TipoDocumento": TipoDocumento,
    "NaturezaJuridica": NaturezaJuridica,
    "PorteEmpresa": PorteEmpresa,
    "AmparoLegal": AmparoLegal,
    "CategoriaItemPlanoContratacoes": CategoriaItemPlanoContratacoes,
    "PoderId": PoderId,
    "EsferaId": EsferaId,
    "TipoPessoa": TipoPessoa,
    "ClassificacaoCatalogo": ClassificacaoCatalogo,
}


def get_enum_by_name(enum_name: str) -> type[Enum] | None:
    """Get enum class by name."""
    return ENUM_REGISTRY.get(enum_name)


def get_all_enum_metadata() -> dict[str, dict[str, str | list[dict[str, int | str]]]]:
    """Get metadata for all enums in the registry."""
    metadata = {}

    for enum_name, enum_class in ENUM_REGISTRY.items():
        metadata[enum_name] = {
            "name": enum_name,
            "description": f"Enum for {enum_name.replace('_', ' ').lower()}",
            "values": [
                {
                    "value": member.value,
                    "name": member.name,
                    "description": get_enum_description(enum_class, member.value),
                }
                for member in enum_class
            ],
        }

    return metadata
