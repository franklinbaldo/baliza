"""
PNCP Enum Utilities - Centralized enum management for BALIZA
"""

from enum import Enum
from typing import Type, Dict, List, Union, Optional


class InvalidEnumValueError(ValueError):
    """Raised when an invalid value is provided for an enum."""
    pass


class InstrumentoConvocatorio(int, Enum):
    """
    Representa os tipos de instrumento convocatório.
    Baseado na Tabela de Domínio: Instrumento Convocatório (seção 5.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    EDITAL = 1
    AVISO_DE_CONTRATACAO_DIRETA = 2
    ATO_QUE_AUTORIZA_CONTRATACAO_DIRETA = 3


class ModalidadeContratacao(int, Enum):
    """
    Representa as modalidades de contratação.
    Baseado na Tabela de Domínio: Modalidade de Contratação (seção 5.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

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


class ModoDisputa(int, Enum):
    """
    Representa os modos de disputa.
    Baseado na Tabela de Domínio: Modo de Disputa (seção 5.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    ABERTO = 1
    FECHADO = 2
    ABERTO_FECHADO = 3
    DISPENSA_COM_DISPUTA = 4
    NAO_SE_APLICA = 5
    FECHADO_ABERTO = 6


class CriterioJulgamento(int, Enum):
    """
    Representa os critérios de julgamento.
    Baseado na Tabela de Domínio: Critério de Julgamento (seção 5.4 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    MENOR_PRECO = 1
    MAIOR_DESCONTO = 2
    TECNICA_E_PRECO = 4
    MAIOR_LANCE = 5
    MAIOR_RETORNO_ECONOMICO = 6
    NAO_SE_APLICA = 7
    MELHOR_TECNICA = 8
    CONTEUDO_ARTISTICO = 9


class SituacaoContratacao(int, Enum):
    """
    Representa a situação da contratação.
    Baseado na Tabela de Domínio: Situação da Contratação (seção 5.5 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    DIVULGADA_NO_PNCP = 1
    REVOGADA = 2
    ANULADA = 3
    SUSPENSA = 4


class SituacaoItemContratacao(int, Enum):
    """
    Representa a situação do item da contratação.
    Baseado na Tabela de Domínio: Situação do Item da Contratação (seção 5.6 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    EM_ANDAMENTO = 1
    HOMOLOGADO = 2
    ANULADO_REVOGADO_CANCELADO = 3
    DESERTO = 4
    FRACASSADO = 5


class TipoBeneficio(int, Enum):
    """
    Representa os tipos de benefício.
    Baseado na Tabela de Domínio: Tipo de Benefício (seção 5.7 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    PARTICIPACAO_EXCLUSIVA_ME_EPP = 1
    SUBCONTRATACAO_PARA_ME_EPP = 2
    COTA_RESERVADA_PARA_ME_EPP = 3
    SEM_BENEFICIO = 4
    NAO_SE_APLICA = 5


class SituacaoResultadoItemContratacao(int, Enum):
    """
    Representa a situação do resultado do item da contratação.
    Baseado na Tabela de Domínio: Situação do Resultado do Item da Contratação (seção 5.8 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    INFORMADO = 1
    CANCELADO = 2


class TipoContrato(int, Enum):
    """
    Representa os tipos de contrato.
    Baseado na Tabela de Domínio: Tipo de Contrato (seção 5.9 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    CONTRATO_TERMO_INICIAL = 1
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


class TipoTermoContrato(int, Enum):
    """
    Representa os tipos de termo de contrato.
    Baseado na Tabela de Domínio: Tipo de Termo de Contrato (seção 5.10 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    TERMO_DE_RESCISAO = 1
    TERMO_ADITIVO = 2
    TERMO_DE_APOSTILAMENTO = 3


class CategoriaProcesso(int, Enum):
    """
    Representa as categorias de processo.
    Baseado na Tabela de Domínio: Categoria do Processo (seção 5.11 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

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


class TipoDocumento(int, Enum):
    """
    Representa os tipos de documento.
    Baseado na Tabela de Domínio: Tipo de Documento (seção 5.12 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    AVISO_DE_CONTRATACAO_DIRETA = 1
    EDITAL = 2
    MINUTA_DO_CONTRATO = 3
    TERMO_DE_REFERENCIA = 4
    ANTEPROJETO = 5
    PROJETO_BASICO = 6
    ESTUDO_TECNICO_PRELIMINAR = 7
    PROJETO_EXECUTIVO = 8
    MAPA_DE_RISCOS = 9
    DFD = 10
    ATA_DE_REGISTRO_DE_PRECO = 11
    CONTRATO = 12
    TERMO_DE_RESCISAO = 13
    TERMO_ADITIVO = 14
    TERMO_DE_APOSTILAMENTO = 15
    OUTROS_DOCUMENTOS_PROCESSO = 16
    NOTA_DE_EMPENHO = 17
    RELATORIO_FINAL_DE_CONTRATO = 18


class NaturezaJuridica(str, Enum):
    """
    Representa as naturezas jurídicas.
    Baseado na Tabela de Domínio: Natureza Jurídica (seção 5.13 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    NATUREZA_JURIDICA_NAO_INFORMADA = "0000"
    ORGAO_PUBLICO_PODER_EXECUTIVO_FEDERAL = "1015"
    ORGAO_PUBLICO_PODER_EXECUTIVO_ESTADUAL_OU_DISTRITO_FEDERAL = "1023"
    ORGAO_PUBLICO_PODER_EXECUTIVO_MUNICIPAL = "1031"
    ORGAO_PUBLICO_PODER_LEGISLATIVO_FEDERAL = "1040"
    ORGAO_PUBLICO_PODER_LEGISLATIVO_ESTADUAL_OU_DISTRITO_FEDERAL = "1058"
    ORGAO_PUBLICO_PODER_LEGISLATIVO_MUNICIPAL = "1066"
    ORGAO_PUBLICO_PODER_JUDICIARIO_FEDERAL = "1074"
    ORGAO_PUBLICO_PODER_JUDICIARIO_ESTADUAL = "1082"
    AUTARQUIA_FEDERAL = "1104"
    AUTARQUIA_ESTADUAL_OU_DISTRITO_FEDERAL = "1112"
    AUTARQUIA_MUNICIPAL = "1120"
    FUNDACAO_PUBLICA_DE_DIREITO_PUBLICO_FEDERAL = "1139"
    FUNDACAO_PUBLICA_DE_DIREITO_PUBLICO_ESTADUAL_OU_DISTRITO_FEDERAL = "1147"
    FUNDACAO_PUBLICA_DE_DIREITO_PUBLICO_MUNICIPAL = "1155"
    ORGAO_PUBLICO_AUTONOMO_FEDERAL = "1163"
    ORGAO_PUBLICO_AUTONOMO_ESTADUAL_OU_DISTRITO_FEDERAL = "1171"
    ORGAO_PUBLICO_AUTONOMO_MUNICIPAL = "1180"
    COMISSAO_POLINACIONAL = "1198"
    CONSÓRCIO_PUBLICO_DE_DIREITO_PUBLICO = "1210"
    CONSÓRCIO_PUBLICO_DE_DIREITO_PRIVADO = "1228"
    ESTADO_OU_DISTRITO_FEDERAL = "1236"
    MUNICIPIO = "1244"
    FUNDACAO_PUBLICA_DE_DIREITO_PRIVADO_FEDERAL = "1252"
    FUNDACAO_PUBLICA_DE_DIREITO_PRIVADO_ESTADUAL_OU_DISTRITO_FEDERAL = "1260"
    FUNDACAO_PUBLICA_DE_DIREITO_PRIVADO_MUNICIPAL = "1279"
    FUNDO_PUBLICO_DA_ADMINISTRACAO_INDIRETA_FEDERAL = "1287"
    FUNDO_PUBLICO_DA_ADMINISTRACAO_INDIRETA_ESTADUAL_OU_DISTRITO_FEDERAL = "1295"
    FUNDO_PUBLICO_DA_ADMINISTRACAO_INDIRETA_MUNICIPAL = "1309"
    FUNDO_PUBLICO_DA_ADMINISTRACAO_DIRETA_FEDERAL = "1317"
    FUNDO_PUBLICO_DA_ADMINISTRACAO_DIRETA_ESTADUAL_OU_DISTRITO_FEDERAL = "1325"
    FUNDO_PUBLICO_DA_ADMINISTRACAO_DIRETA_MUNICIPAL = "1333"
    UNIAO = "1341"
    EMPRESA_PUBLICA = "2011"
    SOCIEDADE_DE_ECONOMIA_MISTA = "2038"
    SOCIEDADE_ANONIMA_ABERTA = "2046"
    SOCIEDADE_ANONIMA_FECHADA = "2054"


class PorteEmpresa(int, Enum):
    """
    Representa o porte da empresa.
    Baseado na Tabela de Domínio: Porte da Empresa (seção 5.14 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    ME = 1
    EPP = 2
    DEMAIS = 3
    NAO_SE_APLICA = 4
    NAO_INFORMADO = 5


class AmparoLegal(int, Enum):
    """
    Representa os amparos legais.
    Baseado na Tabela de Domínio: Amparo Legal (seção 5.15 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

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


class CategoriaItemPlanoContratacoes(int, Enum):
    """
    Representa a categoria do item do Plano de Contratações.
    Baseado na Tabela de Domínio: Categoria do Item do Plano de Contratações (seção 5.16 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    MATERIAL = 1
    SERVICO = 2
    OBRAS = 3
    SERVICOS_DE_ENGENHARIA = 4
    SOLUCOES_DE_TIC = 5
    LOCACAO_DE_IMOVEIS = 6
    ALIENACAO_CONCESSAO_PERMISSAO = 7
    OBRAS_E_SERVICOS_DE_ENGENHARIA = 8


class PoderId(str, Enum):
    """
    Representa o poder ao qual um órgão/entidade pertence.
    Baseado na seção 5.13 - Natureza Jurídica, campo 'poderId' (do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    LEGISLATIVO = "L"
    EXECUTIVO = "E"
    JUDICIARIO = "J"


class EsferaId(str, Enum):
    """
    Representa a esfera à qual um órgão/entidade pertence.
    Baseado na seção 5.13 - Natureza Jurídica, campo 'esferaId' (do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    FEDERAL = "F"
    ESTADUAL = "E"
    MUNICIPAL = "M"
    DISTRITAL = "D"


class TipoPessoa(str, Enum):
    """
    Representa o tipo de pessoa (física, jurídica ou estrangeira).
    Baseado no manual e Pydantic Field Enum (seção 5.13 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    PESSOA_JURIDICA = "PJ"
    PESSOA_FISICA = "PF"
    PESSOA_ESTRANGEIRA = "PE"


class ClassificacaoCatalogo(int, Enum):
    """
    Representa a classificação se um item é Material ou Serviço.
    Baseado na seção 6.1 e 6.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md.
    """

    MATERIAL = 1
    SERVICO = 2


class SituacaoCompra(str, Enum):
    """
    Representa a situação da compra/contratação (versão string da API).
    Baseado na seção 6.3 e 6.4 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md, que lista códigos de situação.
    Conforme OpenAPI, são strings: "1", "2", "3", "4".
    
    Note: This exists alongside SituacaoContratacao (int enum) because the API
    returns string values while internal processing may use integers.
    """

    DIVULGADA_NO_PNCP = "1"
    REVOGADA = "2"
    ANULADA = "3"
    SUSPENSA = "4"


class IndicadorOrcamentoSigiloso(str, Enum):
    """
    Representa o indicador de orçamento sigiloso.
    Baseado no OpenAPI, para o campo indicadorOrcamentoSigiloso (do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """

    COMPRA_SEM_SIGILO = "COMPRA_SEM_SIGILO"
    COMPRA_PARCIALMENTE_SIGILOSA = "COMPRA_PARCIALMENTE_SIGILOSA"
    COMPRA_TOTALMENTE_SIGILOSA = "COMPRA_TOTALMENTE_SIGILOSA"


# Enum utilities (mantidas para possíveis usos futuros ou compatibilidade)
def get_enum_by_value(enum_class: Type[Enum], value: Union[int, str], strict: bool = False) -> Optional[Enum]:
    """Get enum member by value.
    
    Args:
        enum_class: The enum class to search
        value: The value to find
        strict: If True, raises InvalidEnumValueError for invalid values
        
    Returns:
        Enum member if found, None otherwise (unless strict=True)
        
    Raises:
        InvalidEnumValueError: If strict=True and value is invalid
    """
    try:
        return enum_class(value)
    except (ValueError, TypeError) as e:
        if strict:
            valid_values = [member.value for member in enum_class]
            raise InvalidEnumValueError(
                f"Invalid value {value!r} for {enum_class.__name__}. "
                f"Valid values: {valid_values}"
            ) from e
        return None


def get_enum_name_by_value(
    enum_class: Type[Enum], value: Union[int, str]
) -> Optional[str]:
    """Get enum member name by value, returning None if not found."""
    enum_member = get_enum_by_value(enum_class, value)
    return enum_member.name if enum_member else None


def validate_enum_value(enum_class: type[Enum], value: int | str) -> bool:
    """Validate that a value exists in the enum."""
    return get_enum_by_value(enum_class, value) is not None


def get_enum_values(enum_class: Type[Enum]) -> List[Union[int, str]]:
    """Get all values from an enum class."""
    return [member.value for member in enum_class]


def get_enum_choices(enum_class: Type[Enum]) -> Dict[Union[int, str], str]:
    """Get all enum values with their names as a dictionary."""
    return {member.value: member.name for member in enum_class}


def get_enum_description(enum_class: type[Enum], value: int | str) -> str:
    """Get a human-readable description of an enum value."""
    enum_member = get_enum_by_value(enum_class, value)
    if not enum_member:
        return f"Unknown {enum_class.__name__} value: {value}"

    # Use the docstring of the enum member if it exists
    if enum_member.__doc__:
        return enum_member.__doc__.strip()

    # Fallback to the previous implementation
    name = enum_member.name.replace("_", " ").title()
    return f"{name} ({enum_member.value})"


# Note: ENUM_REGISTRY removed as it was unused. Direct enum imports are preferred for type safety.


class TipoEventoNotaFiscal(str, Enum):
    """
    Representa o tipo de evento em uma nota fiscal eletrônica.
    Baseado na seção 6.5.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (implied from context, not explicitly listed as domain table).
    """

    CONFIRMACAO_OPERACAO = "CONFIRMACAO_OPERACAO"
    CIENCIA_OPERACAO = "CIENCIA_OPERACAO"
    DESCONHECIMENTO_OPERACAO = "DESCONHECIMENTO_OPERACAO"
    OPERACAO_NAO_REALIZADA = "OPERACAO_NAO_REALIZADA"


# Note: Removed unused registry functions. Use direct enum imports and type checking instead.


class PncpEndpoint(str, Enum):
    """
    Representa os endpoints da API PNCP.
    Os valores devem corresponder às chaves em ENDPOINT_CONFIG no config.py e aos caminhos da API
    (seção 6 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md).
    """
    CONTRATACOES_PUBLICACAO = "contratacoes_publicacao"
    CONTRATOS = "contratos"
    ATAS = "atas"
    CONTRATACOES_ATUALIZACAO = "contratacoes_atualizacao"
    CONTRATOS_ATUALIZACAO = "contratos_atualizacao"
    ATAS_ATUALIZACAO = "atas_atualizacao"
    CONTRATACOES_PROPOSTA = "contratacoes_proposta"
    INSTRUMENTOSCOBRANCA_INCLUSAO = "instrumentoscobranca_inclusao"
    PCA = "pca"
    PCA_USUARIO = "pca_usuario"
    PCA_ATUALIZACAO = "pca_atualizacao"
    CONTRATACAO_ESPECIFICA = "contratacao_especifica"
