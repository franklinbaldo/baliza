from typing import List, Optional, Union

from pydantic import BaseModel, field_validator

from .schemas import (
    IndicadorOrcamentoSigiloso,
    SituacaoCompra,
    TipoEventoNotaFiscal,
    TipoPessoa,
)


class RespostaErroValidacaoDTO(BaseModel):
    """Represents a validation error response."""

    message: Optional[str] = None
    path: Optional[str] = None
    timestamp: Optional[str] = None
    status: Optional[str] = None
    error: Optional[str] = None


class PlanoContratacaoItemDTO(BaseModel):
    """
    Representa um item em um plano de contratação.
    Baseado na seção 6.1 e 6.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 8.x).
    """

    nomeClassificacaoCatalogo: Optional[str] = None
    descricaoItem: Optional[str] = None
    quantidadeEstimada: Optional[float] = None
    pdmCodigo: Optional[str] = None
    dataInclusao: Optional[str] = None
    numeroItem: Optional[int] = None
    dataAtualizacao: Optional[str] = None
    valorTotal: Optional[float] = None
    pdmDescricao: Optional[str] = None
    codigoItem: Optional[str] = None
    unidadeRequisitante: Optional[str] = None
    grupoContratacaoCodigo: Optional[str] = None
    grupoContratacaoNome: Optional[str] = None
    classificacaoSuperiorCodigo: Optional[str] = None
    classificacaoSuperiorNome: Optional[str] = None
    unidadeFornecimento: Optional[str] = None
    valorUnitario: Optional[float] = None
    valorOrcamentoExercicio: Optional[float] = None
    dataDesejada: Optional[str] = None
    categoriaItemPcaNome: Optional[str] = None
    classificacaoCatalogoId: Optional[int] = None


class PlanoContratacaoComItensDoUsuarioDTO(BaseModel):
    """
    Representa um plano de contratação com itens para um usuário.
    Baseado na seção 6.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).
    """

    itens: Optional[List[PlanoContratacaoItemDTO]] = None
    codigoUnidade: Optional[str] = None
    nomeUnidade: Optional[str] = None
    anoPca: Optional[int] = None
    dataPublicacaoPNCP: Optional[str] = None
    dataAtualizacaoGlobalPCA: Optional[str] = None
    idPcaPncp: Optional[str] = None
    orgaoEntidadeCnpj: Optional[str] = None
    orgaoEntidadeRazaoSocial: Optional[str] = None


class PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO(BaseModel):
    """
    Representa uma resposta paginada para planos de contratação.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: Optional[List[PlanoContratacaoComItensDoUsuarioDTO]] = None
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None


class ContratacaoFonteOrcamentariaDTO(BaseModel):
    """
    Representa uma fonte orçamentária para uma contratação.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 17).
    """

    codigo: Optional[int] = None
    nome: Optional[str] = None
    descricao: Optional[str] = None
    dataInclusao: Optional[str] = None


class RecuperarAmparoLegalDTO(BaseModel):
    """
    Representa o amparo legal para uma contratação.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 16).
    """

    descricao: Optional[str] = None
    nome: Optional[str] = None
    codigo: Optional[int] = None


class RecuperarOrgaoEntidadeDTO(BaseModel):
    """
    Representa uma entidade de órgão.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 25).
    """

    cnpj: Optional[str] = None
    poderId: Optional[str] = None
    esferaId: Optional[str] = None
    razaoSocial: Optional[str] = None


class RecuperarUnidadeOrgaoDTO(BaseModel):
    """
    Representa uma unidade de órgão.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 26).
    """

    ufNome: Optional[str] = None
    codigoUnidade: Optional[str] = None
    nomeUnidade: Optional[str] = None
    ufSigla: Optional[str] = None
    municipioNome: Optional[str] = None
    codigoIbge: Optional[str] = None


class RecuperarCompraDTO(BaseModel):
    """
    Representa uma compra.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).

    Exemplo de JSON:
    ```json
    {
      "valorTotalEstimado": 150000.50,
      "valorTotalHomologado": 145000.00,
      "indicadorOrcamentoSigiloso": "COMPRA_SEM_SIGILO",
      "numeroControlePNCP": "12345678901234567890-1-123456/2024",
      "anoCompra": 2024,
      "sequencialCompra": 123456,
      "numeroCompra": "COMPRA-001/2024",
      "processo": "PROCESSO-ABC-2024",
      "orgaoEntidade": {
        "cnpj": "12345678000100",
        "poderId": "E",
        "esferaId": "M",
        "razaoSocial": "PREFEITURA MUNICIPAL DE EXEMPLO"
      },
      "unidadeOrgao": {
        "ufNome": "São Paulo",
        "codigoUnidade": "UNID001",
        "nomeUnidade": "Secretaria de Compras",
        "ufSigla": "SP",
        "municipioNome": "São Paulo",
        "codigoIbge": "3550308"
      },
      "modalidadeId": 6,
      "modalidadeNome": "Pregão Eletrônico",
      "modoDisputaId": 1,
      "modoDisputaNome": "Aberto",
      "tipoInstrumentoConvocatorioCodigo": 1,
      "tipoInstrumentoConvocatorioNome": "Edital",
      "amparoLegal": {
        "descricao": "Lei 14.133/2021, Art. 75, II",
        "nome": "Art. 75, II",
        "codigo": 19
      },
      "objetoCompra": "Aquisição de materiais de escritório",
      "srp": false,
      "fontesOrcamentarias": [
        {
          "codigo": 1,
          "nome": "Recursos Próprios",
          "descricao": "Recursos do Tesouro Municipal",
          "dataInclusao": "2024-01-01T00:00:00Z"
        }
      ],
      "dataPublicacaoPncp": "2024-01-10T09:00:00Z",
      "dataAberturaProposta": "2024-01-15T10:00:00Z",
      "dataEncerramentoProposta": "2024-01-20T17:00:00Z",
      "situacaoCompraId": "1",
      "situacaoCompraNome": "Divulgada no PNCP",
      "existeResultado": true,
      "dataInclusao": "2024-01-05T08:00:00Z",
      "dataAtualizacao": "2024-01-10T09:00:00Z",
      "dataAtualizacaoGlobal": "2024-01-10T09:00:00Z",
      "usuarioNome": "Sistema Interno"
    }
    ```
    """

    valorTotalEstimado: Optional[float] = None
    valorTotalHomologado: Optional[float] = None
    indicadorOrcamentoSigiloso: Optional[IndicadorOrcamentoSigiloso] = None
    orcamentoSigilosoCodigo: Optional[int] = None
    orcamentoSigilosoDescricao: Optional[str] = None
    numeroControlePNCP: Optional[str] = None
    linkSistemaOrigem: Optional[str] = None
    linkProcessoEletronico: Optional[str] = None
    anoCompra: Optional[int] = None
    sequencialCompra: Optional[int] = None
    numeroCompra: Optional[str] = None
    processo: Optional[str] = None
    orgaoEntidade: Optional[RecuperarOrgaoEntidadeDTO] = None
    unidadeOrgao: Optional[RecuperarUnidadeOrgaoDTO] = None
    orgaoSubRogado: Optional[RecuperarOrgaoEntidadeDTO] = None
    unidadeSubRogada: Optional[RecuperarUnidadeOrgaoDTO] = None
    modalidadeId: Optional[int] = None
    modalidadeNome: Optional[str] = None
    justificativaPresencial: Optional[str] = None
    modoDisputaId: Optional[int] = None
    modoDisputaNome: Optional[str] = None
    tipoInstrumentoConvocatorioCodigo: Optional[int] = None
    tipoInstrumentoConvocatorioNome: Optional[str] = None
    amparoLegal: Optional[RecuperarAmparoLegalDTO] = None
    objetoCompra: Optional[str] = None
    informacaoComplementar: Optional[str] = None
    srp: Optional[bool] = None
    fontesOrcamentarias: Optional[List[ContratacaoFonteOrcamentariaDTO]] = None
    dataPublicacaoPncp: Optional[str] = None
    dataAberturaProposta: Optional[str] = None
    dataEncerramentoProposta: Optional[str] = None
    situacaoCompraId: Optional[SituacaoCompra] = None
    situacaoCompraNome: Optional[str] = None
    existeResultado: Optional[bool] = None
    dataInclusao: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    dataAtualizacaoGlobal: Optional[str] = None
    usuarioNome: Optional[str] = None
    
    @field_validator('situacaoCompraId', mode='before')
    @classmethod
    def coerce_situacao_compra_id(cls, v):
        """Handle API returning integers instead of strings as specified in OpenAPI."""
        if isinstance(v, int):
            return str(v)  # Convert int to string to match OpenAPI spec
        return v


class Categoria(BaseModel):
    """
    Representa uma categoria.
    Baseado na seção 6.6 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 8).
    """

    id: Optional[int] = None
    nome: Optional[str] = None


class NotaFiscalEletronicaConsultaDTO(BaseModel):
    """
    Representa uma nota fiscal eletrônica para consulta.
    Baseado na seção 6.5.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (implied from context, not explicitly listed).

    Note: Forward references handle circular dependencies in PNCP's complex API structure.
    These models represent the nested structure of the electronic invoice system.
    """

    instrumentoCobrancaId: Optional[int] = None
    chave: Optional[str] = None
    nfTransparenciaID: Optional[int] = None
    numero: Optional[int] = None
    serie: Optional[int] = None
    dataEmissao: Optional[str] = None
    niEmitente: Optional[str] = None
    nomeEmitente: Optional[str] = None
    nomeMunicipioEmitente: Optional[str] = None
    codigoOrgaoDestinatario: Optional[str] = None
    nomeOrgaoDestinatario: Optional[str] = None
    codigoOrgaoSuperiorDestinatario: Optional[str] = None
    nomeOrgaoSuperiorDestinatario: Optional[str] = None
    valorNotaFiscal: Optional[str] = None
    tipoEventoMaisRecente: Optional[TipoEventoNotaFiscal] = None
    dataTipoEventoMaisRecente: Optional[str] = None
    dataInclusao: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    itens: List["ItemNotaFiscalConsultaDTO"]
    eventos: List["EventoNotaFiscalConsultaDTO"]


class ItemNotaFiscalConsultaDTO(BaseModel):
    """
    Representa um item em uma nota fiscal eletrônica.
    Baseado na seção 6.5.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (implied from context).
    """

    numeroItem: Optional[int] = None
    descricaoProdutoServico: Optional[str] = None
    codigoNCM: Optional[str] = None
    descricaoNCM: Optional[str] = None
    cfop: Optional[str] = None
    quantidade: Optional[float] = None
    unidade: Optional[str] = None
    valorUnitario: Optional[float] = None
    valorTotal: Optional[float] = None


class EventoNotaFiscalConsultaDTO(BaseModel):
    """
    Representa um evento em uma nota fiscal eletrônica.
    Baseado na seção 6.5.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (implied from context).
    """

    dataEvento: Optional[str] = None
    tipoEvento: Optional[TipoEventoNotaFiscal] = None
    evento: Optional[str] = None
    motivoEvento: Optional[str] = None


class TipoInstrumentoCobrancaDTO(BaseModel):
    """
    Representa um tipo de instrumento de cobrança.
    Baseado na seção 5.18 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (implied from context, not explicitly listed as domain table).
    """

    id: Optional[int] = None
    nome: Optional[str] = None
    descricao: Optional[str] = None
    dataInclusao: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    statusAtivo: Optional[bool] = None


class ConsultarInstrumentoCobrancaDTO(BaseModel):
    """
    Representa um instrumento de cobrança para consulta.
    Baseado na seção 6.5 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).
    """

    cnpj: Optional[str] = None
    ano: Optional[int] = None
    sequencialContrato: Optional[int] = None
    sequencialInstrumentoCobranca: Optional[int] = None
    tipoInstrumentoCobranca: Optional[TipoInstrumentoCobrancaDTO] = None
    numeroInstrumentoCobranca: Optional[str] = None
    dataEmissaoDocumento: Optional[str] = None
    observacao: Optional[str] = None
    chaveNFe: Optional[str] = None
    fonteNFe: Optional[int] = None
    dataConsultaNFe: Optional[str] = None
    statusResponseNFe: Optional[str] = None
    jsonResponseNFe: Optional[str] = None
    notaFiscalEletronica: Optional[NotaFiscalEletronicaConsultaDTO] = None
    dataInclusao: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    recuperarContratoDTO: Optional["RecuperarContratoDTO"] = None


class PaginaRetornoConsultarInstrumentoCobrancaDTO(BaseModel):
    """
    Representa uma resposta paginada para instrumentos de cobrança.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: List[ConsultarInstrumentoCobrancaDTO]
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None


class TipoContrato(BaseModel):
    """
    Representa um tipo de contrato.
    Baseado na seção 6.6 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 7).
    """

    id: Optional[int] = None
    nome: Optional[str] = None


class RecuperarContratoDTO(BaseModel):
    """
    Representa um contrato.
    Baseado na seção 6.6 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).

    Exemplo de JSON:
    ```json
    {
      "codigoPaisFornecedor": "BR",
      "numeroControlePncpCompra": "12345678901234567890-1-123456/2024",
      "numeroControlePNCP": "12345678901234567890-2-789012/2024",
      "anoContrato": 2024,
      "tipoContrato": {
        "id": 1,
        "nome": "Contrato (termo inicial)"
      },
      "numeroContratoEmpenho": "CONTRATO-001/2024",
      "dataAssinatura": "2024-01-25T00:00:00Z",
      "dataVigenciaInicio": "2024-02-01T00:00:00Z",
      "dataVigenciaFim": "2025-01-31T00:00:00Z",
      "niFornecedor": "12345678000199",
      "tipoPessoa": "PJ",
      "orgaoEntidade": {
        "cnpj": "12345678000100",
        "poderId": "E",
        "esferaId": "M",
        "razaoSocial": "PREFEITURA MUNICIPAL DE EXEMPLO"
      },
      "categoriaProcesso": {
        "id": 2,
        "nome": "Compras"
      },
      "dataPublicacaoPncp": "2024-01-30T10:00:00Z",
      "dataAtualizacao": "2024-01-30T10:00:00Z",
      "sequencialContrato": 789012,
      "unidadeOrgao": {
        "ufNome": "São Paulo",
        "codigoUnidade": "UNID001",
        "nomeUnidade": "Secretaria de Compras",
        "ufSigla": "SP",
        "municipioNome": "São Paulo",
        "codigoIbge": "3550308"
      },
      "informacaoComplementar": "Contrato para fornecimento de bens e serviços.",
      "processo": "PROCESSO-ABC-2024",
      "nomeRazaoSocialFornecedor": "FORNECEDOR LTDA",
      "receita": false,
      "numeroParcelas": 12,
      "objetoContrato": "Fornecimento de materiais de escritório",
      "valorInicial": 100000.00,
      "valorParcela": 8333.33,
      "valorGlobal": 100000.00,
      "valorAcumulado": 100000.00,
      "dataAtualizacaoGlobal": "2024-01-30T10:00:00Z",
      "usuarioNome": "Sistema Interno"
    }
    ```
    """

    codigoPaisFornecedor: Optional[str] = None
    numeroControlePncpCompra: Optional[str] = None
    numeroControlePNCP: Optional[str] = None
    anoContrato: Optional[int] = None
    tipoContrato: Optional[TipoContrato] = None
    numeroContratoEmpenho: Optional[str] = None
    dataAssinatura: Optional[str] = None
    dataVigenciaInicio: Optional[str] = None
    dataVigenciaFim: Optional[str] = None
    niFornecedor: Optional[str] = None
    tipoPessoa: Optional[TipoPessoa] = None
    orgaoEntidade: Optional[RecuperarOrgaoEntidadeDTO] = None
    categoriaProcesso: Optional[Categoria] = None
    dataPublicacaoPncp: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    sequencialContrato: Optional[int] = None
    unidadeOrgao: Optional[RecuperarUnidadeOrgaoDTO] = None
    informacaoComplementar: Optional[str] = None
    processo: Optional[str] = None
    unidadeSubRogada: Optional[RecuperarUnidadeOrgaoDTO] = None
    orgaoSubRogado: Optional[RecuperarOrgaoEntidadeDTO] = None
    nomeRazaoSocialFornecedor: Optional[str] = None
    niFornecedorSubContratado: Optional[str] = None
    nomeFornecedorSubContratado: Optional[str] = None
    receita: Optional[bool] = None
    numeroParcelas: Optional[int] = None
    numeroRetificacao: Optional[int] = None
    tipoPessoaSubContratada: Optional[TipoPessoa] = None
    objetoContrato: Optional[str] = None
    valorInicial: Optional[float] = None
    valorParcela: Optional[float] = None
    valorGlobal: Optional[float] = None
    valorAcumulado: Optional[float] = None
    dataAtualizacaoGlobal: Optional[str] = None
    identificadorCipi: Optional[str] = None
    urlCipi: Optional[str] = None
    usuarioNome: Optional[str] = None


class PaginaRetornoRecuperarContratoDTO(BaseModel):
    """
    Representa uma resposta paginada para contratos.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: List[RecuperarContratoDTO]
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None


class RecuperarCompraPublicacaoDTO(BaseModel):
    """
    Representa uma publicação de compra.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).
    """

    numeroControlePNCP: Optional[str] = None
    srp: Optional[bool] = None
    orgaoEntidade: Optional[RecuperarOrgaoEntidadeDTO] = None
    anoCompra: Optional[int] = None
    sequencialCompra: Optional[int] = None
    dataInclusao: Optional[str] = None
    dataPublicacaoPncp: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    numeroCompra: Optional[str] = None
    unidadeOrgao: Optional[RecuperarUnidadeOrgaoDTO] = None
    amparoLegal: Optional[RecuperarAmparoLegalDTO] = None
    dataAberturaProposta: Optional[str] = None
    dataEncerramentoProposta: Optional[str] = None
    informacaoComplementar: Optional[str] = None
    processo: Optional[str] = None
    objetoCompra: Optional[str] = None
    linkSistemaOrigem: Optional[str] = None
    justificativaPresencial: Optional[str] = None
    unidadeSubRogada: Optional[RecuperarUnidadeOrgaoDTO] = None
    orgaoSubRogado: Optional[RecuperarOrgaoEntidadeDTO] = None
    valorTotalHomologado: Optional[float] = None
    modoDisputaId: Optional[int] = None
    modalidadeId: Optional[int] = None
    linkProcessoEletronico: Optional[str] = None
    dataAtualizacaoGlobal: Optional[str] = None
    valorTotalEstimado: Optional[float] = None
    modalidadeNome: Optional[str] = None
    modoDisputaNome: Optional[str] = None
    tipoInstrumentoConvocatorioCodigo: Optional[int] = None
    tipoInstrumentoConvocatorioNome: Optional[str] = None
    fontesOrcamentarias: List[ContratacaoFonteOrcamentariaDTO]
    situacaoCompraId: Optional[SituacaoCompra] = None
    situacaoCompraNome: Optional[str] = None
    usuarioNome: Optional[str] = None
    
    @field_validator('situacaoCompraId', mode='before')
    @classmethod
    def coerce_situacao_compra_id(cls, v):
        """Handle API returning integers instead of strings as specified in OpenAPI."""
        if isinstance(v, int):
            return str(v)  # Convert int to string to match OpenAPI spec
        return v


class PaginaRetornoRecuperarCompraPublicacaoDTO(BaseModel):
    """
    Representa uma resposta paginada para publicações de compra.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: List[RecuperarCompraPublicacaoDTO]
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None


class AtaRegistroPrecoPeriodoDTO(BaseModel):
    """
    Representa uma ata de registro de preço por período.
    Baseado na seção 6.5 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).
    """

    numeroControlePNCPAta: Optional[str] = None
    numeroAtaRegistroPreco: Optional[str] = None
    anoAta: Optional[int] = None
    numeroControlePNCPCompra: Optional[str] = None
    cancelado: Optional[bool] = None
    dataCancelamento: Optional[str] = None
    dataAssinatura: Optional[str] = None
    vigenciaInicio: Optional[str] = None
    vigenciaFim: Optional[str] = None
    dataPublicacaoPncp: Optional[str] = None
    dataInclusao: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    dataAtualizacaoGlobal: Optional[str] = None
    usuario: Optional[str] = None
    objetoContratacao: Optional[str] = None
    cnpjOrgao: Optional[str] = None
    nomeOrgao: Optional[str] = None
    cnpjOrgaoSubrogado: Optional[str] = None
    nomeOrgaoSubrogado: Optional[str] = None
    codigoUnidadeOrgao: Optional[str] = None
    nomeUnidadeOrgao: Optional[str] = None
    codigoUnidadeOrgaoSubrogado: Optional[str] = None
    nomeUnidadeOrgaoSubrogado: Optional[str] = None


class PaginaRetornoAtaRegistroPrecoPeriodoDTO(BaseModel):
    """
    Representa uma resposta paginada para atas de registro de preço.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: List[AtaRegistroPrecoPeriodoDTO]
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None
