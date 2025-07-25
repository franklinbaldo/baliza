from typing import List, Optional

from pydantic import BaseModel

from .enums import (
    IndicadorOrcamentoSigiloso,
    SituacaoCompra,
    TipoEventoNotaFiscal,
    TipoPessoa,
)


class RespostaErroValidacaoDTO(BaseModel):
    """Represents a validation error response."""

    message: str
    path: str
    timestamp: str
    status: str
    error: str


class PlanoContratacaoItemDTO(BaseModel):
    """Represents an item in a procurement plan."""

    nomeClassificacaoCatalogo: str
    descricaoItem: str
    quantidadeEstimada: float
    pdmCodigo: str
    dataInclusao: str
    numeroItem: int
    dataAtualizacao: str
    valorTotal: float
    pdmDescricao: str
    codigoItem: str
    unidadeRequisitante: str
    grupoContratacaoCodigo: str
    grupoContratacaoNome: str
    classificacaoSuperiorCodigo: str
    classificacaoSuperiorNome: str
    unidadeFornecimento: str
    valorUnitario: float
    valorOrcamentoExercicio: float
    dataDesejada: str
    categoriaItemPcaNome: str
    classificacaoCatalogoId: int


class PlanoContratacaoComItensDoUsuarioDTO(BaseModel):
    """Represents a procurement plan with items for a user."""

    itens: List[PlanoContratacaoItemDTO]
    codigoUnidade: str
    nomeUnidade: str
    anoPca: int
    dataPublicacaoPNCP: str
    dataAtualizacaoGlobalPCA: str
    idPcaPncp: str
    orgaoEntidadeCnpj: str
    orgaoEntidadeRazaoSocial: str


class PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO(BaseModel):
    """Represents a paginated response for procurement plans."""

    data: List[PlanoContratacaoComItensDoUsuarioDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool


class ContratacaoFonteOrcamentariaDTO(BaseModel):
    """Represents a budgetary source for a contract."""

    codigo: int
    nome: str
    descricao: str
    dataInclusao: str


class RecuperarAmparoLegalDTO(BaseModel):
    """Represents the legal support for a contract."""

    descricao: str
    nome: str
    codigo: int


class RecuperarOrgaoEntidadeDTO(BaseModel):
    """Represents an organization entity."""

    cnpj: str
    poderId: str
    esferaId: str
    razaoSocial: str


class RecuperarUnidadeOrgaoDTO(BaseModel):
    """Represents an organization unit."""

    ufNome: str
    codigoUnidade: str
    nomeUnidade: str
    ufSigla: str
    municipioNome: str
    codigoIbge: str


class RecuperarCompraDTO(BaseModel):
    """Represents a purchase."""

    valorTotalEstimado: float
    valorTotalHomologado: float
    indicadorOrcamentoSigiloso: Optional[IndicadorOrcamentoSigiloso] = None
    orcamentoSigilosoCodigo: Optional[int] = None
    orcamentoSigilosoDescricao: Optional[str] = None
    numeroControlePNCP: str
    linkSistemaOrigem: Optional[str] = None
    linkProcessoEletronico: Optional[str] = None
    anoCompra: int
    sequencialCompra: int
    numeroCompra: str
    processo: str
    orgaoEntidade: RecuperarOrgaoEntidadeDTO
    unidadeOrgao: RecuperarUnidadeOrgaoDTO
    orgaoSubRogado: Optional[RecuperarOrgaoEntidadeDTO] = None
    unidadeSubRogada: Optional[RecuperarUnidadeOrgaoDTO] = None
    modalidadeId: int
    modalidadeNome: str
    justificativaPresencial: Optional[str] = None
    modoDisputaId: int
    modoDisputaNome: str
    tipoInstrumentoConvocatorioCodigo: int
    tipoInstrumentoConvocatorioNome: str
    amparoLegal: RecuperarAmparoLegalDTO
    objetoCompra: str
    informacaoComplementar: Optional[str] = None
    srp: bool
    fontesOrcamentarias: List[ContratacaoFonteOrcamentariaDTO]
    dataPublicacaoPncp: str
    dataAberturaProposta: str
    dataEncerramentoProposta: str
    situacaoCompraId: SituacaoCompra
    situacaoCompraNome: str
    existeResultado: bool
    dataInclusao: str
    dataAtualizacao: str
    dataAtualizacaoGlobal: str
    usuarioNome: str


class Categoria(BaseModel):
    """Represents a category."""

    id: int
    nome: str


class NotaFiscalEletronicaConsultaDTO(BaseModel):
    """Represents an electronic invoice for consultation."""

    instrumentoCobrancaId: int
    chave: str
    nfTransparenciaID: int
    numero: int
    serie: int
    dataEmissao: str
    niEmitente: str
    nomeEmitente: str
    nomeMunicipioEmitente: str
    codigoOrgaoDestinatario: str
    nomeOrgaoDestinatario: str
    codigoOrgaoSuperiorDestinatario: str
    nomeOrgaoSuperiorDestinatario: str
    valorNotaFiscal: str
    tipoEventoMaisRecente: Optional[TipoEventoNotaFiscal] = None
    dataTipoEventoMaisRecente: Optional[str] = None
    dataInclusao: str
    dataAtualizacao: str
    itens: List["ItemNotaFiscalConsultaDTO"]
    eventos: List["EventoNotaFiscalConsultaDTO"]


class ItemNotaFiscalConsultaDTO(BaseModel):
    """Represents an item in an electronic invoice."""

    numeroItem: int
    descricaoProdutoServico: str
    codigoNCM: str
    descricaoNCM: str
    cfop: str
    quantidade: float
    unidade: str
    valorUnitario: float
    valorTotal: float


class EventoNotaFiscalConsultaDTO(BaseModel):
    """Represents an event in an electronic invoice."""

    dataEvento: str
    tipoEvento: TipoEventoNotaFiscal
    evento: str
    motivoEvento: str


class TipoInstrumentoCobrancaDTO(BaseModel):
    """Represents a type of collection instrument."""

    id: int
    nome: str
    descricao: str
    dataInclusao: str
    dataAtualizacao: str
    statusAtivo: bool


class ConsultarInstrumentoCobrancaDTO(BaseModel):
    """Represents a collection instrument for consultation."""

    cnpj: str
    ano: int
    sequencialContrato: int
    sequencialInstrumentoCobranca: int
    tipoInstrumentoCobranca: TipoInstrumentoCobrancaDTO
    numeroInstrumentoCobranca: str
    dataEmissaoDocumento: str
    observacao: Optional[str] = None
    chaveNFe: Optional[str] = None
    fonteNFe: Optional[int] = None
    dataConsultaNFe: Optional[str] = None
    statusResponseNFe: Optional[str] = None
    jsonResponseNFe: Optional[str] = None
    notaFiscalEletronica: Optional[NotaFiscalEletronicaConsultaDTO] = None
    dataInclusao: str
    dataAtualizacao: str
    recuperarContratoDTO: Optional["RecuperarContratoDTO"] = None


class PaginaRetornoConsultarInstrumentoCobrancaDTO(BaseModel):
    """Represents a paginated response for collection instruments."""

    data: List[ConsultarInstrumentoCobrancaDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool


class TipoContrato(BaseModel):
    """Represents a contract type."""

    id: int
    nome: str


class RecuperarContratoDTO(BaseModel):
    """Represents a contract."""

    codigoPaisFornecedor: Optional[str] = None
    numeroControlePncpCompra: str
    numeroControlePNCP: str
    anoContrato: int
    tipoContrato: TipoContrato
    numeroContratoEmpenho: str
    dataAssinatura: str
    dataVigenciaInicio: str
    dataVigenciaFim: str
    niFornecedor: str
    tipoPessoa: TipoPessoa
    orgaoEntidade: RecuperarOrgaoEntidadeDTO
    categoriaProcesso: Categoria
    dataPublicacaoPncp: str
    dataAtualizacao: str
    sequencialContrato: int
    unidadeOrgao: RecuperarUnidadeOrgaoDTO
    informacaoComplementar: Optional[str] = None
    processo: str
    unidadeSubRogada: Optional[RecuperarUnidadeOrgaoDTO] = None
    orgaoSubRogado: Optional[RecuperarOrgaoEntidadeDTO] = None
    nomeRazaoSocialFornecedor: str
    niFornecedorSubContratado: Optional[str] = None
    nomeFornecedorSubContratado: Optional[str] = None
    receita: bool
    numeroParcelas: int
    numeroRetificacao: Optional[int] = None
    tipoPessoaSubContratada: Optional[TipoPessoa] = None
    objetoContrato: str
    valorInicial: float
    valorParcela: float
    valorGlobal: float
    valorAcumulado: float
    dataAtualizacaoGlobal: str
    identificadorCipi: Optional[str] = None
    urlCipi: Optional[str] = None
    usuarioNome: str


class PaginaRetornoRecuperarContratoDTO(BaseModel):
    """Represents a paginated response for contracts."""

    data: List[RecuperarContratoDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool


class RecuperarCompraPublicacaoDTO(BaseModel):
    """Represents a purchase publication."""

    numeroControlePNCP: str
    srp: bool
    orgaoEntidade: RecuperarOrgaoEntidadeDTO
    anoCompra: int
    sequencialCompra: int
    dataInclusao: str
    dataPublicacaoPncp: str
    dataAtualizacao: str
    numeroCompra: str
    unidadeOrgao: RecuperarUnidadeOrgaoDTO
    amparoLegal: RecuperarAmparoLegalDTO
    dataAberturaProposta: str
    dataEncerramentoProposta: str
    informacaoComplementar: Optional[str] = None
    processo: str
    objetoCompra: str
    linkSistemaOrigem: Optional[str] = None
    justificativaPresencial: Optional[str] = None
    unidadeSubRogada: Optional[RecuperarUnidadeOrgaoDTO] = None
    orgaoSubRogado: Optional[RecuperarOrgaoEntidadeDTO] = None
    valorTotalHomologado: float
    modoDisputaId: int
    modalidadeId: int
    linkProcessoEletronico: Optional[str] = None
    dataAtualizacaoGlobal: str
    valorTotalEstimado: float
    modalidadeNome: str
    modoDisputaNome: str
    tipoInstrumentoConvocatorioCodigo: int
    tipoInstrumentoConvocatorioNome: str
    fontesOrcamentarias: List[ContratacaoFonteOrcamentariaDTO]
    situacaoCompraId: SituacaoCompra
    situacaoCompraNome: str
    usuarioNome: str


class PaginaRetornoRecuperarCompraPublicacaoDTO(BaseModel):
    """Represents a paginated response for purchase publications."""

    data: List[RecuperarCompraPublicacaoDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool


class AtaRegistroPrecoPeriodoDTO(BaseModel):
    """Represents a price registration act for a period."""

    numeroControlePNCPAta: str
    numeroAtaRegistroPreco: str
    anoAta: int
    numeroControlePNCPCompra: str
    cancelado: bool
    dataCancelamento: Optional[str] = None
    dataAssinatura: str
    vigenciaInicio: str
    vigenciaFim: str
    dataPublicacaoPncp: str
    dataInclusao: str
    dataAtualizacao: str
    dataAtualizacaoGlobal: str
    usuario: str
    objetoContratacao: str
    cnpjOrgao: str
    nomeOrgao: str
    cnpjOrgaoSubrogado: Optional[str] = None
    nomeOrgaoSubrogado: Optional[str] = None
    codigoUnidadeOrgao: str
    nomeUnidadeOrgao: str
    codigoUnidadeOrgaoSubrogado: Optional[str] = None
    nomeUnidadeOrgaoSubrogado: Optional[str] = None


class PaginaRetornoAtaRegistroPrecoPeriodoDTO(BaseModel):
    """Represents a paginated response for price registration acts."""

    data: List[AtaRegistroPrecoPeriodoDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool
