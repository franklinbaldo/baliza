from pydantic import BaseModel
from typing import List, Optional


class RespostaErroValidacaoDTO(BaseModel):
    # TODO: Add docstrings to all models to explain the purpose of each
    # model and its fields.
    message: Optional[str] = None
    path: Optional[str] = None
    timestamp: Optional[str] = None
    status: Optional[str] = None
    error: Optional[str] = None


class PlanoContratacaoItemDTO(BaseModel):
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
    data: Optional[List[PlanoContratacaoComItensDoUsuarioDTO]] = None
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None


class ContratacaoFonteOrcamentariaDTO(BaseModel):
    codigo: Optional[int] = None
    nome: Optional[str] = None
    descricao: Optional[str] = None
    dataInclusao: Optional[str] = None


class RecuperarAmparoLegalDTO(BaseModel):
    descricao: Optional[str] = None
    nome: Optional[str] = None
    codigo: Optional[int] = None


class RecuperarOrgaoEntidadeDTO(BaseModel):
    cnpj: Optional[str] = None
    poderId: Optional[str] = None
    esferaId: Optional[str] = None
    razaoSocial: Optional[str] = None


class RecuperarUnidadeOrgaoDTO(BaseModel):
    ufNome: Optional[str] = None
    codigoUnidade: Optional[str] = None
    nomeUnidade: Optional[str] = None
    ufSigla: Optional[str] = None
    municipioNome: Optional[str] = None
    codigoIbge: Optional[str] = None


class RecuperarCompraDTO(BaseModel):
    valorTotalEstimado: Optional[float] = None
    valorTotalHomologado: Optional[float] = None
    indicadorOrcamentoSigiloso: Optional[str] = None
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
    situacaoCompraId: Optional[str] = None
    situacaoCompraNome: Optional[str] = None
    existeResultado: Optional[bool] = None
    dataInclusao: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    dataAtualizacaoGlobal: Optional[str] = None
    usuarioNome: Optional[str] = None


class Categoria(BaseModel):
    id: Optional[int] = None
    nome: Optional[str] = None


class NotaFiscalEletronicaConsultaDTO(BaseModel):
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
    tipoEventoMaisRecente: Optional[str] = None
    dataTipoEventoMaisRecente: Optional[str] = None
    dataInclusao: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    itens: Optional[List["ItemNotaFiscalConsultaDTO"]] = None
    eventos: Optional[List["EventoNotaFiscalConsultaDTO"]] = None


class ItemNotaFiscalConsultaDTO(BaseModel):
    # FIXME: Some of these fields are typed as `str` when they should
    # probably be `float` or `int`. This could lead to data inconsistencies
    # and should be corrected.
    numeroItem: Optional[str] = None
    descricaoProdutoServico: Optional[str] = None
    codigoNCM: Optional[str] = None
    descricaoNCM: Optional[str] = None
    cfop: Optional[str] = None
    quantidade: Optional[str] = None
    unidade: Optional[str] = None
    valorUnitario: Optional[str] = None
    valorTotal: Optional[str] = None


class EventoNotaFiscalConsultaDTO(BaseModel):
    dataEvento: Optional[str] = None
    tipoEvento: Optional[str] = None
    evento: Optional[str] = None
    motivoEvento: Optional[str] = None


class TipoInstrumentoCobrancaDTO(BaseModel):
    id: Optional[int] = None
    nome: Optional[str] = None
    descricao: Optional[str] = None
    dataInclusao: Optional[str] = None
    dataAtualizacao: Optional[str] = None
    statusAtivo: Optional[bool] = None


class ConsultarInstrumentoCobrancaDTO(BaseModel):
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
    data: Optional[List[ConsultarInstrumentoCobrancaDTO]] = None
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None


class TipoContrato(BaseModel):
    id: Optional[int] = None
    nome: Optional[str] = None


class RecuperarContratoDTO(BaseModel):
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
    tipoPessoa: Optional[str] = None
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
    tipoPessoaSubContratada: Optional[str] = None
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
    data: Optional[List[RecuperarContratoDTO]] = None
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None


class RecuperarCompraPublicacaoDTO(BaseModel):
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
    fontesOrcamentarias: Optional[List[ContratacaoFonteOrcamentariaDTO]] = None
    situacaoCompraId: Optional[str] = None
    situacaoCompraNome: Optional[str] = None
    usuarioNome: Optional[str] = None


class PaginaRetornoRecuperarCompraPublicacaoDTO(BaseModel):
    data: Optional[List[RecuperarCompraPublicacaoDTO]] = None
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None


class AtaRegistroPrecoPeriodoDTO(BaseModel):
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
    data: Optional[List[AtaRegistroPrecoPeriodoDTO]] = None
    totalRegistros: Optional[int] = None
    totalPaginas: Optional[int] = None
    numeroPagina: Optional[int] = None
    paginasRestantes: Optional[int] = None
    empty: Optional[bool] = None
