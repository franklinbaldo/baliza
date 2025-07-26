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
    """
    Representa um item em um plano de contratação.
    Baseado na seção 6.1 e 6.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 8.x).
    """

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
    """
    Representa um plano de contratação com itens para um usuário.
    Baseado na seção 6.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).
    """

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
    """
    Representa uma resposta paginada para planos de contratação.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: List[PlanoContratacaoComItensDoUsuarioDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool


class ContratacaoFonteOrcamentariaDTO(BaseModel):
    """
    Representa uma fonte orçamentária para uma contratação.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 17).
    """

    codigo: int
    nome: str
    descricao: str
    dataInclusao: str


class RecuperarAmparoLegalDTO(BaseModel):
    """
    Representa o amparo legal para uma contratação.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 16).
    """

    descricao: str
    nome: str
    codigo: int


class RecuperarOrgaoEntidadeDTO(BaseModel):
    """
    Representa uma entidade de órgão.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 25).
    """

    cnpj: str
    poderId: str
    esferaId: str
    razaoSocial: str


class RecuperarUnidadeOrgaoDTO(BaseModel):
    """
    Representa uma unidade de órgão.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 26).
    """

    ufNome: str
    codigoUnidade: str
    nomeUnidade: str
    ufSigla: str
    municipioNome: str
    codigoIbge: str


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
    """
    Representa uma categoria.
    Baseado na seção 6.6 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 8).
    """

    id: int
    nome: str


class NotaFiscalEletronicaConsultaDTO(BaseModel):
    """
    Representa uma nota fiscal eletrônica para consulta.
    Baseado na seção 6.5.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (implied from context, not explicitly listed).

    Note: Forward references handle circular dependencies in PNCP's complex API structure.
    These models represent the nested structure of the electronic invoice system.
    """

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
    """
    Representa um item em uma nota fiscal eletrônica.
    Baseado na seção 6.5.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (implied from context).
    """

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
    """
    Representa um evento em uma nota fiscal eletrônica.
    Baseado na seção 6.5.1 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (implied from context).
    """

    dataEvento: str
    tipoEvento: TipoEventoNotaFiscal
    evento: str
    motivoEvento: str


class TipoInstrumentoCobrancaDTO(BaseModel):
    """
    Representa um tipo de instrumento de cobrança.
    Baseado na seção 5.18 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (implied from context, not explicitly listed as domain table).
    """

    id: int
    nome: str
    descricao: str
    dataInclusao: str
    dataAtualizacao: str
    statusAtivo: bool


class ConsultarInstrumentoCobrancaDTO(BaseModel):
    """
    Representa um instrumento de cobrança para consulta.
    Baseado na seção 6.5 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).
    """

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
    """
    Representa uma resposta paginada para instrumentos de cobrança.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: List[ConsultarInstrumentoCobrancaDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool


class TipoContrato(BaseModel):
    """
    Representa um tipo de contrato.
    Baseado na seção 6.6 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno, item 7).
    """

    id: int
    nome: str


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
    """
    Representa uma resposta paginada para contratos.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: List[RecuperarContratoDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool


class RecuperarCompraPublicacaoDTO(BaseModel):
    """
    Representa uma publicação de compra.
    Baseado na seção 6.3 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).
    """

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
    """
    Representa uma resposta paginada para publicações de compra.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: List[RecuperarCompraPublicacaoDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool


class AtaRegistroPrecoPeriodoDTO(BaseModel):
    """
    Representa uma ata de registro de preço por período.
    Baseado na seção 6.5 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de retorno).
    """

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
    """
    Representa uma resposta paginada para atas de registro de preço.
    Baseado na seção 4.2 do MANUAL-PNCP-CONSULTAS-VERSAO-1.md (Dados de Retorno padronizados).
    """

    data: List[AtaRegistroPrecoPeriodoDTO]
    totalRegistros: int
    totalPaginas: int
    numeroPagina: int
    paginasRestantes: int
    empty: bool
