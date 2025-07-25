"""Pydantic schemas for PNCP API responses - Direct mapping to bronze tables."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import uuid4
import re

from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator


class OrgaoEntidadeDTO(BaseModel):
    """Schema for RecuperarOrgaoEntidadeDTO from OpenAPI."""

    cnpj: str
    razao_social: str
    poder_id: str
    esfera_id: str


class UnidadeOrgaoDTO(BaseModel):
    """Schema for RecuperarUnidadeOrgaoDTO from OpenAPI."""

    uf_nome: str
    codigo_unidade: str
    nome_unidade: str
    uf_sigla: str
    municipio_nome: str
    codigo_ibge: str


class TipoContratoDTO(BaseModel):
    """Schema for TipoContrato from OpenAPI."""

    id: int
    nome: str


class CategoriaDTO(BaseModel):
    """Schema for Categoria from OpenAPI."""

    id: int
    nome: str


class AmparoLegalDTO(BaseModel):
    """Schema for RecuperarAmparoLegalDTO from OpenAPI."""

    codigo: int
    nome: str
    descricao: str


class FonteOrcamentariaDTO(BaseModel):
    """Schema for ContratacaoFonteOrcamentariaDTO from OpenAPI."""

    codigo: int
    nome: str
    descricao: str
    data_inclusao: datetime


# === BRONZE TABLE MODELS ===


class BronzeContrato(BaseModel):
    """Direct mapping to bronze_contratos table."""

    model_config = ConfigDict(
        str_strip_whitespace=True, validate_assignment=True, use_enum_values=True
    )

    # Identificadores
    numero_controle_pncp_compra: Optional[str] = None
    numero_controle_pncp: str
    ano_contrato: int
    sequencial_contrato: int
    numero_contrato_empenho: Optional[str] = None

    # Datas
    data_assinatura: Optional[date] = None
    data_vigencia_inicio: Optional[date] = None
    data_vigencia_fim: Optional[date] = None
    data_publicacao_pncp: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    data_atualizacao_global: Optional[datetime] = None

    # Fornecedor
    ni_fornecedor: Optional[str] = None
    tipo_pessoa: Optional[str] = Field(None, max_length=2)  # PJ, PF, PE
    nome_razao_social_fornecedor: Optional[str] = None
    codigo_pais_fornecedor: Optional[str] = None

    # Subcontratação
    ni_fornecedor_subcontratado: Optional[str] = None
    nome_fornecedor_subcontratado: Optional[str] = None
    tipo_pessoa_subcontratada: Optional[str] = Field(None, max_length=2)

    # Orgão/Entidade
    orgao_cnpj: Optional[str] = Field(None, max_length=14)
    orgao_razao_social: Optional[str] = None
    orgao_poder_id: Optional[str] = None
    orgao_esfera_id: Optional[str] = None

    # Unidade
    unidade_codigo: Optional[str] = None
    unidade_nome: Optional[str] = None
    unidade_uf_sigla: Optional[str] = Field(None, max_length=2)
    unidade_uf_nome: Optional[str] = None
    unidade_municipio_nome: Optional[str] = None
    unidade_codigo_ibge: Optional[str] = None

    # Subrogação
    orgao_subrogado_cnpj: Optional[str] = Field(None, max_length=14)
    orgao_subrogado_razao_social: Optional[str] = None
    unidade_subrogada_codigo: Optional[str] = None
    unidade_subrogada_nome: Optional[str] = None

    # Tipo e Categoria
    tipo_contrato_id: Optional[int] = None
    tipo_contrato_nome: Optional[str] = None
    categoria_processo_id: Optional[int] = None
    categoria_processo_nome: Optional[str] = None

    # Valores
    valor_inicial: Optional[Decimal] = None
    valor_parcela: Optional[Decimal] = None
    valor_global: Optional[Decimal] = None
    valor_acumulado: Optional[Decimal] = None

    # Parcelas e Controle
    numero_parcelas: Optional[int] = None
    numero_retificacao: Optional[int] = None
    receita: Optional[bool] = None

    # Texto e Observações
    objeto_contrato: Optional[str] = None
    informacao_complementar: Optional[str] = None
    processo: Optional[str] = None

    # CIPI
    identificador_cipi: Optional[str] = None
    url_cipi: Optional[str] = None

    # Metadados
    usuario_nome: Optional[str] = None
    extracted_at: datetime = Field(default_factory=datetime.now)

    # FIXME: The CNPJ and CPF validation logic is repeated in multiple
    # models. It would be better to create custom Pydantic types for these
    # fields to reduce code duplication and improve reusability.
    @field_validator("orgao_cnpj", "orgao_subrogado_cnpj", mode="before")
    @classmethod
    def validate_cnpj(cls, v: Optional[str]) -> Optional[str]:
        """Valida formato e dígitos verificadores de CNPJ."""
        if v is None or v == "":
            return None

        # Remove formatação
        digits_only = re.sub(r"[^\d]", "", str(v))

        if len(digits_only) != 14:
            raise ValueError(f"CNPJ deve ter 14 dígitos: {v}")

        # Valida dígitos verificadores
        if not cls._validate_cnpj_digits(digits_only):
            raise ValueError(f"CNPJ inválido: {v}")

        return digits_only

    @field_validator("ni_fornecedor", "ni_fornecedor_subcontratado", mode="before")
    @classmethod
    def validate_ni_fornecedor(cls, v: Optional[str]) -> Optional[str]:
        """Valida NI (CNPJ ou CPF) do fornecedor."""
        if v is None or v == "":
            return None

        # Remove formatação
        digits_only = re.sub(r"[^\d]", "", str(v))

        # Valida CNPJ (14 dígitos) ou CPF (11 dígitos)
        if len(digits_only) == 14:
            if not cls._validate_cnpj_digits(digits_only):
                raise ValueError(f"CNPJ inválido: {v}")
        elif len(digits_only) == 11:
            if not cls._validate_cpf_digits(digits_only):
                raise ValueError(f"CPF inválido: {v}")
        else:
            raise ValueError(f"NI deve ter 11 (CPF) ou 14 (CNPJ) dígitos: {v}")

        return digits_only

    @field_validator(
        "valor_inicial", "valor_parcela", "valor_global", "valor_acumulado"
    )
    @classmethod
    def validate_positive_values(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Garante que valores monetários não são negativos."""
        if v is not None and v < 0:
            raise ValueError(f"Valor não pode ser negativo: {v}")
        return v

    @field_validator("tipo_pessoa", "tipo_pessoa_subcontratada")
    @classmethod
    def validate_tipo_pessoa(cls, v: Optional[str]) -> Optional[str]:
        """Valida tipo de pessoa."""
        if v is None:
            return v
        v_upper = v.upper()
        if v_upper not in ["PJ", "PF", "PE"]:
            raise ValueError(f"Tipo pessoa deve ser PJ, PF ou PE: {v}")
        return v_upper

    @field_validator("unidade_uf_sigla")
    @classmethod
    def validate_uf_sigla(cls, v: Optional[str]) -> Optional[str]:
        """Valida sigla da UF."""
        if v is None:
            return v
        v_upper = v.upper()
        valid_ufs = {
            "AC",
            "AL",
            "AP",
            "AM",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MT",
            "MS",
            "MG",
            "PA",
            "PB",
            "PR",
            "PE",
            "PI",
            "RJ",
            "RN",
            "RS",
            "RO",
            "RR",
            "SC",
            "SP",
            "SE",
            "TO",
        }
        if v_upper not in valid_ufs:
            raise ValueError(f"UF inválida: {v}")
        return v_upper

    @model_validator(mode="after")
    def validate_date_consistency(self) -> "BronzeContrato":
        """Valida consistência entre datas."""
        if (
            self.data_vigencia_inicio
            and self.data_vigencia_fim
            and self.data_vigencia_inicio > self.data_vigencia_fim
        ):
            raise ValueError(
                "Data de início da vigência não pode ser posterior à data de fim"
            )

        # Data de assinatura não pode ser no futuro
        if self.data_assinatura and self.data_assinatura > date.today():
            raise ValueError("Data de assinatura não pode ser no futuro")

        return self

    @classmethod
    def _validate_cnpj_digits(cls, cnpj: str) -> bool:
        """Valida dígitos verificadores do CNPJ."""
        # Algoritmo oficial de validação CNPJ
        if len(cnpj) != 14:
            return False

        # Verifica se todos os dígitos são iguais
        if cnpj == cnpj[0] * 14:
            return False

        # Validação do primeiro dígito verificador
        weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum1 = sum(int(cnpj[i]) * weights[i] for i in range(12))
        remainder1 = sum1 % 11
        digit1 = 0 if remainder1 < 2 else 11 - remainder1

        if int(cnpj[12]) != digit1:
            return False

        # Validação do segundo dígito verificador
        weights = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum2 = sum(int(cnpj[i]) * weights[i] for i in range(13))
        remainder2 = sum2 % 11
        digit2 = 0 if remainder2 < 2 else 11 - remainder2

        return int(cnpj[13]) == digit2

    @classmethod
    def _validate_cpf_digits(cls, cpf: str) -> bool:
        """Valida dígitos verificadores do CPF."""
        if len(cpf) != 11:
            return False

        # Verifica se todos os dígitos são iguais
        if cpf == cpf[0] * 11:
            return False

        # Validação do primeiro dígito verificador
        sum1 = sum(int(cpf[i]) * (10 - i) for i in range(9))
        remainder1 = sum1 % 11
        digit1 = 0 if remainder1 < 2 else 11 - remainder1

        if int(cpf[9]) != digit1:
            return False

        # Validação do segundo dígito verificador
        sum2 = sum(int(cpf[i]) * (11 - i) for i in range(10))
        remainder2 = sum2 % 11
        digit2 = 0 if remainder2 < 2 else 11 - remainder2

        return int(cpf[10]) == digit2

    @classmethod
    def from_api_response(cls, contrato_data: dict) -> "BronzeContrato":
        """Parse RecuperarContratoDTO from API to BronzeContrato."""
        # Map nested objects
        orgao_entidade = contrato_data.get("orgaoEntidade", {})
        unidade_orgao = contrato_data.get("unidadeOrgao", {})
        orgao_subrogado = contrato_data.get("orgaoSubRogado", {})
        unidade_subrogada = contrato_data.get("unidadeSubRogada", {})
        tipo_contrato = contrato_data.get("tipoContrato", {})
        categoria_processo = contrato_data.get("categoriaProcesso", {})

        return cls(
            # Identificadores
            numero_controle_pncp_compra=contrato_data.get("numeroControlePncpCompra"),
            numero_controle_pncp=contrato_data["numeroControlePNCP"],
            ano_contrato=contrato_data["anoContrato"],
            sequencial_contrato=contrato_data["sequencialContrato"],
            numero_contrato_empenho=contrato_data.get("numeroContratoEmpenho"),
            # Datas
            data_assinatura=contrato_data.get("dataAssinatura"),
            data_vigencia_inicio=contrato_data.get("dataVigenciaInicio"),
            data_vigencia_fim=contrato_data.get("dataVigenciaFim"),
            data_publicacao_pncp=contrato_data.get("dataPublicacaoPncp"),
            data_atualizacao=contrato_data.get("dataAtualizacao"),
            data_atualizacao_global=contrato_data.get("dataAtualizacaoGlobal"),
            # Fornecedor
            ni_fornecedor=contrato_data.get("niFornecedor"),
            tipo_pessoa=contrato_data.get("tipoPessoa"),
            nome_razao_social_fornecedor=contrato_data.get("nomeRazaoSocialFornecedor"),
            codigo_pais_fornecedor=contrato_data.get("codigoPaisFornecedor"),
            # Subcontratação
            ni_fornecedor_subcontratado=contrato_data.get("niFornecedorSubContratado"),
            nome_fornecedor_subcontratado=contrato_data.get(
                "nomeFornecedorSubContratado"
            ),
            tipo_pessoa_subcontratada=contrato_data.get("tipoPessoaSubContratada"),
            # Orgão/Entidade
            orgao_cnpj=orgao_entidade.get("cnpj"),
            orgao_razao_social=orgao_entidade.get("razaoSocial"),
            orgao_poder_id=orgao_entidade.get("poderId"),
            orgao_esfera_id=orgao_entidade.get("esferaId"),
            # Unidade
            unidade_codigo=unidade_orgao.get("codigoUnidade"),
            unidade_nome=unidade_orgao.get("nomeUnidade"),
            unidade_uf_sigla=unidade_orgao.get("ufSigla"),
            unidade_uf_nome=unidade_orgao.get("ufNome"),
            unidade_municipio_nome=unidade_orgao.get("municipioNome"),
            unidade_codigo_ibge=unidade_orgao.get("codigoIbge"),
            # Subrogação
            orgao_subrogado_cnpj=orgao_subrogado.get("cnpj"),
            orgao_subrogado_razao_social=orgao_subrogado.get("razaoSocial"),
            unidade_subrogada_codigo=unidade_subrogada.get("codigoUnidade"),
            unidade_subrogada_nome=unidade_subrogada.get("nomeUnidade"),
            # Tipo e Categoria
            tipo_contrato_id=tipo_contrato.get("id"),
            tipo_contrato_nome=tipo_contrato.get("nome"),
            categoria_processo_id=categoria_processo.get("id"),
            categoria_processo_nome=categoria_processo.get("nome"),
            # Valores
            valor_inicial=contrato_data.get("valorInicial"),
            valor_parcela=contrato_data.get("valorParcela"),
            valor_global=contrato_data.get("valorGlobal"),
            valor_acumulado=contrato_data.get("valorAcumulado"),
            # Parcelas e Controle
            numero_parcelas=contrato_data.get("numeroParcelas"),
            numero_retificacao=contrato_data.get("numeroRetificacao"),
            receita=contrato_data.get("receita"),
            # Texto e Observações
            objeto_contrato=contrato_data.get("objetoContrato"),
            informacao_complementar=contrato_data.get("informacaoComplementar"),
            processo=contrato_data.get("processo"),
            # CIPI
            identificador_cipi=contrato_data.get("identificadorCipi"),
            url_cipi=contrato_data.get("urlCipi"),
            # Metadados
            usuario_nome=contrato_data.get("usuarioNome"),
        )


class BronzeContratacao(BaseModel):
    """Direct mapping to bronze_contratacoes table."""

    model_config = ConfigDict(
        str_strip_whitespace=True, validate_assignment=True, use_enum_values=True
    )

    # Identificadores
    numero_controle_pncp: str
    ano_compra: int
    sequencial_compra: int
    numero_compra: Optional[str] = None
    processo: Optional[str] = None

    # Datas
    data_inclusao: Optional[datetime] = None
    data_publicacao_pncp: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    data_atualizacao_global: Optional[datetime] = None
    data_abertura_proposta: Optional[datetime] = None
    data_encerramento_proposta: Optional[datetime] = None

    # Orgão/Entidade
    orgao_cnpj: Optional[str] = Field(None, max_length=14)
    orgao_razao_social: Optional[str] = None
    orgao_poder_id: Optional[str] = None
    orgao_esfera_id: Optional[str] = None

    # Unidade
    unidade_codigo: Optional[str] = None
    unidade_nome: Optional[str] = None
    unidade_uf_sigla: Optional[str] = Field(None, max_length=2)
    unidade_uf_nome: Optional[str] = None
    unidade_municipio_nome: Optional[str] = None
    unidade_codigo_ibge: Optional[str] = None

    # Subrogação
    orgao_subrogado_cnpj: Optional[str] = Field(None, max_length=14)
    orgao_subrogado_razao_social: Optional[str] = None
    unidade_subrogada_codigo: Optional[str] = None
    unidade_subrogada_nome: Optional[str] = None

    # Modalidade e Disputa
    modalidade_id: Optional[int] = None
    modalidade_nome: Optional[str] = None
    modo_disputa_id: Optional[int] = None
    modo_disputa_nome: Optional[str] = None

    # Instrumento Convocatório
    tipo_instrumento_convocatorio_codigo: Optional[int] = None
    tipo_instrumento_convocatorio_nome: Optional[str] = None

    # Amparo Legal
    amparo_legal_codigo: Optional[int] = None
    amparo_legal_nome: Optional[str] = None
    amparo_legal_descricao: Optional[str] = None

    # Valores
    valor_total_estimado: Optional[Decimal] = None
    valor_total_homologado: Optional[Decimal] = None

    # Situação
    situacao_compra_id: Optional[str] = None  # ENUM: 1,2,3,4
    situacao_compra_nome: Optional[str] = None

    # Flags
    srp: Optional[bool] = None  # Sistema de Registro de Preços

    # Texto
    objeto_compra: Optional[str] = None
    informacao_complementar: Optional[str] = None
    justificativa_presencial: Optional[str] = None

    # Links
    link_sistema_origem: Optional[str] = None
    link_processo_eletronico: Optional[str] = None

    # Metadados
    usuario_nome: Optional[str] = None
    extracted_at: datetime = Field(default_factory=datetime.now)

    # TODO: Add validation for the `link_sistema_origem` and
    # `link_processo_eletronico` fields to ensure that they are valid URLs.

    @field_validator("orgao_cnpj", "orgao_subrogado_cnpj", mode="before")
    @classmethod
    def validate_cnpj(cls, v: Optional[str]) -> Optional[str]:
        """Valida formato e dígitos verificadores de CNPJ."""
        if v is None or v == "":
            return None
        digits_only = re.sub(r"[^\d]", "", str(v))
        if len(digits_only) != 14:
            raise ValueError(f"CNPJ deve ter 14 dígitos: {v}")
        if not cls._validate_cnpj_digits(digits_only):
            raise ValueError(f"CNPJ inválido: {v}")
        return digits_only

    @field_validator("modalidade_id")
    @classmethod
    def validate_modalidade(cls, v: Optional[int]) -> Optional[int]:
        """Valida se modalidade existe nos ENUMs oficiais PNCP."""
        if v is None:
            return v
        # IDs válidos de modalidade conforme PNCP OpenAPI
        valid_modalidades = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
        if v not in valid_modalidades:
            raise ValueError(
                f"Modalidade inválida: {v}. Deve ser uma de: {sorted(valid_modalidades)}"
            )
        return v

    @field_validator("situacao_compra_id")
    @classmethod
    def validate_situacao(cls, v: Optional[str]) -> Optional[str]:
        """Valida se situação existe nos ENUMs oficiais PNCP."""
        if v is None:
            return v
        # Situações válidas conforme PNCP OpenAPI
        valid_situacoes = {"1", "2", "3", "4"}
        if v not in valid_situacoes:
            raise ValueError(
                f"Situação inválida: {v}. Deve ser uma de: {sorted(valid_situacoes)}"
            )
        return v

    @field_validator("valor_total_estimado", "valor_total_homologado")
    @classmethod
    def validate_positive_values(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Garante que valores monetários não são negativos."""
        if v is not None and v < 0:
            raise ValueError(f"Valor não pode ser negativo: {v}")
        return v

    @field_validator("unidade_uf_sigla")
    @classmethod
    def validate_uf_sigla(cls, v: Optional[str]) -> Optional[str]:
        """Valida sigla da UF."""
        if v is None:
            return v
        v_upper = v.upper()
        valid_ufs = {
            "AC",
            "AL",
            "AP",
            "AM",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MT",
            "MS",
            "MG",
            "PA",
            "PB",
            "PR",
            "PE",
            "PI",
            "RJ",
            "RN",
            "RS",
            "RO",
            "RR",
            "SC",
            "SP",
            "SE",
            "TO",
        }
        if v_upper not in valid_ufs:
            raise ValueError(f"UF inválida: {v}")
        return v_upper

    @model_validator(mode="after")
    def validate_date_consistency(self) -> "BronzeContratacao":
        """Valida consistência entre datas."""
        if (
            self.data_abertura_proposta
            and self.data_encerramento_proposta
            and self.data_abertura_proposta > self.data_encerramento_proposta
        ):
            raise ValueError(
                "Data de abertura não pode ser posterior à data de encerramento"
            )

        # Datas não podem ser no futuro distante (> 1 ano)
        future_limit = date.today().replace(year=date.today().year + 1)
        if (
            self.data_abertura_proposta
            and self.data_abertura_proposta.date() > future_limit
        ):
            raise ValueError("Data de abertura de proposta muito distante no futuro")

        return self

    @classmethod
    def _validate_cnpj_digits(cls, cnpj: str) -> bool:
        """Valida dígitos verificadores do CNPJ."""
        if len(cnpj) != 14:
            return False
        if cnpj == cnpj[0] * 14:
            return False
        # Primeiro dígito verificador
        weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum1 = sum(int(cnpj[i]) * weights[i] for i in range(12))
        remainder1 = sum1 % 11
        digit1 = 0 if remainder1 < 2 else 11 - remainder1
        if int(cnpj[12]) != digit1:
            return False
        # Segundo dígito verificador
        weights = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum2 = sum(int(cnpj[i]) * weights[i] for i in range(13))
        remainder2 = sum2 % 11
        digit2 = 0 if remainder2 < 2 else 11 - remainder2
        return int(cnpj[13]) == digit2

    @classmethod
    def from_api_response(cls, contratacao_data: dict) -> "BronzeContratacao":
        """Parse RecuperarCompraPublicacaoDTO from API to BronzeContratacao."""
        # Map nested objects
        orgao_entidade = contratacao_data.get("orgaoEntidade", {})
        unidade_orgao = contratacao_data.get("unidadeOrgao", {})
        orgao_subrogado = contratacao_data.get("orgaoSubRogado", {})
        unidade_subrogada = contratacao_data.get("unidadeSubRogada", {})
        amparo_legal = contratacao_data.get("amparoLegal", {})

        return cls(
            # Identificadores
            numero_controle_pncp=contratacao_data["numeroControlePNCP"],
            ano_compra=contratacao_data["anoCompra"],
            sequencial_compra=contratacao_data["sequencialCompra"],
            numero_compra=contratacao_data.get("numeroCompra"),
            processo=contratacao_data.get("processo"),
            # Datas
            data_inclusao=contratacao_data.get("dataInclusao"),
            data_publicacao_pncp=contratacao_data.get("dataPublicacaoPncp"),
            data_atualizacao=contratacao_data.get("dataAtualizacao"),
            data_atualizacao_global=contratacao_data.get("dataAtualizacaoGlobal"),
            data_abertura_proposta=contratacao_data.get("dataAberturaProposta"),
            data_encerramento_proposta=contratacao_data.get("dataEncerramentoProposta"),
            # Orgão/Entidade
            orgao_cnpj=orgao_entidade.get("cnpj"),
            orgao_razao_social=orgao_entidade.get("razaoSocial"),
            orgao_poder_id=orgao_entidade.get("poderId"),
            orgao_esfera_id=orgao_entidade.get("esferaId"),
            # Unidade
            unidade_codigo=unidade_orgao.get("codigoUnidade"),
            unidade_nome=unidade_orgao.get("nomeUnidade"),
            unidade_uf_sigla=unidade_orgao.get("ufSigla"),
            unidade_uf_nome=unidade_orgao.get("ufNome"),
            unidade_municipio_nome=unidade_orgao.get("municipioNome"),
            unidade_codigo_ibge=unidade_orgao.get("codigoIbge"),
            # Subrogação
            orgao_subrogado_cnpj=orgao_subrogado.get("cnpj"),
            orgao_subrogado_razao_social=orgao_subrogado.get("razaoSocial"),
            unidade_subrogada_codigo=unidade_subrogada.get("codigoUnidade"),
            unidade_subrogada_nome=unidade_subrogada.get("nomeUnidade"),
            # Modalidade e Disputa
            modalidade_id=contratacao_data.get("modalidadeId"),
            modalidade_nome=contratacao_data.get("modalidadeNome"),
            modo_disputa_id=contratacao_data.get("modoDisputaId"),
            modo_disputa_nome=contratacao_data.get("modoDisputaNome"),
            # Instrumento Convocatório
            tipo_instrumento_convocatorio_codigo=contratacao_data.get(
                "tipoInstrumentoConvocatorioCodigo"
            ),
            tipo_instrumento_convocatorio_nome=contratacao_data.get(
                "tipoInstrumentoConvocatorioNome"
            ),
            # Amparo Legal
            amparo_legal_codigo=amparo_legal.get("codigo"),
            amparo_legal_nome=amparo_legal.get("nome"),
            amparo_legal_descricao=amparo_legal.get("descricao"),
            # Valores
            valor_total_estimado=contratacao_data.get("valorTotalEstimado"),
            valor_total_homologado=contratacao_data.get("valorTotalHomologado"),
            # Situação
            situacao_compra_id=contratacao_data.get("situacaoCompraId"),
            situacao_compra_nome=contratacao_data.get("situacaoCompraNome"),
            # Flags
            srp=contratacao_data.get("srp"),
            # Texto
            objeto_compra=contratacao_data.get("objetoCompra"),
            informacao_complementar=contratacao_data.get("informacaoComplementar"),
            justificativa_presencial=contratacao_data.get("justificativaPresencial"),
            # Links
            link_sistema_origem=contratacao_data.get("linkSistemaOrigem"),
            link_processo_eletronico=contratacao_data.get("linkProcessoEletronico"),
            # Metadados
            usuario_nome=contratacao_data.get("usuarioNome"),
        )


class BronzeFonteOrcamentaria(BaseModel):
    """Direct mapping to bronze_fontes_orcamentarias table."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True)

    # Relacionamento
    contratacao_numero_controle_pncp: str

    # Dados da Fonte
    codigo: int
    nome: str
    descricao: Optional[str] = None
    data_inclusao: Optional[datetime] = None

    # Metadados
    extracted_at: datetime = Field(default_factory=datetime.now)

    @field_validator("codigo")
    @classmethod
    def validate_codigo_positivo(cls, v: int) -> int:
        """Valida que código da fonte orçamentária é positivo."""
        if v <= 0:
            raise ValueError(f"Código da fonte orçamentária deve ser positivo: {v}")
        return v

    @field_validator("nome", mode="before")
    @classmethod
    def sanitize_nome(cls, v: str) -> str:
        """Sanitiza nome da fonte orçamentária."""
        if not v or not v.strip():
            raise ValueError("Nome da fonte orçamentária é obrigatório")
        return v.strip()

    @classmethod
    def from_api_response(
        cls, fonte_data: dict, numero_controle_pncp: str
    ) -> "BronzeFonteOrcamentaria":
        """Parse ContratacaoFonteOrcamentariaDTO from API to BronzeFonteOrcamentaria."""
        return cls(
            contratacao_numero_controle_pncp=numero_controle_pncp,
            codigo=fonte_data["codigo"],
            nome=fonte_data["nome"],
            descricao=fonte_data.get("descricao"),
            data_inclusao=fonte_data.get("dataInclusao"),
        )


class BronzeAta(BaseModel):
    """Direct mapping to bronze_atas table."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True)

    # Identificadores
    numero_controle_pncp_ata: str
    numero_ata_registro_preco: Optional[str] = None
    ano_ata: Optional[int] = None
    numero_controle_pncp_compra: Optional[str] = None  # FK para contratação

    # Controle
    cancelado: Optional[bool] = None
    data_cancelamento: Optional[datetime] = None

    # Datas
    data_assinatura: Optional[datetime] = None
    vigencia_inicio: Optional[datetime] = None
    vigencia_fim: Optional[datetime] = None
    data_publicacao_pncp: Optional[datetime] = None
    data_inclusao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    data_atualizacao_global: Optional[datetime] = None

    # Orgão Principal
    cnpj_orgao: Optional[str] = Field(None, max_length=14)
    nome_orgao: Optional[str] = None
    codigo_unidade_orgao: Optional[str] = None
    nome_unidade_orgao: Optional[str] = None

    # Orgão Subrogado
    cnpj_orgao_subrogado: Optional[str] = Field(None, max_length=14)
    nome_orgao_subrogado: Optional[str] = None
    codigo_unidade_orgao_subrogado: Optional[str] = None
    nome_unidade_orgao_subrogado: Optional[str] = None

    # Objeto
    objeto_contratacao: Optional[str] = None

    # Metadados
    usuario: Optional[str] = None
    extracted_at: datetime = Field(default_factory=datetime.now)

    @field_validator("cnpj_orgao", "cnpj_orgao_subrogado", mode="before")
    @classmethod
    def validate_cnpj(cls, v: Optional[str]) -> Optional[str]:
        """Valida formato e dígitos verificadores de CNPJ."""
        if v is None or v == "":
            return None
        digits_only = re.sub(r"[^\d]", "", str(v))
        if len(digits_only) != 14:
            raise ValueError(f"CNPJ deve ter 14 dígitos: {v}")
        if not cls._validate_cnpj_digits(digits_only):
            raise ValueError(f"CNPJ inválido: {v}")
        return digits_only

    @model_validator(mode="after")
    def validate_date_consistency(self) -> "BronzeAta":
        """Valida consistência entre datas."""
        # Vigência: início deve ser anterior ao fim
        if (
            self.vigencia_inicio
            and self.vigencia_fim
            and self.vigencia_inicio > self.vigencia_fim
        ):
            raise ValueError(
                "Data de início da vigência não pode ser posterior à data de fim"
            )

        # Data de assinatura deve ser anterior ao início da vigência
        if (
            self.data_assinatura
            and self.vigencia_inicio
            and self.data_assinatura > self.vigencia_inicio
        ):
            raise ValueError(
                "Data de assinatura deve ser anterior ao início da vigência"
            )

        # Se cancelado, deve ter data de cancelamento
        if self.cancelado and not self.data_cancelamento:
            raise ValueError("Ata cancelada deve ter data de cancelamento")

        # Data de cancelamento deve ser posterior à assinatura
        if (
            self.data_cancelamento
            and self.data_assinatura
            and self.data_cancelamento < self.data_assinatura
        ):
            raise ValueError(
                "Data de cancelamento deve ser posterior à data de assinatura"
            )

        return self

    @classmethod
    def _validate_cnpj_digits(cls, cnpj: str) -> bool:
        """Valida dígitos verificadores do CNPJ."""
        if len(cnpj) != 14:
            return False
        if cnpj == cnpj[0] * 14:
            return False
        # Primeiro dígito verificador
        weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum1 = sum(int(cnpj[i]) * weights[i] for i in range(12))
        remainder1 = sum1 % 11
        digit1 = 0 if remainder1 < 2 else 11 - remainder1
        if int(cnpj[12]) != digit1:
            return False
        # Segundo dígito verificador
        weights = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum2 = sum(int(cnpj[i]) * weights[i] for i in range(13))
        remainder2 = sum2 % 11
        digit2 = 0 if remainder2 < 2 else 11 - remainder2
        return int(cnpj[13]) == digit2

    @classmethod
    def from_api_response(cls, ata_data: dict) -> "BronzeAta":
        """Parse AtaRegistroPrecoPeriodoDTO from API to BronzeAta."""
        return cls(
            # Identificadores
            numero_controle_pncp_ata=ata_data["numeroControlePNCPAta"],
            numero_ata_registro_preco=ata_data.get("numeroAtaRegistroPreco"),
            ano_ata=ata_data.get("anoAta"),
            numero_controle_pncp_compra=ata_data.get("numeroControlePNCPCompra"),
            # Controle
            cancelado=ata_data.get("cancelado"),
            data_cancelamento=ata_data.get("dataCancelamento"),
            # Datas
            data_assinatura=ata_data.get("dataAssinatura"),
            vigencia_inicio=ata_data.get("vigenciaInicio"),
            vigencia_fim=ata_data.get("vigenciaFim"),
            data_publicacao_pncp=ata_data.get("dataPublicacaoPncp"),
            data_inclusao=ata_data.get("dataInclusao"),
            data_atualizacao=ata_data.get("dataAtualizacao"),
            data_atualizacao_global=ata_data.get("dataAtualizacaoGlobal"),
            # Orgão Principal
            cnpj_orgao=ata_data.get("cnpjOrgao"),
            nome_orgao=ata_data.get("nomeOrgao"),
            codigo_unidade_orgao=ata_data.get("codigoUnidadeOrgao"),
            nome_unidade_orgao=ata_data.get("nomeUnidadeOrgao"),
            # Orgão Subrogado
            cnpj_orgao_subrogado=ata_data.get("cnpjOrgaoSubrogado"),
            nome_orgao_subrogado=ata_data.get("nomeOrgaoSubrogado"),
            codigo_unidade_orgao_subrogado=ata_data.get("codigoUnidadeOrgaoSubrogado"),
            nome_unidade_orgao_subrogado=ata_data.get("nomeUnidadeOrgaoSubrogado"),
            # Objeto
            objeto_contratacao=ata_data.get("objetoContratacao"),
            # Metadados
            usuario=ata_data.get("usuario"),
        )


class BronzeRequest(BaseModel):
    """Direct mapping to bronze_pncp_requests table."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True)

    # Identificação da Requisição
    endpoint_name: str
    endpoint_url: str

    # Parâmetros da Requisição
    month: Optional[str] = Field(None, max_length=7)  # YYYY-MM format
    request_parameters: Optional[dict] = None

    # Metadados da Resposta
    response_code: int
    response_headers: Optional[dict] = None
    total_records: Optional[int] = None
    total_pages: Optional[int] = None
    current_page: Optional[int] = None
    page_size: Optional[int] = None

    # Controle de Execução
    run_id: Optional[str] = None
    data_date: Optional[date] = None

    # Status de Processamento
    parse_status: str = "pending"  # pending, success, failed
    parse_error_message: Optional[str] = None
    records_parsed: int = 0

    # Timestamps
    extracted_at: datetime = Field(default_factory=datetime.now)
    parsed_at: Optional[datetime] = None


class ParseError(BaseModel):
    """Direct mapping to pncp_parse_errors table."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True)

    error_id: str = Field(default_factory=lambda: str(uuid4()))  # UUID para o erro

    # Request Info
    endpoint_name: str
    endpoint_url: str
    http_status_code: Optional[int] = None

    # Response Data
    raw_data_sample: Optional[dict] = (
        None  # Amostra do JSON bruto que falhou no parsing
    )
    response_headers: Optional[dict] = None

    # Error Info
    error_message: str
    error_type: Optional[str] = None  # parse_error, validation_error, etc.
    field_name: Optional[str] = None  # Campo que causou o erro (se aplicável)
    record_identifier: Optional[str] = (
        None  # Identificador do registro (e.g., numeroControlePNCP)
    )
    stack_trace: Optional[str] = None

    # Retry Control
    retry_count: int = 0
    max_retries: int = 3
    next_retry_at: Optional[datetime] = None

    # Timestamps
    extracted_at: datetime = Field(default_factory=datetime.now)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
