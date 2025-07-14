import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.recuperar_contrato_dto_tipo_pessoa import RecuperarContratoDTOTipoPessoa
from ..models.recuperar_contrato_dto_tipo_pessoa_sub_contratada import RecuperarContratoDTOTipoPessoaSubContratada
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.categoria import Categoria
    from ..models.recuperar_orgao_entidade_dto import RecuperarOrgaoEntidadeDTO
    from ..models.recuperar_unidade_orgao_dto import RecuperarUnidadeOrgaoDTO
    from ..models.tipo_contrato import TipoContrato


T = TypeVar("T", bound="RecuperarContratoDTO")


@_attrs_define
class RecuperarContratoDTO:
    """
    Attributes:
        numero_controle_pncp_compra (Union[Unset, str]):
        codigo_pais_fornecedor (Union[Unset, str]):
        informacao_complementar (Union[Unset, str]):
        processo (Union[Unset, str]):
        unidade_sub_rogada (Union[Unset, RecuperarUnidadeOrgaoDTO]):
        orgao_sub_rogado (Union[Unset, RecuperarOrgaoEntidadeDTO]):
        ano_contrato (Union[Unset, int]):
        tipo_contrato (Union[Unset, TipoContrato]):
        numero_contrato_empenho (Union[Unset, str]):
        data_assinatura (Union[Unset, datetime.date]):
        data_vigencia_inicio (Union[Unset, datetime.date]):
        data_vigencia_fim (Union[Unset, datetime.date]):
        ni_fornecedor (Union[Unset, str]):
        tipo_pessoa (Union[Unset, RecuperarContratoDTOTipoPessoa]):
        orgao_entidade (Union[Unset, RecuperarOrgaoEntidadeDTO]):
        categoria_processo (Union[Unset, Categoria]):
        data_publicacao_pncp (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_atualizacao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        sequencial_contrato (Union[Unset, int]):
        nome_razao_social_fornecedor (Union[Unset, str]):
        unidade_orgao (Union[Unset, RecuperarUnidadeOrgaoDTO]):
        ni_fornecedor_sub_contratado (Union[Unset, str]):
        nome_fornecedor_sub_contratado (Union[Unset, str]):
        receita (Union[Unset, bool]):
        numero_parcelas (Union[Unset, int]):
        numero_retificacao (Union[Unset, int]):
        tipo_pessoa_sub_contratada (Union[Unset, RecuperarContratoDTOTipoPessoaSubContratada]):
        objeto_contrato (Union[Unset, str]):
        valor_inicial (Union[Unset, float]):
        valor_parcela (Union[Unset, float]):
        valor_global (Union[Unset, float]):
        valor_acumulado (Union[Unset, float]):
        data_atualizacao_global (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        identificador_cipi (Union[Unset, str]):
        url_cipi (Union[Unset, str]):
        numero_controle_pncp (Union[Unset, str]):
        usuario_nome (Union[Unset, str]):
    """

    numero_controle_pncp_compra: Union[Unset, str] = UNSET
    codigo_pais_fornecedor: Union[Unset, str] = UNSET
    informacao_complementar: Union[Unset, str] = UNSET
    processo: Union[Unset, str] = UNSET
    unidade_sub_rogada: Union[Unset, "RecuperarUnidadeOrgaoDTO"] = UNSET
    orgao_sub_rogado: Union[Unset, "RecuperarOrgaoEntidadeDTO"] = UNSET
    ano_contrato: Union[Unset, int] = UNSET
    tipo_contrato: Union[Unset, "TipoContrato"] = UNSET
    numero_contrato_empenho: Union[Unset, str] = UNSET
    data_assinatura: Union[Unset, datetime.date] = UNSET
    data_vigencia_inicio: Union[Unset, datetime.date] = UNSET
    data_vigencia_fim: Union[Unset, datetime.date] = UNSET
    ni_fornecedor: Union[Unset, str] = UNSET
    tipo_pessoa: Union[Unset, RecuperarContratoDTOTipoPessoa] = UNSET
    orgao_entidade: Union[Unset, "RecuperarOrgaoEntidadeDTO"] = UNSET
    categoria_processo: Union[Unset, "Categoria"] = UNSET
    data_publicacao_pncp: Union[Unset, datetime.datetime] = UNSET
    data_atualizacao: Union[Unset, datetime.datetime] = UNSET
    sequencial_contrato: Union[Unset, int] = UNSET
    nome_razao_social_fornecedor: Union[Unset, str] = UNSET
    unidade_orgao: Union[Unset, "RecuperarUnidadeOrgaoDTO"] = UNSET
    ni_fornecedor_sub_contratado: Union[Unset, str] = UNSET
    nome_fornecedor_sub_contratado: Union[Unset, str] = UNSET
    receita: Union[Unset, bool] = UNSET
    numero_parcelas: Union[Unset, int] = UNSET
    numero_retificacao: Union[Unset, int] = UNSET
    tipo_pessoa_sub_contratada: Union[Unset, RecuperarContratoDTOTipoPessoaSubContratada] = UNSET
    objeto_contrato: Union[Unset, str] = UNSET
    valor_inicial: Union[Unset, float] = UNSET
    valor_parcela: Union[Unset, float] = UNSET
    valor_global: Union[Unset, float] = UNSET
    valor_acumulado: Union[Unset, float] = UNSET
    data_atualizacao_global: Union[Unset, datetime.datetime] = UNSET
    identificador_cipi: Union[Unset, str] = UNSET
    url_cipi: Union[Unset, str] = UNSET
    numero_controle_pncp: Union[Unset, str] = UNSET
    usuario_nome: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        numero_controle_pncp_compra = self.numero_controle_pncp_compra

        codigo_pais_fornecedor = self.codigo_pais_fornecedor

        informacao_complementar = self.informacao_complementar

        processo = self.processo

        unidade_sub_rogada: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.unidade_sub_rogada, Unset):
            unidade_sub_rogada = self.unidade_sub_rogada.to_dict()

        orgao_sub_rogado: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.orgao_sub_rogado, Unset):
            orgao_sub_rogado = self.orgao_sub_rogado.to_dict()

        ano_contrato = self.ano_contrato

        tipo_contrato: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.tipo_contrato, Unset):
            tipo_contrato = self.tipo_contrato.to_dict()

        numero_contrato_empenho = self.numero_contrato_empenho

        data_assinatura: Union[Unset, str] = UNSET
        if not isinstance(self.data_assinatura, Unset):
            data_assinatura = self.data_assinatura.isoformat()

        data_vigencia_inicio: Union[Unset, str] = UNSET
        if not isinstance(self.data_vigencia_inicio, Unset):
            data_vigencia_inicio = self.data_vigencia_inicio.isoformat()

        data_vigencia_fim: Union[Unset, str] = UNSET
        if not isinstance(self.data_vigencia_fim, Unset):
            data_vigencia_fim = self.data_vigencia_fim.isoformat()

        ni_fornecedor = self.ni_fornecedor

        tipo_pessoa: Union[Unset, str] = UNSET
        if not isinstance(self.tipo_pessoa, Unset):
            tipo_pessoa = self.tipo_pessoa.value

        orgao_entidade: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.orgao_entidade, Unset):
            orgao_entidade = self.orgao_entidade.to_dict()

        categoria_processo: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.categoria_processo, Unset):
            categoria_processo = self.categoria_processo.to_dict()

        data_publicacao_pncp: Union[Unset, str] = UNSET
        if not isinstance(self.data_publicacao_pncp, Unset):
            data_publicacao_pncp = self.data_publicacao_pncp.isoformat()

        data_atualizacao: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao, Unset):
            data_atualizacao = self.data_atualizacao.isoformat()

        sequencial_contrato = self.sequencial_contrato

        nome_razao_social_fornecedor = self.nome_razao_social_fornecedor

        unidade_orgao: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.unidade_orgao, Unset):
            unidade_orgao = self.unidade_orgao.to_dict()

        ni_fornecedor_sub_contratado = self.ni_fornecedor_sub_contratado

        nome_fornecedor_sub_contratado = self.nome_fornecedor_sub_contratado

        receita = self.receita

        numero_parcelas = self.numero_parcelas

        numero_retificacao = self.numero_retificacao

        tipo_pessoa_sub_contratada: Union[Unset, str] = UNSET
        if not isinstance(self.tipo_pessoa_sub_contratada, Unset):
            tipo_pessoa_sub_contratada = self.tipo_pessoa_sub_contratada.value

        objeto_contrato = self.objeto_contrato

        valor_inicial = self.valor_inicial

        valor_parcela = self.valor_parcela

        valor_global = self.valor_global

        valor_acumulado = self.valor_acumulado

        data_atualizacao_global: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao_global, Unset):
            data_atualizacao_global = self.data_atualizacao_global.isoformat()

        identificador_cipi = self.identificador_cipi

        url_cipi = self.url_cipi

        numero_controle_pncp = self.numero_controle_pncp

        usuario_nome = self.usuario_nome

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if numero_controle_pncp_compra is not UNSET:
            field_dict["numeroControlePncpCompra"] = numero_controle_pncp_compra
        if codigo_pais_fornecedor is not UNSET:
            field_dict["codigoPaisFornecedor"] = codigo_pais_fornecedor
        if informacao_complementar is not UNSET:
            field_dict["informacaoComplementar"] = informacao_complementar
        if processo is not UNSET:
            field_dict["processo"] = processo
        if unidade_sub_rogada is not UNSET:
            field_dict["unidadeSubRogada"] = unidade_sub_rogada
        if orgao_sub_rogado is not UNSET:
            field_dict["orgaoSubRogado"] = orgao_sub_rogado
        if ano_contrato is not UNSET:
            field_dict["anoContrato"] = ano_contrato
        if tipo_contrato is not UNSET:
            field_dict["tipoContrato"] = tipo_contrato
        if numero_contrato_empenho is not UNSET:
            field_dict["numeroContratoEmpenho"] = numero_contrato_empenho
        if data_assinatura is not UNSET:
            field_dict["dataAssinatura"] = data_assinatura
        if data_vigencia_inicio is not UNSET:
            field_dict["dataVigenciaInicio"] = data_vigencia_inicio
        if data_vigencia_fim is not UNSET:
            field_dict["dataVigenciaFim"] = data_vigencia_fim
        if ni_fornecedor is not UNSET:
            field_dict["niFornecedor"] = ni_fornecedor
        if tipo_pessoa is not UNSET:
            field_dict["tipoPessoa"] = tipo_pessoa
        if orgao_entidade is not UNSET:
            field_dict["orgaoEntidade"] = orgao_entidade
        if categoria_processo is not UNSET:
            field_dict["categoriaProcesso"] = categoria_processo
        if data_publicacao_pncp is not UNSET:
            field_dict["dataPublicacaoPncp"] = data_publicacao_pncp
        if data_atualizacao is not UNSET:
            field_dict["dataAtualizacao"] = data_atualizacao
        if sequencial_contrato is not UNSET:
            field_dict["sequencialContrato"] = sequencial_contrato
        if nome_razao_social_fornecedor is not UNSET:
            field_dict["nomeRazaoSocialFornecedor"] = nome_razao_social_fornecedor
        if unidade_orgao is not UNSET:
            field_dict["unidadeOrgao"] = unidade_orgao
        if ni_fornecedor_sub_contratado is not UNSET:
            field_dict["niFornecedorSubContratado"] = ni_fornecedor_sub_contratado
        if nome_fornecedor_sub_contratado is not UNSET:
            field_dict["nomeFornecedorSubContratado"] = nome_fornecedor_sub_contratado
        if receita is not UNSET:
            field_dict["receita"] = receita
        if numero_parcelas is not UNSET:
            field_dict["numeroParcelas"] = numero_parcelas
        if numero_retificacao is not UNSET:
            field_dict["numeroRetificacao"] = numero_retificacao
        if tipo_pessoa_sub_contratada is not UNSET:
            field_dict["tipoPessoaSubContratada"] = tipo_pessoa_sub_contratada
        if objeto_contrato is not UNSET:
            field_dict["objetoContrato"] = objeto_contrato
        if valor_inicial is not UNSET:
            field_dict["valorInicial"] = valor_inicial
        if valor_parcela is not UNSET:
            field_dict["valorParcela"] = valor_parcela
        if valor_global is not UNSET:
            field_dict["valorGlobal"] = valor_global
        if valor_acumulado is not UNSET:
            field_dict["valorAcumulado"] = valor_acumulado
        if data_atualizacao_global is not UNSET:
            field_dict["dataAtualizacaoGlobal"] = data_atualizacao_global
        if identificador_cipi is not UNSET:
            field_dict["identificadorCipi"] = identificador_cipi
        if url_cipi is not UNSET:
            field_dict["urlCipi"] = url_cipi
        if numero_controle_pncp is not UNSET:
            field_dict["numeroControlePNCP"] = numero_controle_pncp
        if usuario_nome is not UNSET:
            field_dict["usuarioNome"] = usuario_nome

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.categoria import Categoria
        from ..models.recuperar_orgao_entidade_dto import RecuperarOrgaoEntidadeDTO
        from ..models.recuperar_unidade_orgao_dto import RecuperarUnidadeOrgaoDTO
        from ..models.tipo_contrato import TipoContrato

        d = dict(src_dict)
        numero_controle_pncp_compra = d.pop("numeroControlePncpCompra", UNSET)

        codigo_pais_fornecedor = d.pop("codigoPaisFornecedor", UNSET)

        informacao_complementar = d.pop("informacaoComplementar", UNSET)

        processo = d.pop("processo", UNSET)

        _unidade_sub_rogada = d.pop("unidadeSubRogada", UNSET)
        unidade_sub_rogada: Union[Unset, RecuperarUnidadeOrgaoDTO]
        if isinstance(_unidade_sub_rogada, Unset):
            unidade_sub_rogada = UNSET
        else:
            unidade_sub_rogada = RecuperarUnidadeOrgaoDTO.from_dict(_unidade_sub_rogada)

        _orgao_sub_rogado = d.pop("orgaoSubRogado", UNSET)
        orgao_sub_rogado: Union[Unset, RecuperarOrgaoEntidadeDTO]
        if isinstance(_orgao_sub_rogado, Unset):
            orgao_sub_rogado = UNSET
        else:
            orgao_sub_rogado = RecuperarOrgaoEntidadeDTO.from_dict(_orgao_sub_rogado)

        ano_contrato = d.pop("anoContrato", UNSET)

        _tipo_contrato = d.pop("tipoContrato", UNSET)
        tipo_contrato: Union[Unset, TipoContrato]
        if isinstance(_tipo_contrato, Unset):
            tipo_contrato = UNSET
        else:
            tipo_contrato = TipoContrato.from_dict(_tipo_contrato)

        numero_contrato_empenho = d.pop("numeroContratoEmpenho", UNSET)

        _data_assinatura = d.pop("dataAssinatura", UNSET)
        data_assinatura: Union[Unset, datetime.date]
        if isinstance(_data_assinatura, Unset):
            data_assinatura = UNSET
        else:
            data_assinatura = isoparse(_data_assinatura).date()

        _data_vigencia_inicio = d.pop("dataVigenciaInicio", UNSET)
        data_vigencia_inicio: Union[Unset, datetime.date]
        if isinstance(_data_vigencia_inicio, Unset):
            data_vigencia_inicio = UNSET
        else:
            data_vigencia_inicio = isoparse(_data_vigencia_inicio).date()

        _data_vigencia_fim = d.pop("dataVigenciaFim", UNSET)
        data_vigencia_fim: Union[Unset, datetime.date]
        if isinstance(_data_vigencia_fim, Unset):
            data_vigencia_fim = UNSET
        else:
            data_vigencia_fim = isoparse(_data_vigencia_fim).date()

        ni_fornecedor = d.pop("niFornecedor", UNSET)

        _tipo_pessoa = d.pop("tipoPessoa", UNSET)
        tipo_pessoa: Union[Unset, RecuperarContratoDTOTipoPessoa]
        if isinstance(_tipo_pessoa, Unset):
            tipo_pessoa = UNSET
        else:
            tipo_pessoa = RecuperarContratoDTOTipoPessoa(_tipo_pessoa)

        _orgao_entidade = d.pop("orgaoEntidade", UNSET)
        orgao_entidade: Union[Unset, RecuperarOrgaoEntidadeDTO]
        if isinstance(_orgao_entidade, Unset):
            orgao_entidade = UNSET
        else:
            orgao_entidade = RecuperarOrgaoEntidadeDTO.from_dict(_orgao_entidade)

        _categoria_processo = d.pop("categoriaProcesso", UNSET)
        categoria_processo: Union[Unset, Categoria]
        if isinstance(_categoria_processo, Unset):
            categoria_processo = UNSET
        else:
            categoria_processo = Categoria.from_dict(_categoria_processo)

        _data_publicacao_pncp = d.pop("dataPublicacaoPncp", UNSET)
        data_publicacao_pncp: Union[Unset, datetime.datetime]
        if isinstance(_data_publicacao_pncp, Unset):
            data_publicacao_pncp = UNSET
        else:
            data_publicacao_pncp = isoparse(_data_publicacao_pncp)

        _data_atualizacao = d.pop("dataAtualizacao", UNSET)
        data_atualizacao: Union[Unset, datetime.datetime]
        if isinstance(_data_atualizacao, Unset):
            data_atualizacao = UNSET
        else:
            data_atualizacao = isoparse(_data_atualizacao)

        sequencial_contrato = d.pop("sequencialContrato", UNSET)

        nome_razao_social_fornecedor = d.pop("nomeRazaoSocialFornecedor", UNSET)

        _unidade_orgao = d.pop("unidadeOrgao", UNSET)
        unidade_orgao: Union[Unset, RecuperarUnidadeOrgaoDTO]
        if isinstance(_unidade_orgao, Unset):
            unidade_orgao = UNSET
        else:
            unidade_orgao = RecuperarUnidadeOrgaoDTO.from_dict(_unidade_orgao)

        ni_fornecedor_sub_contratado = d.pop("niFornecedorSubContratado", UNSET)

        nome_fornecedor_sub_contratado = d.pop("nomeFornecedorSubContratado", UNSET)

        receita = d.pop("receita", UNSET)

        numero_parcelas = d.pop("numeroParcelas", UNSET)

        numero_retificacao = d.pop("numeroRetificacao", UNSET)

        _tipo_pessoa_sub_contratada = d.pop("tipoPessoaSubContratada", UNSET)
        tipo_pessoa_sub_contratada: Union[Unset, RecuperarContratoDTOTipoPessoaSubContratada]
        if isinstance(_tipo_pessoa_sub_contratada, Unset):
            tipo_pessoa_sub_contratada = UNSET
        else:
            tipo_pessoa_sub_contratada = RecuperarContratoDTOTipoPessoaSubContratada(_tipo_pessoa_sub_contratada)

        objeto_contrato = d.pop("objetoContrato", UNSET)

        valor_inicial = d.pop("valorInicial", UNSET)

        valor_parcela = d.pop("valorParcela", UNSET)

        valor_global = d.pop("valorGlobal", UNSET)

        valor_acumulado = d.pop("valorAcumulado", UNSET)

        _data_atualizacao_global = d.pop("dataAtualizacaoGlobal", UNSET)
        data_atualizacao_global: Union[Unset, datetime.datetime]
        if isinstance(_data_atualizacao_global, Unset):
            data_atualizacao_global = UNSET
        else:
            data_atualizacao_global = isoparse(_data_atualizacao_global)

        identificador_cipi = d.pop("identificadorCipi", UNSET)

        url_cipi = d.pop("urlCipi", UNSET)

        numero_controle_pncp = d.pop("numeroControlePNCP", UNSET)

        usuario_nome = d.pop("usuarioNome", UNSET)

        recuperar_contrato_dto = cls(
            numero_controle_pncp_compra=numero_controle_pncp_compra,
            codigo_pais_fornecedor=codigo_pais_fornecedor,
            informacao_complementar=informacao_complementar,
            processo=processo,
            unidade_sub_rogada=unidade_sub_rogada,
            orgao_sub_rogado=orgao_sub_rogado,
            ano_contrato=ano_contrato,
            tipo_contrato=tipo_contrato,
            numero_contrato_empenho=numero_contrato_empenho,
            data_assinatura=data_assinatura,
            data_vigencia_inicio=data_vigencia_inicio,
            data_vigencia_fim=data_vigencia_fim,
            ni_fornecedor=ni_fornecedor,
            tipo_pessoa=tipo_pessoa,
            orgao_entidade=orgao_entidade,
            categoria_processo=categoria_processo,
            data_publicacao_pncp=data_publicacao_pncp,
            data_atualizacao=data_atualizacao,
            sequencial_contrato=sequencial_contrato,
            nome_razao_social_fornecedor=nome_razao_social_fornecedor,
            unidade_orgao=unidade_orgao,
            ni_fornecedor_sub_contratado=ni_fornecedor_sub_contratado,
            nome_fornecedor_sub_contratado=nome_fornecedor_sub_contratado,
            receita=receita,
            numero_parcelas=numero_parcelas,
            numero_retificacao=numero_retificacao,
            tipo_pessoa_sub_contratada=tipo_pessoa_sub_contratada,
            objeto_contrato=objeto_contrato,
            valor_inicial=valor_inicial,
            valor_parcela=valor_parcela,
            valor_global=valor_global,
            valor_acumulado=valor_acumulado,
            data_atualizacao_global=data_atualizacao_global,
            identificador_cipi=identificador_cipi,
            url_cipi=url_cipi,
            numero_controle_pncp=numero_controle_pncp,
            usuario_nome=usuario_nome,
        )

        recuperar_contrato_dto.additional_properties = d
        return recuperar_contrato_dto

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
