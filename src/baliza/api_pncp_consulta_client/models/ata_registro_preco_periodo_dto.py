import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="AtaRegistroPrecoPeriodoDTO")


@_attrs_define
class AtaRegistroPrecoPeriodoDTO:
    """
    Attributes:
        numero_controle_pncp_ata (Union[Unset, str]):
        numero_ata_registro_preco (Union[Unset, str]):
        ano_ata (Union[Unset, int]):
        numero_controle_pncp_compra (Union[Unset, str]):
        cancelado (Union[Unset, bool]):
        data_cancelamento (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_assinatura (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        vigencia_inicio (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        vigencia_fim (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_publicacao_pncp (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_inclusao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_atualizacao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_atualizacao_global (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        usuario (Union[Unset, str]):
        objeto_contratacao (Union[Unset, str]):
        cnpj_orgao (Union[Unset, str]):
        nome_orgao (Union[Unset, str]):
        cnpj_orgao_subrogado (Union[Unset, str]):
        nome_orgao_subrogado (Union[Unset, str]):
        codigo_unidade_orgao (Union[Unset, str]):
        nome_unidade_orgao (Union[Unset, str]):
        codigo_unidade_orgao_subrogado (Union[Unset, str]):
        nome_unidade_orgao_subrogado (Union[Unset, str]):
    """

    numero_controle_pncp_ata: Union[Unset, str] = UNSET
    numero_ata_registro_preco: Union[Unset, str] = UNSET
    ano_ata: Union[Unset, int] = UNSET
    numero_controle_pncp_compra: Union[Unset, str] = UNSET
    cancelado: Union[Unset, bool] = UNSET
    data_cancelamento: Union[Unset, datetime.datetime] = UNSET
    data_assinatura: Union[Unset, datetime.datetime] = UNSET
    vigencia_inicio: Union[Unset, datetime.datetime] = UNSET
    vigencia_fim: Union[Unset, datetime.datetime] = UNSET
    data_publicacao_pncp: Union[Unset, datetime.datetime] = UNSET
    data_inclusao: Union[Unset, datetime.datetime] = UNSET
    data_atualizacao: Union[Unset, datetime.datetime] = UNSET
    data_atualizacao_global: Union[Unset, datetime.datetime] = UNSET
    usuario: Union[Unset, str] = UNSET
    objeto_contratacao: Union[Unset, str] = UNSET
    cnpj_orgao: Union[Unset, str] = UNSET
    nome_orgao: Union[Unset, str] = UNSET
    cnpj_orgao_subrogado: Union[Unset, str] = UNSET
    nome_orgao_subrogado: Union[Unset, str] = UNSET
    codigo_unidade_orgao: Union[Unset, str] = UNSET
    nome_unidade_orgao: Union[Unset, str] = UNSET
    codigo_unidade_orgao_subrogado: Union[Unset, str] = UNSET
    nome_unidade_orgao_subrogado: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        numero_controle_pncp_ata = self.numero_controle_pncp_ata

        numero_ata_registro_preco = self.numero_ata_registro_preco

        ano_ata = self.ano_ata

        numero_controle_pncp_compra = self.numero_controle_pncp_compra

        cancelado = self.cancelado

        data_cancelamento: Union[Unset, str] = UNSET
        if not isinstance(self.data_cancelamento, Unset):
            data_cancelamento = self.data_cancelamento.isoformat()

        data_assinatura: Union[Unset, str] = UNSET
        if not isinstance(self.data_assinatura, Unset):
            data_assinatura = self.data_assinatura.isoformat()

        vigencia_inicio: Union[Unset, str] = UNSET
        if not isinstance(self.vigencia_inicio, Unset):
            vigencia_inicio = self.vigencia_inicio.isoformat()

        vigencia_fim: Union[Unset, str] = UNSET
        if not isinstance(self.vigencia_fim, Unset):
            vigencia_fim = self.vigencia_fim.isoformat()

        data_publicacao_pncp: Union[Unset, str] = UNSET
        if not isinstance(self.data_publicacao_pncp, Unset):
            data_publicacao_pncp = self.data_publicacao_pncp.isoformat()

        data_inclusao: Union[Unset, str] = UNSET
        if not isinstance(self.data_inclusao, Unset):
            data_inclusao = self.data_inclusao.isoformat()

        data_atualizacao: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao, Unset):
            data_atualizacao = self.data_atualizacao.isoformat()

        data_atualizacao_global: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao_global, Unset):
            data_atualizacao_global = self.data_atualizacao_global.isoformat()

        usuario = self.usuario

        objeto_contratacao = self.objeto_contratacao

        cnpj_orgao = self.cnpj_orgao

        nome_orgao = self.nome_orgao

        cnpj_orgao_subrogado = self.cnpj_orgao_subrogado

        nome_orgao_subrogado = self.nome_orgao_subrogado

        codigo_unidade_orgao = self.codigo_unidade_orgao

        nome_unidade_orgao = self.nome_unidade_orgao

        codigo_unidade_orgao_subrogado = self.codigo_unidade_orgao_subrogado

        nome_unidade_orgao_subrogado = self.nome_unidade_orgao_subrogado

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if numero_controle_pncp_ata is not UNSET:
            field_dict["numeroControlePNCPAta"] = numero_controle_pncp_ata
        if numero_ata_registro_preco is not UNSET:
            field_dict["numeroAtaRegistroPreco"] = numero_ata_registro_preco
        if ano_ata is not UNSET:
            field_dict["anoAta"] = ano_ata
        if numero_controle_pncp_compra is not UNSET:
            field_dict["numeroControlePNCPCompra"] = numero_controle_pncp_compra
        if cancelado is not UNSET:
            field_dict["cancelado"] = cancelado
        if data_cancelamento is not UNSET:
            field_dict["dataCancelamento"] = data_cancelamento
        if data_assinatura is not UNSET:
            field_dict["dataAssinatura"] = data_assinatura
        if vigencia_inicio is not UNSET:
            field_dict["vigenciaInicio"] = vigencia_inicio
        if vigencia_fim is not UNSET:
            field_dict["vigenciaFim"] = vigencia_fim
        if data_publicacao_pncp is not UNSET:
            field_dict["dataPublicacaoPncp"] = data_publicacao_pncp
        if data_inclusao is not UNSET:
            field_dict["dataInclusao"] = data_inclusao
        if data_atualizacao is not UNSET:
            field_dict["dataAtualizacao"] = data_atualizacao
        if data_atualizacao_global is not UNSET:
            field_dict["dataAtualizacaoGlobal"] = data_atualizacao_global
        if usuario is not UNSET:
            field_dict["usuario"] = usuario
        if objeto_contratacao is not UNSET:
            field_dict["objetoContratacao"] = objeto_contratacao
        if cnpj_orgao is not UNSET:
            field_dict["cnpjOrgao"] = cnpj_orgao
        if nome_orgao is not UNSET:
            field_dict["nomeOrgao"] = nome_orgao
        if cnpj_orgao_subrogado is not UNSET:
            field_dict["cnpjOrgaoSubrogado"] = cnpj_orgao_subrogado
        if nome_orgao_subrogado is not UNSET:
            field_dict["nomeOrgaoSubrogado"] = nome_orgao_subrogado
        if codigo_unidade_orgao is not UNSET:
            field_dict["codigoUnidadeOrgao"] = codigo_unidade_orgao
        if nome_unidade_orgao is not UNSET:
            field_dict["nomeUnidadeOrgao"] = nome_unidade_orgao
        if codigo_unidade_orgao_subrogado is not UNSET:
            field_dict["codigoUnidadeOrgaoSubrogado"] = codigo_unidade_orgao_subrogado
        if nome_unidade_orgao_subrogado is not UNSET:
            field_dict["nomeUnidadeOrgaoSubrogado"] = nome_unidade_orgao_subrogado

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        numero_controle_pncp_ata = d.pop("numeroControlePNCPAta", UNSET)

        numero_ata_registro_preco = d.pop("numeroAtaRegistroPreco", UNSET)

        ano_ata = d.pop("anoAta", UNSET)

        numero_controle_pncp_compra = d.pop("numeroControlePNCPCompra", UNSET)

        cancelado = d.pop("cancelado", UNSET)

        _data_cancelamento = d.pop("dataCancelamento", UNSET)
        data_cancelamento: Union[Unset, datetime.datetime]
        if isinstance(_data_cancelamento, Unset):
            data_cancelamento = UNSET
        else:
            data_cancelamento = isoparse(_data_cancelamento)

        _data_assinatura = d.pop("dataAssinatura", UNSET)
        data_assinatura: Union[Unset, datetime.datetime]
        if isinstance(_data_assinatura, Unset):
            data_assinatura = UNSET
        else:
            data_assinatura = isoparse(_data_assinatura)

        _vigencia_inicio = d.pop("vigenciaInicio", UNSET)
        vigencia_inicio: Union[Unset, datetime.datetime]
        if isinstance(_vigencia_inicio, Unset):
            vigencia_inicio = UNSET
        else:
            vigencia_inicio = isoparse(_vigencia_inicio)

        _vigencia_fim = d.pop("vigenciaFim", UNSET)
        vigencia_fim: Union[Unset, datetime.datetime]
        if isinstance(_vigencia_fim, Unset):
            vigencia_fim = UNSET
        else:
            vigencia_fim = isoparse(_vigencia_fim)

        _data_publicacao_pncp = d.pop("dataPublicacaoPncp", UNSET)
        data_publicacao_pncp: Union[Unset, datetime.datetime]
        if isinstance(_data_publicacao_pncp, Unset):
            data_publicacao_pncp = UNSET
        else:
            data_publicacao_pncp = isoparse(_data_publicacao_pncp)

        _data_inclusao = d.pop("dataInclusao", UNSET)
        data_inclusao: Union[Unset, datetime.datetime]
        if isinstance(_data_inclusao, Unset):
            data_inclusao = UNSET
        else:
            data_inclusao = isoparse(_data_inclusao)

        _data_atualizacao = d.pop("dataAtualizacao", UNSET)
        data_atualizacao: Union[Unset, datetime.datetime]
        if isinstance(_data_atualizacao, Unset):
            data_atualizacao = UNSET
        else:
            data_atualizacao = isoparse(_data_atualizacao)

        _data_atualizacao_global = d.pop("dataAtualizacaoGlobal", UNSET)
        data_atualizacao_global: Union[Unset, datetime.datetime]
        if isinstance(_data_atualizacao_global, Unset):
            data_atualizacao_global = UNSET
        else:
            data_atualizacao_global = isoparse(_data_atualizacao_global)

        usuario = d.pop("usuario", UNSET)

        objeto_contratacao = d.pop("objetoContratacao", UNSET)

        cnpj_orgao = d.pop("cnpjOrgao", UNSET)

        nome_orgao = d.pop("nomeOrgao", UNSET)

        cnpj_orgao_subrogado = d.pop("cnpjOrgaoSubrogado", UNSET)

        nome_orgao_subrogado = d.pop("nomeOrgaoSubrogado", UNSET)

        codigo_unidade_orgao = d.pop("codigoUnidadeOrgao", UNSET)

        nome_unidade_orgao = d.pop("nomeUnidadeOrgao", UNSET)

        codigo_unidade_orgao_subrogado = d.pop("codigoUnidadeOrgaoSubrogado", UNSET)

        nome_unidade_orgao_subrogado = d.pop("nomeUnidadeOrgaoSubrogado", UNSET)

        ata_registro_preco_periodo_dto = cls(
            numero_controle_pncp_ata=numero_controle_pncp_ata,
            numero_ata_registro_preco=numero_ata_registro_preco,
            ano_ata=ano_ata,
            numero_controle_pncp_compra=numero_controle_pncp_compra,
            cancelado=cancelado,
            data_cancelamento=data_cancelamento,
            data_assinatura=data_assinatura,
            vigencia_inicio=vigencia_inicio,
            vigencia_fim=vigencia_fim,
            data_publicacao_pncp=data_publicacao_pncp,
            data_inclusao=data_inclusao,
            data_atualizacao=data_atualizacao,
            data_atualizacao_global=data_atualizacao_global,
            usuario=usuario,
            objeto_contratacao=objeto_contratacao,
            cnpj_orgao=cnpj_orgao,
            nome_orgao=nome_orgao,
            cnpj_orgao_subrogado=cnpj_orgao_subrogado,
            nome_orgao_subrogado=nome_orgao_subrogado,
            codigo_unidade_orgao=codigo_unidade_orgao,
            nome_unidade_orgao=nome_unidade_orgao,
            codigo_unidade_orgao_subrogado=codigo_unidade_orgao_subrogado,
            nome_unidade_orgao_subrogado=nome_unidade_orgao_subrogado,
        )

        ata_registro_preco_periodo_dto.additional_properties = d
        return ata_registro_preco_periodo_dto

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
