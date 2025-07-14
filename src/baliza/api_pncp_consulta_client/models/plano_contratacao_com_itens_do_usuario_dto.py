import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.plano_contratacao_item_dto import PlanoContratacaoItemDTO


T = TypeVar("T", bound="PlanoContratacaoComItensDoUsuarioDTO")


@_attrs_define
class PlanoContratacaoComItensDoUsuarioDTO:
    """
    Attributes:
        itens (Union[Unset, list['PlanoContratacaoItemDTO']]):
        codigo_unidade (Union[Unset, str]):
        nome_unidade (Union[Unset, str]):
        ano_pca (Union[Unset, int]):
        data_publicacao_pncp (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_atualizacao_global_pca (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        id_pca_pncp (Union[Unset, str]):
        orgao_entidade_cnpj (Union[Unset, str]):
        orgao_entidade_razao_social (Union[Unset, str]):
    """

    itens: Union[Unset, list["PlanoContratacaoItemDTO"]] = UNSET
    codigo_unidade: Union[Unset, str] = UNSET
    nome_unidade: Union[Unset, str] = UNSET
    ano_pca: Union[Unset, int] = UNSET
    data_publicacao_pncp: Union[Unset, datetime.datetime] = UNSET
    data_atualizacao_global_pca: Union[Unset, datetime.datetime] = UNSET
    id_pca_pncp: Union[Unset, str] = UNSET
    orgao_entidade_cnpj: Union[Unset, str] = UNSET
    orgao_entidade_razao_social: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        itens: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.itens, Unset):
            itens = []
            for itens_item_data in self.itens:
                itens_item = itens_item_data.to_dict()
                itens.append(itens_item)

        codigo_unidade = self.codigo_unidade

        nome_unidade = self.nome_unidade

        ano_pca = self.ano_pca

        data_publicacao_pncp: Union[Unset, str] = UNSET
        if not isinstance(self.data_publicacao_pncp, Unset):
            data_publicacao_pncp = self.data_publicacao_pncp.isoformat()

        data_atualizacao_global_pca: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao_global_pca, Unset):
            data_atualizacao_global_pca = self.data_atualizacao_global_pca.isoformat()

        id_pca_pncp = self.id_pca_pncp

        orgao_entidade_cnpj = self.orgao_entidade_cnpj

        orgao_entidade_razao_social = self.orgao_entidade_razao_social

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if itens is not UNSET:
            field_dict["itens"] = itens
        if codigo_unidade is not UNSET:
            field_dict["codigoUnidade"] = codigo_unidade
        if nome_unidade is not UNSET:
            field_dict["nomeUnidade"] = nome_unidade
        if ano_pca is not UNSET:
            field_dict["anoPca"] = ano_pca
        if data_publicacao_pncp is not UNSET:
            field_dict["dataPublicacaoPNCP"] = data_publicacao_pncp
        if data_atualizacao_global_pca is not UNSET:
            field_dict["dataAtualizacaoGlobalPCA"] = data_atualizacao_global_pca
        if id_pca_pncp is not UNSET:
            field_dict["idPcaPncp"] = id_pca_pncp
        if orgao_entidade_cnpj is not UNSET:
            field_dict["orgaoEntidadeCnpj"] = orgao_entidade_cnpj
        if orgao_entidade_razao_social is not UNSET:
            field_dict["orgaoEntidadeRazaoSocial"] = orgao_entidade_razao_social

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.plano_contratacao_item_dto import PlanoContratacaoItemDTO

        d = dict(src_dict)
        itens = []
        _itens = d.pop("itens", UNSET)
        for itens_item_data in _itens or []:
            itens_item = PlanoContratacaoItemDTO.from_dict(itens_item_data)

            itens.append(itens_item)

        codigo_unidade = d.pop("codigoUnidade", UNSET)

        nome_unidade = d.pop("nomeUnidade", UNSET)

        ano_pca = d.pop("anoPca", UNSET)

        _data_publicacao_pncp = d.pop("dataPublicacaoPNCP", UNSET)
        data_publicacao_pncp: Union[Unset, datetime.datetime]
        if isinstance(_data_publicacao_pncp, Unset):
            data_publicacao_pncp = UNSET
        else:
            data_publicacao_pncp = isoparse(_data_publicacao_pncp)

        _data_atualizacao_global_pca = d.pop("dataAtualizacaoGlobalPCA", UNSET)
        data_atualizacao_global_pca: Union[Unset, datetime.datetime]
        if isinstance(_data_atualizacao_global_pca, Unset):
            data_atualizacao_global_pca = UNSET
        else:
            data_atualizacao_global_pca = isoparse(_data_atualizacao_global_pca)

        id_pca_pncp = d.pop("idPcaPncp", UNSET)

        orgao_entidade_cnpj = d.pop("orgaoEntidadeCnpj", UNSET)

        orgao_entidade_razao_social = d.pop("orgaoEntidadeRazaoSocial", UNSET)

        plano_contratacao_com_itens_do_usuario_dto = cls(
            itens=itens,
            codigo_unidade=codigo_unidade,
            nome_unidade=nome_unidade,
            ano_pca=ano_pca,
            data_publicacao_pncp=data_publicacao_pncp,
            data_atualizacao_global_pca=data_atualizacao_global_pca,
            id_pca_pncp=id_pca_pncp,
            orgao_entidade_cnpj=orgao_entidade_cnpj,
            orgao_entidade_razao_social=orgao_entidade_razao_social,
        )

        plano_contratacao_com_itens_do_usuario_dto.additional_properties = d
        return plano_contratacao_com_itens_do_usuario_dto

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
