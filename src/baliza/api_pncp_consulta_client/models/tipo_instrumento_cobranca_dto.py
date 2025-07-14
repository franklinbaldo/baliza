import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="TipoInstrumentoCobrancaDTO")


@_attrs_define
class TipoInstrumentoCobrancaDTO:
    """
    Attributes:
        id (Union[Unset, int]):
        nome (Union[Unset, str]):
        descricao (Union[Unset, str]):
        data_inclusao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_atualizacao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        status_ativo (Union[Unset, bool]):
    """

    id: Union[Unset, int] = UNSET
    nome: Union[Unset, str] = UNSET
    descricao: Union[Unset, str] = UNSET
    data_inclusao: Union[Unset, datetime.datetime] = UNSET
    data_atualizacao: Union[Unset, datetime.datetime] = UNSET
    status_ativo: Union[Unset, bool] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        nome = self.nome

        descricao = self.descricao

        data_inclusao: Union[Unset, str] = UNSET
        if not isinstance(self.data_inclusao, Unset):
            data_inclusao = self.data_inclusao.isoformat()

        data_atualizacao: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao, Unset):
            data_atualizacao = self.data_atualizacao.isoformat()

        status_ativo = self.status_ativo

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if id is not UNSET:
            field_dict["id"] = id
        if nome is not UNSET:
            field_dict["nome"] = nome
        if descricao is not UNSET:
            field_dict["descricao"] = descricao
        if data_inclusao is not UNSET:
            field_dict["dataInclusao"] = data_inclusao
        if data_atualizacao is not UNSET:
            field_dict["dataAtualizacao"] = data_atualizacao
        if status_ativo is not UNSET:
            field_dict["statusAtivo"] = status_ativo

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id", UNSET)

        nome = d.pop("nome", UNSET)

        descricao = d.pop("descricao", UNSET)

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

        status_ativo = d.pop("statusAtivo", UNSET)

        tipo_instrumento_cobranca_dto = cls(
            id=id,
            nome=nome,
            descricao=descricao,
            data_inclusao=data_inclusao,
            data_atualizacao=data_atualizacao,
            status_ativo=status_ativo,
        )

        tipo_instrumento_cobranca_dto.additional_properties = d
        return tipo_instrumento_cobranca_dto

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
