import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="ContratacaoFonteOrcamentariaDTO")


@_attrs_define
class ContratacaoFonteOrcamentariaDTO:
    """
    Attributes:
        codigo (Union[Unset, int]):
        nome (Union[Unset, str]):
        descricao (Union[Unset, str]):
        data_inclusao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
    """

    codigo: Union[Unset, int] = UNSET
    nome: Union[Unset, str] = UNSET
    descricao: Union[Unset, str] = UNSET
    data_inclusao: Union[Unset, datetime.datetime] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        codigo = self.codigo

        nome = self.nome

        descricao = self.descricao

        data_inclusao: Union[Unset, str] = UNSET
        if not isinstance(self.data_inclusao, Unset):
            data_inclusao = self.data_inclusao.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if codigo is not UNSET:
            field_dict["codigo"] = codigo
        if nome is not UNSET:
            field_dict["nome"] = nome
        if descricao is not UNSET:
            field_dict["descricao"] = descricao
        if data_inclusao is not UNSET:
            field_dict["dataInclusao"] = data_inclusao

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        codigo = d.pop("codigo", UNSET)

        nome = d.pop("nome", UNSET)

        descricao = d.pop("descricao", UNSET)

        _data_inclusao = d.pop("dataInclusao", UNSET)
        data_inclusao: Union[Unset, datetime.datetime]
        if isinstance(_data_inclusao, Unset):
            data_inclusao = UNSET
        else:
            data_inclusao = isoparse(_data_inclusao)

        contratacao_fonte_orcamentaria_dto = cls(
            codigo=codigo,
            nome=nome,
            descricao=descricao,
            data_inclusao=data_inclusao,
        )

        contratacao_fonte_orcamentaria_dto.additional_properties = d
        return contratacao_fonte_orcamentaria_dto

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
