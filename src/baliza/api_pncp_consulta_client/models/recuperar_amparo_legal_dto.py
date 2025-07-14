from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RecuperarAmparoLegalDTO")


@_attrs_define
class RecuperarAmparoLegalDTO:
    """
    Attributes:
        descricao (Union[Unset, str]):
        nome (Union[Unset, str]):
        codigo (Union[Unset, int]):
    """

    descricao: Union[Unset, str] = UNSET
    nome: Union[Unset, str] = UNSET
    codigo: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        descricao = self.descricao

        nome = self.nome

        codigo = self.codigo

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if descricao is not UNSET:
            field_dict["descricao"] = descricao
        if nome is not UNSET:
            field_dict["nome"] = nome
        if codigo is not UNSET:
            field_dict["codigo"] = codigo

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        descricao = d.pop("descricao", UNSET)

        nome = d.pop("nome", UNSET)

        codigo = d.pop("codigo", UNSET)

        recuperar_amparo_legal_dto = cls(
            descricao=descricao,
            nome=nome,
            codigo=codigo,
        )

        recuperar_amparo_legal_dto.additional_properties = d
        return recuperar_amparo_legal_dto

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
