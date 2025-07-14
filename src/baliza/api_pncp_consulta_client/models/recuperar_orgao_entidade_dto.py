from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RecuperarOrgaoEntidadeDTO")


@_attrs_define
class RecuperarOrgaoEntidadeDTO:
    """
    Attributes:
        cnpj (Union[Unset, str]):
        razao_social (Union[Unset, str]):
        poder_id (Union[Unset, str]):
        esfera_id (Union[Unset, str]):
    """

    cnpj: Union[Unset, str] = UNSET
    razao_social: Union[Unset, str] = UNSET
    poder_id: Union[Unset, str] = UNSET
    esfera_id: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        cnpj = self.cnpj

        razao_social = self.razao_social

        poder_id = self.poder_id

        esfera_id = self.esfera_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if cnpj is not UNSET:
            field_dict["cnpj"] = cnpj
        if razao_social is not UNSET:
            field_dict["razaoSocial"] = razao_social
        if poder_id is not UNSET:
            field_dict["poderId"] = poder_id
        if esfera_id is not UNSET:
            field_dict["esferaId"] = esfera_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        cnpj = d.pop("cnpj", UNSET)

        razao_social = d.pop("razaoSocial", UNSET)

        poder_id = d.pop("poderId", UNSET)

        esfera_id = d.pop("esferaId", UNSET)

        recuperar_orgao_entidade_dto = cls(
            cnpj=cnpj,
            razao_social=razao_social,
            poder_id=poder_id,
            esfera_id=esfera_id,
        )

        recuperar_orgao_entidade_dto.additional_properties = d
        return recuperar_orgao_entidade_dto

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
