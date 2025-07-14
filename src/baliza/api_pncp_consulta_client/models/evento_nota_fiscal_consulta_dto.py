from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EventoNotaFiscalConsultaDTO")


@_attrs_define
class EventoNotaFiscalConsultaDTO:
    """
    Attributes:
        data_evento (Union[Unset, str]):
        tipo_evento (Union[Unset, str]):
        evento (Union[Unset, str]):
        motivo_evento (Union[Unset, str]):
    """

    data_evento: Union[Unset, str] = UNSET
    tipo_evento: Union[Unset, str] = UNSET
    evento: Union[Unset, str] = UNSET
    motivo_evento: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data_evento = self.data_evento

        tipo_evento = self.tipo_evento

        evento = self.evento

        motivo_evento = self.motivo_evento

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if data_evento is not UNSET:
            field_dict["dataEvento"] = data_evento
        if tipo_evento is not UNSET:
            field_dict["tipoEvento"] = tipo_evento
        if evento is not UNSET:
            field_dict["evento"] = evento
        if motivo_evento is not UNSET:
            field_dict["motivoEvento"] = motivo_evento

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        data_evento = d.pop("dataEvento", UNSET)

        tipo_evento = d.pop("tipoEvento", UNSET)

        evento = d.pop("evento", UNSET)

        motivo_evento = d.pop("motivoEvento", UNSET)

        evento_nota_fiscal_consulta_dto = cls(
            data_evento=data_evento,
            tipo_evento=tipo_evento,
            evento=evento,
            motivo_evento=motivo_evento,
        )

        evento_nota_fiscal_consulta_dto.additional_properties = d
        return evento_nota_fiscal_consulta_dto

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
