from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RespostaErroValidacaoDTO")


@_attrs_define
class RespostaErroValidacaoDTO:
    """
    Attributes:
        message (Union[Unset, str]):
        path (Union[Unset, str]):
        timestamp (Union[Unset, str]):
        status (Union[Unset, str]):
        error (Union[Unset, str]):
    """

    message: Union[Unset, str] = UNSET
    path: Union[Unset, str] = UNSET
    timestamp: Union[Unset, str] = UNSET
    status: Union[Unset, str] = UNSET
    error: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        message = self.message

        path = self.path

        timestamp = self.timestamp

        status = self.status

        error = self.error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if message is not UNSET:
            field_dict["message"] = message
        if path is not UNSET:
            field_dict["path"] = path
        if timestamp is not UNSET:
            field_dict["timestamp"] = timestamp
        if status is not UNSET:
            field_dict["status"] = status
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        message = d.pop("message", UNSET)

        path = d.pop("path", UNSET)

        timestamp = d.pop("timestamp", UNSET)

        status = d.pop("status", UNSET)

        error = d.pop("error", UNSET)

        resposta_erro_validacao_dto = cls(
            message=message,
            path=path,
            timestamp=timestamp,
            status=status,
            error=error,
        )

        resposta_erro_validacao_dto.additional_properties = d
        return resposta_erro_validacao_dto

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
