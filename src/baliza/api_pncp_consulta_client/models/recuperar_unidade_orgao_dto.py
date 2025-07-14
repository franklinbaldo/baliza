from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RecuperarUnidadeOrgaoDTO")


@_attrs_define
class RecuperarUnidadeOrgaoDTO:
    """
    Attributes:
        uf_nome (Union[Unset, str]):
        codigo_ibge (Union[Unset, str]):
        uf_sigla (Union[Unset, str]):
        municipio_nome (Union[Unset, str]):
        codigo_unidade (Union[Unset, str]):
        nome_unidade (Union[Unset, str]):
    """

    uf_nome: Union[Unset, str] = UNSET
    codigo_ibge: Union[Unset, str] = UNSET
    uf_sigla: Union[Unset, str] = UNSET
    municipio_nome: Union[Unset, str] = UNSET
    codigo_unidade: Union[Unset, str] = UNSET
    nome_unidade: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        uf_nome = self.uf_nome

        codigo_ibge = self.codigo_ibge

        uf_sigla = self.uf_sigla

        municipio_nome = self.municipio_nome

        codigo_unidade = self.codigo_unidade

        nome_unidade = self.nome_unidade

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if uf_nome is not UNSET:
            field_dict["ufNome"] = uf_nome
        if codigo_ibge is not UNSET:
            field_dict["codigoIbge"] = codigo_ibge
        if uf_sigla is not UNSET:
            field_dict["ufSigla"] = uf_sigla
        if municipio_nome is not UNSET:
            field_dict["municipioNome"] = municipio_nome
        if codigo_unidade is not UNSET:
            field_dict["codigoUnidade"] = codigo_unidade
        if nome_unidade is not UNSET:
            field_dict["nomeUnidade"] = nome_unidade

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        uf_nome = d.pop("ufNome", UNSET)

        codigo_ibge = d.pop("codigoIbge", UNSET)

        uf_sigla = d.pop("ufSigla", UNSET)

        municipio_nome = d.pop("municipioNome", UNSET)

        codigo_unidade = d.pop("codigoUnidade", UNSET)

        nome_unidade = d.pop("nomeUnidade", UNSET)

        recuperar_unidade_orgao_dto = cls(
            uf_nome=uf_nome,
            codigo_ibge=codigo_ibge,
            uf_sigla=uf_sigla,
            municipio_nome=municipio_nome,
            codigo_unidade=codigo_unidade,
            nome_unidade=nome_unidade,
        )

        recuperar_unidade_orgao_dto.additional_properties = d
        return recuperar_unidade_orgao_dto

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
