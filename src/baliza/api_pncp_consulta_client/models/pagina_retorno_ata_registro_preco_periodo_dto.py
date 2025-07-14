from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.ata_registro_preco_periodo_dto import AtaRegistroPrecoPeriodoDTO


T = TypeVar("T", bound="PaginaRetornoAtaRegistroPrecoPeriodoDTO")


@_attrs_define
class PaginaRetornoAtaRegistroPrecoPeriodoDTO:
    """
    Attributes:
        data (Union[Unset, list['AtaRegistroPrecoPeriodoDTO']]):
        total_registros (Union[Unset, int]):
        total_paginas (Union[Unset, int]):
        numero_pagina (Union[Unset, int]):
        paginas_restantes (Union[Unset, int]):
        empty (Union[Unset, bool]):
    """

    data: Union[Unset, list["AtaRegistroPrecoPeriodoDTO"]] = UNSET
    total_registros: Union[Unset, int] = UNSET
    total_paginas: Union[Unset, int] = UNSET
    numero_pagina: Union[Unset, int] = UNSET
    paginas_restantes: Union[Unset, int] = UNSET
    empty: Union[Unset, bool] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.data, Unset):
            data = []
            for data_item_data in self.data:
                data_item = data_item_data.to_dict()
                data.append(data_item)

        total_registros = self.total_registros

        total_paginas = self.total_paginas

        numero_pagina = self.numero_pagina

        paginas_restantes = self.paginas_restantes

        empty = self.empty

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if data is not UNSET:
            field_dict["data"] = data
        if total_registros is not UNSET:
            field_dict["totalRegistros"] = total_registros
        if total_paginas is not UNSET:
            field_dict["totalPaginas"] = total_paginas
        if numero_pagina is not UNSET:
            field_dict["numeroPagina"] = numero_pagina
        if paginas_restantes is not UNSET:
            field_dict["paginasRestantes"] = paginas_restantes
        if empty is not UNSET:
            field_dict["empty"] = empty

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ata_registro_preco_periodo_dto import AtaRegistroPrecoPeriodoDTO

        d = dict(src_dict)
        data = []
        _data = d.pop("data", UNSET)
        for data_item_data in _data or []:
            data_item = AtaRegistroPrecoPeriodoDTO.from_dict(data_item_data)

            data.append(data_item)

        total_registros = d.pop("totalRegistros", UNSET)

        total_paginas = d.pop("totalPaginas", UNSET)

        numero_pagina = d.pop("numeroPagina", UNSET)

        paginas_restantes = d.pop("paginasRestantes", UNSET)

        empty = d.pop("empty", UNSET)

        pagina_retorno_ata_registro_preco_periodo_dto = cls(
            data=data,
            total_registros=total_registros,
            total_paginas=total_paginas,
            numero_pagina=numero_pagina,
            paginas_restantes=paginas_restantes,
            empty=empty,
        )

        pagina_retorno_ata_registro_preco_periodo_dto.additional_properties = d
        return pagina_retorno_ata_registro_preco_periodo_dto

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
