from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ItemNotaFiscalConsultaDTO")


@_attrs_define
class ItemNotaFiscalConsultaDTO:
    """
    Attributes:
        numero_item (Union[Unset, str]):
        descricao_produto_servico (Union[Unset, str]):
        codigo_ncm (Union[Unset, str]):
        descricao_ncm (Union[Unset, str]):
        cfop (Union[Unset, str]):
        quantidade (Union[Unset, str]):
        unidade (Union[Unset, str]):
        valor_unitario (Union[Unset, str]):
        valor_total (Union[Unset, str]):
    """

    numero_item: Union[Unset, str] = UNSET
    descricao_produto_servico: Union[Unset, str] = UNSET
    codigo_ncm: Union[Unset, str] = UNSET
    descricao_ncm: Union[Unset, str] = UNSET
    cfop: Union[Unset, str] = UNSET
    quantidade: Union[Unset, str] = UNSET
    unidade: Union[Unset, str] = UNSET
    valor_unitario: Union[Unset, str] = UNSET
    valor_total: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        numero_item = self.numero_item

        descricao_produto_servico = self.descricao_produto_servico

        codigo_ncm = self.codigo_ncm

        descricao_ncm = self.descricao_ncm

        cfop = self.cfop

        quantidade = self.quantidade

        unidade = self.unidade

        valor_unitario = self.valor_unitario

        valor_total = self.valor_total

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if numero_item is not UNSET:
            field_dict["numeroItem"] = numero_item
        if descricao_produto_servico is not UNSET:
            field_dict["descricaoProdutoServico"] = descricao_produto_servico
        if codigo_ncm is not UNSET:
            field_dict["codigoNCM"] = codigo_ncm
        if descricao_ncm is not UNSET:
            field_dict["descricaoNCM"] = descricao_ncm
        if cfop is not UNSET:
            field_dict["cfop"] = cfop
        if quantidade is not UNSET:
            field_dict["quantidade"] = quantidade
        if unidade is not UNSET:
            field_dict["unidade"] = unidade
        if valor_unitario is not UNSET:
            field_dict["valorUnitario"] = valor_unitario
        if valor_total is not UNSET:
            field_dict["valorTotal"] = valor_total

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        numero_item = d.pop("numeroItem", UNSET)

        descricao_produto_servico = d.pop("descricaoProdutoServico", UNSET)

        codigo_ncm = d.pop("codigoNCM", UNSET)

        descricao_ncm = d.pop("descricaoNCM", UNSET)

        cfop = d.pop("cfop", UNSET)

        quantidade = d.pop("quantidade", UNSET)

        unidade = d.pop("unidade", UNSET)

        valor_unitario = d.pop("valorUnitario", UNSET)

        valor_total = d.pop("valorTotal", UNSET)

        item_nota_fiscal_consulta_dto = cls(
            numero_item=numero_item,
            descricao_produto_servico=descricao_produto_servico,
            codigo_ncm=codigo_ncm,
            descricao_ncm=descricao_ncm,
            cfop=cfop,
            quantidade=quantidade,
            unidade=unidade,
            valor_unitario=valor_unitario,
            valor_total=valor_total,
        )

        item_nota_fiscal_consulta_dto.additional_properties = d
        return item_nota_fiscal_consulta_dto

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
