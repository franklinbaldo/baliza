import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="PlanoContratacaoItemDTO")


@_attrs_define
class PlanoContratacaoItemDTO:
    """
    Attributes:
        nome_classificacao_catalogo (Union[Unset, str]):
        descricao_item (Union[Unset, str]):
        quantidade_estimada (Union[Unset, float]):
        pdm_codigo (Union[Unset, str]):
        pdm_descricao (Union[Unset, str]):
        codigo_item (Union[Unset, str]):
        unidade_requisitante (Union[Unset, str]):
        grupo_contratacao_codigo (Union[Unset, str]):
        grupo_contratacao_nome (Union[Unset, str]):
        classificacao_superior_codigo (Union[Unset, str]):
        classificacao_superior_nome (Union[Unset, str]):
        unidade_fornecimento (Union[Unset, str]):
        valor_unitario (Union[Unset, float]):
        valor_orcamento_exercicio (Union[Unset, float]):
        data_desejada (Union[Unset, datetime.date]):
        data_inclusao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        numero_item (Union[Unset, int]):
        data_atualizacao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        valor_total (Union[Unset, float]):
        categoria_item_pca_nome (Union[Unset, str]):
        classificacao_catalogo_id (Union[Unset, int]):
    """

    nome_classificacao_catalogo: Union[Unset, str] = UNSET
    descricao_item: Union[Unset, str] = UNSET
    quantidade_estimada: Union[Unset, float] = UNSET
    pdm_codigo: Union[Unset, str] = UNSET
    pdm_descricao: Union[Unset, str] = UNSET
    codigo_item: Union[Unset, str] = UNSET
    unidade_requisitante: Union[Unset, str] = UNSET
    grupo_contratacao_codigo: Union[Unset, str] = UNSET
    grupo_contratacao_nome: Union[Unset, str] = UNSET
    classificacao_superior_codigo: Union[Unset, str] = UNSET
    classificacao_superior_nome: Union[Unset, str] = UNSET
    unidade_fornecimento: Union[Unset, str] = UNSET
    valor_unitario: Union[Unset, float] = UNSET
    valor_orcamento_exercicio: Union[Unset, float] = UNSET
    data_desejada: Union[Unset, datetime.date] = UNSET
    data_inclusao: Union[Unset, datetime.datetime] = UNSET
    numero_item: Union[Unset, int] = UNSET
    data_atualizacao: Union[Unset, datetime.datetime] = UNSET
    valor_total: Union[Unset, float] = UNSET
    categoria_item_pca_nome: Union[Unset, str] = UNSET
    classificacao_catalogo_id: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        nome_classificacao_catalogo = self.nome_classificacao_catalogo

        descricao_item = self.descricao_item

        quantidade_estimada = self.quantidade_estimada

        pdm_codigo = self.pdm_codigo

        pdm_descricao = self.pdm_descricao

        codigo_item = self.codigo_item

        unidade_requisitante = self.unidade_requisitante

        grupo_contratacao_codigo = self.grupo_contratacao_codigo

        grupo_contratacao_nome = self.grupo_contratacao_nome

        classificacao_superior_codigo = self.classificacao_superior_codigo

        classificacao_superior_nome = self.classificacao_superior_nome

        unidade_fornecimento = self.unidade_fornecimento

        valor_unitario = self.valor_unitario

        valor_orcamento_exercicio = self.valor_orcamento_exercicio

        data_desejada: Union[Unset, str] = UNSET
        if not isinstance(self.data_desejada, Unset):
            data_desejada = self.data_desejada.isoformat()

        data_inclusao: Union[Unset, str] = UNSET
        if not isinstance(self.data_inclusao, Unset):
            data_inclusao = self.data_inclusao.isoformat()

        numero_item = self.numero_item

        data_atualizacao: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao, Unset):
            data_atualizacao = self.data_atualizacao.isoformat()

        valor_total = self.valor_total

        categoria_item_pca_nome = self.categoria_item_pca_nome

        classificacao_catalogo_id = self.classificacao_catalogo_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if nome_classificacao_catalogo is not UNSET:
            field_dict["nomeClassificacaoCatalogo"] = nome_classificacao_catalogo
        if descricao_item is not UNSET:
            field_dict["descricaoItem"] = descricao_item
        if quantidade_estimada is not UNSET:
            field_dict["quantidadeEstimada"] = quantidade_estimada
        if pdm_codigo is not UNSET:
            field_dict["pdmCodigo"] = pdm_codigo
        if pdm_descricao is not UNSET:
            field_dict["pdmDescricao"] = pdm_descricao
        if codigo_item is not UNSET:
            field_dict["codigoItem"] = codigo_item
        if unidade_requisitante is not UNSET:
            field_dict["unidadeRequisitante"] = unidade_requisitante
        if grupo_contratacao_codigo is not UNSET:
            field_dict["grupoContratacaoCodigo"] = grupo_contratacao_codigo
        if grupo_contratacao_nome is not UNSET:
            field_dict["grupoContratacaoNome"] = grupo_contratacao_nome
        if classificacao_superior_codigo is not UNSET:
            field_dict["classificacaoSuperiorCodigo"] = classificacao_superior_codigo
        if classificacao_superior_nome is not UNSET:
            field_dict["classificacaoSuperiorNome"] = classificacao_superior_nome
        if unidade_fornecimento is not UNSET:
            field_dict["unidadeFornecimento"] = unidade_fornecimento
        if valor_unitario is not UNSET:
            field_dict["valorUnitario"] = valor_unitario
        if valor_orcamento_exercicio is not UNSET:
            field_dict["valorOrcamentoExercicio"] = valor_orcamento_exercicio
        if data_desejada is not UNSET:
            field_dict["dataDesejada"] = data_desejada
        if data_inclusao is not UNSET:
            field_dict["dataInclusao"] = data_inclusao
        if numero_item is not UNSET:
            field_dict["numeroItem"] = numero_item
        if data_atualizacao is not UNSET:
            field_dict["dataAtualizacao"] = data_atualizacao
        if valor_total is not UNSET:
            field_dict["valorTotal"] = valor_total
        if categoria_item_pca_nome is not UNSET:
            field_dict["categoriaItemPcaNome"] = categoria_item_pca_nome
        if classificacao_catalogo_id is not UNSET:
            field_dict["classificacaoCatalogoId"] = classificacao_catalogo_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        nome_classificacao_catalogo = d.pop("nomeClassificacaoCatalogo", UNSET)

        descricao_item = d.pop("descricaoItem", UNSET)

        quantidade_estimada = d.pop("quantidadeEstimada", UNSET)

        pdm_codigo = d.pop("pdmCodigo", UNSET)

        pdm_descricao = d.pop("pdmDescricao", UNSET)

        codigo_item = d.pop("codigoItem", UNSET)

        unidade_requisitante = d.pop("unidadeRequisitante", UNSET)

        grupo_contratacao_codigo = d.pop("grupoContratacaoCodigo", UNSET)

        grupo_contratacao_nome = d.pop("grupoContratacaoNome", UNSET)

        classificacao_superior_codigo = d.pop("classificacaoSuperiorCodigo", UNSET)

        classificacao_superior_nome = d.pop("classificacaoSuperiorNome", UNSET)

        unidade_fornecimento = d.pop("unidadeFornecimento", UNSET)

        valor_unitario = d.pop("valorUnitario", UNSET)

        valor_orcamento_exercicio = d.pop("valorOrcamentoExercicio", UNSET)

        _data_desejada = d.pop("dataDesejada", UNSET)
        data_desejada: Union[Unset, datetime.date]
        if isinstance(_data_desejada, Unset):
            data_desejada = UNSET
        else:
            data_desejada = isoparse(_data_desejada).date()

        _data_inclusao = d.pop("dataInclusao", UNSET)
        data_inclusao: Union[Unset, datetime.datetime]
        if isinstance(_data_inclusao, Unset):
            data_inclusao = UNSET
        else:
            data_inclusao = isoparse(_data_inclusao)

        numero_item = d.pop("numeroItem", UNSET)

        _data_atualizacao = d.pop("dataAtualizacao", UNSET)
        data_atualizacao: Union[Unset, datetime.datetime]
        if isinstance(_data_atualizacao, Unset):
            data_atualizacao = UNSET
        else:
            data_atualizacao = isoparse(_data_atualizacao)

        valor_total = d.pop("valorTotal", UNSET)

        categoria_item_pca_nome = d.pop("categoriaItemPcaNome", UNSET)

        classificacao_catalogo_id = d.pop("classificacaoCatalogoId", UNSET)

        plano_contratacao_item_dto = cls(
            nome_classificacao_catalogo=nome_classificacao_catalogo,
            descricao_item=descricao_item,
            quantidade_estimada=quantidade_estimada,
            pdm_codigo=pdm_codigo,
            pdm_descricao=pdm_descricao,
            codigo_item=codigo_item,
            unidade_requisitante=unidade_requisitante,
            grupo_contratacao_codigo=grupo_contratacao_codigo,
            grupo_contratacao_nome=grupo_contratacao_nome,
            classificacao_superior_codigo=classificacao_superior_codigo,
            classificacao_superior_nome=classificacao_superior_nome,
            unidade_fornecimento=unidade_fornecimento,
            valor_unitario=valor_unitario,
            valor_orcamento_exercicio=valor_orcamento_exercicio,
            data_desejada=data_desejada,
            data_inclusao=data_inclusao,
            numero_item=numero_item,
            data_atualizacao=data_atualizacao,
            valor_total=valor_total,
            categoria_item_pca_nome=categoria_item_pca_nome,
            classificacao_catalogo_id=classificacao_catalogo_id,
        )

        plano_contratacao_item_dto.additional_properties = d
        return plano_contratacao_item_dto

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
