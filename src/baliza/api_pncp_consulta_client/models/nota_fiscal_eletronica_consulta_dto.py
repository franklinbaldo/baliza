import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.evento_nota_fiscal_consulta_dto import EventoNotaFiscalConsultaDTO
    from ..models.item_nota_fiscal_consulta_dto import ItemNotaFiscalConsultaDTO


T = TypeVar("T", bound="NotaFiscalEletronicaConsultaDTO")


@_attrs_define
class NotaFiscalEletronicaConsultaDTO:
    """
    Attributes:
        instrumento_cobranca_id (Union[Unset, int]):
        chave (Union[Unset, str]):
        nf_transparencia_id (Union[Unset, int]):
        numero (Union[Unset, int]):
        serie (Union[Unset, int]):
        data_emissao (Union[Unset, str]):
        ni_emitente (Union[Unset, str]):
        nome_emitente (Union[Unset, str]):
        nome_municipio_emitente (Union[Unset, str]):
        codigo_orgao_destinatario (Union[Unset, str]):
        nome_orgao_destinatario (Union[Unset, str]):
        codigo_orgao_superior_destinatario (Union[Unset, str]):
        nome_orgao_superior_destinatario (Union[Unset, str]):
        valor_nota_fiscal (Union[Unset, str]):
        tipo_evento_mais_recente (Union[Unset, str]):
        data_tipo_evento_mais_recente (Union[Unset, str]):
        data_inclusao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_atualizacao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        itens (Union[Unset, list['ItemNotaFiscalConsultaDTO']]):
        eventos (Union[Unset, list['EventoNotaFiscalConsultaDTO']]):
    """

    instrumento_cobranca_id: Union[Unset, int] = UNSET
    chave: Union[Unset, str] = UNSET
    nf_transparencia_id: Union[Unset, int] = UNSET
    numero: Union[Unset, int] = UNSET
    serie: Union[Unset, int] = UNSET
    data_emissao: Union[Unset, str] = UNSET
    ni_emitente: Union[Unset, str] = UNSET
    nome_emitente: Union[Unset, str] = UNSET
    nome_municipio_emitente: Union[Unset, str] = UNSET
    codigo_orgao_destinatario: Union[Unset, str] = UNSET
    nome_orgao_destinatario: Union[Unset, str] = UNSET
    codigo_orgao_superior_destinatario: Union[Unset, str] = UNSET
    nome_orgao_superior_destinatario: Union[Unset, str] = UNSET
    valor_nota_fiscal: Union[Unset, str] = UNSET
    tipo_evento_mais_recente: Union[Unset, str] = UNSET
    data_tipo_evento_mais_recente: Union[Unset, str] = UNSET
    data_inclusao: Union[Unset, datetime.datetime] = UNSET
    data_atualizacao: Union[Unset, datetime.datetime] = UNSET
    itens: Union[Unset, list["ItemNotaFiscalConsultaDTO"]] = UNSET
    eventos: Union[Unset, list["EventoNotaFiscalConsultaDTO"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        instrumento_cobranca_id = self.instrumento_cobranca_id

        chave = self.chave

        nf_transparencia_id = self.nf_transparencia_id

        numero = self.numero

        serie = self.serie

        data_emissao = self.data_emissao

        ni_emitente = self.ni_emitente

        nome_emitente = self.nome_emitente

        nome_municipio_emitente = self.nome_municipio_emitente

        codigo_orgao_destinatario = self.codigo_orgao_destinatario

        nome_orgao_destinatario = self.nome_orgao_destinatario

        codigo_orgao_superior_destinatario = self.codigo_orgao_superior_destinatario

        nome_orgao_superior_destinatario = self.nome_orgao_superior_destinatario

        valor_nota_fiscal = self.valor_nota_fiscal

        tipo_evento_mais_recente = self.tipo_evento_mais_recente

        data_tipo_evento_mais_recente = self.data_tipo_evento_mais_recente

        data_inclusao: Union[Unset, str] = UNSET
        if not isinstance(self.data_inclusao, Unset):
            data_inclusao = self.data_inclusao.isoformat()

        data_atualizacao: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao, Unset):
            data_atualizacao = self.data_atualizacao.isoformat()

        itens: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.itens, Unset):
            itens = []
            for itens_item_data in self.itens:
                itens_item = itens_item_data.to_dict()
                itens.append(itens_item)

        eventos: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.eventos, Unset):
            eventos = []
            for eventos_item_data in self.eventos:
                eventos_item = eventos_item_data.to_dict()
                eventos.append(eventos_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if instrumento_cobranca_id is not UNSET:
            field_dict["instrumentoCobrancaId"] = instrumento_cobranca_id
        if chave is not UNSET:
            field_dict["chave"] = chave
        if nf_transparencia_id is not UNSET:
            field_dict["nfTransparenciaID"] = nf_transparencia_id
        if numero is not UNSET:
            field_dict["numero"] = numero
        if serie is not UNSET:
            field_dict["serie"] = serie
        if data_emissao is not UNSET:
            field_dict["dataEmissao"] = data_emissao
        if ni_emitente is not UNSET:
            field_dict["niEmitente"] = ni_emitente
        if nome_emitente is not UNSET:
            field_dict["nomeEmitente"] = nome_emitente
        if nome_municipio_emitente is not UNSET:
            field_dict["nomeMunicipioEmitente"] = nome_municipio_emitente
        if codigo_orgao_destinatario is not UNSET:
            field_dict["codigoOrgaoDestinatario"] = codigo_orgao_destinatario
        if nome_orgao_destinatario is not UNSET:
            field_dict["nomeOrgaoDestinatario"] = nome_orgao_destinatario
        if codigo_orgao_superior_destinatario is not UNSET:
            field_dict["codigoOrgaoSuperiorDestinatario"] = codigo_orgao_superior_destinatario
        if nome_orgao_superior_destinatario is not UNSET:
            field_dict["nomeOrgaoSuperiorDestinatario"] = nome_orgao_superior_destinatario
        if valor_nota_fiscal is not UNSET:
            field_dict["valorNotaFiscal"] = valor_nota_fiscal
        if tipo_evento_mais_recente is not UNSET:
            field_dict["tipoEventoMaisRecente"] = tipo_evento_mais_recente
        if data_tipo_evento_mais_recente is not UNSET:
            field_dict["dataTipoEventoMaisRecente"] = data_tipo_evento_mais_recente
        if data_inclusao is not UNSET:
            field_dict["dataInclusao"] = data_inclusao
        if data_atualizacao is not UNSET:
            field_dict["dataAtualizacao"] = data_atualizacao
        if itens is not UNSET:
            field_dict["itens"] = itens
        if eventos is not UNSET:
            field_dict["eventos"] = eventos

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.evento_nota_fiscal_consulta_dto import EventoNotaFiscalConsultaDTO
        from ..models.item_nota_fiscal_consulta_dto import ItemNotaFiscalConsultaDTO

        d = dict(src_dict)
        instrumento_cobranca_id = d.pop("instrumentoCobrancaId", UNSET)

        chave = d.pop("chave", UNSET)

        nf_transparencia_id = d.pop("nfTransparenciaID", UNSET)

        numero = d.pop("numero", UNSET)

        serie = d.pop("serie", UNSET)

        data_emissao = d.pop("dataEmissao", UNSET)

        ni_emitente = d.pop("niEmitente", UNSET)

        nome_emitente = d.pop("nomeEmitente", UNSET)

        nome_municipio_emitente = d.pop("nomeMunicipioEmitente", UNSET)

        codigo_orgao_destinatario = d.pop("codigoOrgaoDestinatario", UNSET)

        nome_orgao_destinatario = d.pop("nomeOrgaoDestinatario", UNSET)

        codigo_orgao_superior_destinatario = d.pop("codigoOrgaoSuperiorDestinatario", UNSET)

        nome_orgao_superior_destinatario = d.pop("nomeOrgaoSuperiorDestinatario", UNSET)

        valor_nota_fiscal = d.pop("valorNotaFiscal", UNSET)

        tipo_evento_mais_recente = d.pop("tipoEventoMaisRecente", UNSET)

        data_tipo_evento_mais_recente = d.pop("dataTipoEventoMaisRecente", UNSET)

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

        itens = []
        _itens = d.pop("itens", UNSET)
        for itens_item_data in _itens or []:
            itens_item = ItemNotaFiscalConsultaDTO.from_dict(itens_item_data)

            itens.append(itens_item)

        eventos = []
        _eventos = d.pop("eventos", UNSET)
        for eventos_item_data in _eventos or []:
            eventos_item = EventoNotaFiscalConsultaDTO.from_dict(eventos_item_data)

            eventos.append(eventos_item)

        nota_fiscal_eletronica_consulta_dto = cls(
            instrumento_cobranca_id=instrumento_cobranca_id,
            chave=chave,
            nf_transparencia_id=nf_transparencia_id,
            numero=numero,
            serie=serie,
            data_emissao=data_emissao,
            ni_emitente=ni_emitente,
            nome_emitente=nome_emitente,
            nome_municipio_emitente=nome_municipio_emitente,
            codigo_orgao_destinatario=codigo_orgao_destinatario,
            nome_orgao_destinatario=nome_orgao_destinatario,
            codigo_orgao_superior_destinatario=codigo_orgao_superior_destinatario,
            nome_orgao_superior_destinatario=nome_orgao_superior_destinatario,
            valor_nota_fiscal=valor_nota_fiscal,
            tipo_evento_mais_recente=tipo_evento_mais_recente,
            data_tipo_evento_mais_recente=data_tipo_evento_mais_recente,
            data_inclusao=data_inclusao,
            data_atualizacao=data_atualizacao,
            itens=itens,
            eventos=eventos,
        )

        nota_fiscal_eletronica_consulta_dto.additional_properties = d
        return nota_fiscal_eletronica_consulta_dto

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
