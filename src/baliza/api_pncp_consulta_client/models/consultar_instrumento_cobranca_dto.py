import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.nota_fiscal_eletronica_consulta_dto import NotaFiscalEletronicaConsultaDTO
    from ..models.recuperar_contrato_dto import RecuperarContratoDTO
    from ..models.tipo_instrumento_cobranca_dto import TipoInstrumentoCobrancaDTO


T = TypeVar("T", bound="ConsultarInstrumentoCobrancaDTO")


@_attrs_define
class ConsultarInstrumentoCobrancaDTO:
    """
    Attributes:
        cnpj (Union[Unset, str]):
        ano (Union[Unset, int]):
        sequencial_contrato (Union[Unset, int]):
        sequencial_instrumento_cobranca (Union[Unset, int]):
        tipo_instrumento_cobranca (Union[Unset, TipoInstrumentoCobrancaDTO]):
        numero_instrumento_cobranca (Union[Unset, str]):
        data_emissao_documento (Union[Unset, datetime.date]):
        observacao (Union[Unset, str]):
        chave_n_fe (Union[Unset, str]):
        fonte_n_fe (Union[Unset, int]):
        data_consulta_n_fe (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        status_response_n_fe (Union[Unset, str]):
        json_response_n_fe (Union[Unset, str]):
        nota_fiscal_eletronica (Union[Unset, NotaFiscalEletronicaConsultaDTO]):
        data_inclusao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_atualizacao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        recuperar_contrato_dto (Union[Unset, RecuperarContratoDTO]):
    """

    cnpj: Union[Unset, str] = UNSET
    ano: Union[Unset, int] = UNSET
    sequencial_contrato: Union[Unset, int] = UNSET
    sequencial_instrumento_cobranca: Union[Unset, int] = UNSET
    tipo_instrumento_cobranca: Union[Unset, "TipoInstrumentoCobrancaDTO"] = UNSET
    numero_instrumento_cobranca: Union[Unset, str] = UNSET
    data_emissao_documento: Union[Unset, datetime.date] = UNSET
    observacao: Union[Unset, str] = UNSET
    chave_n_fe: Union[Unset, str] = UNSET
    fonte_n_fe: Union[Unset, int] = UNSET
    data_consulta_n_fe: Union[Unset, datetime.datetime] = UNSET
    status_response_n_fe: Union[Unset, str] = UNSET
    json_response_n_fe: Union[Unset, str] = UNSET
    nota_fiscal_eletronica: Union[Unset, "NotaFiscalEletronicaConsultaDTO"] = UNSET
    data_inclusao: Union[Unset, datetime.datetime] = UNSET
    data_atualizacao: Union[Unset, datetime.datetime] = UNSET
    recuperar_contrato_dto: Union[Unset, "RecuperarContratoDTO"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        cnpj = self.cnpj

        ano = self.ano

        sequencial_contrato = self.sequencial_contrato

        sequencial_instrumento_cobranca = self.sequencial_instrumento_cobranca

        tipo_instrumento_cobranca: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.tipo_instrumento_cobranca, Unset):
            tipo_instrumento_cobranca = self.tipo_instrumento_cobranca.to_dict()

        numero_instrumento_cobranca = self.numero_instrumento_cobranca

        data_emissao_documento: Union[Unset, str] = UNSET
        if not isinstance(self.data_emissao_documento, Unset):
            data_emissao_documento = self.data_emissao_documento.isoformat()

        observacao = self.observacao

        chave_n_fe = self.chave_n_fe

        fonte_n_fe = self.fonte_n_fe

        data_consulta_n_fe: Union[Unset, str] = UNSET
        if not isinstance(self.data_consulta_n_fe, Unset):
            data_consulta_n_fe = self.data_consulta_n_fe.isoformat()

        status_response_n_fe = self.status_response_n_fe

        json_response_n_fe = self.json_response_n_fe

        nota_fiscal_eletronica: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.nota_fiscal_eletronica, Unset):
            nota_fiscal_eletronica = self.nota_fiscal_eletronica.to_dict()

        data_inclusao: Union[Unset, str] = UNSET
        if not isinstance(self.data_inclusao, Unset):
            data_inclusao = self.data_inclusao.isoformat()

        data_atualizacao: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao, Unset):
            data_atualizacao = self.data_atualizacao.isoformat()

        recuperar_contrato_dto: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.recuperar_contrato_dto, Unset):
            recuperar_contrato_dto = self.recuperar_contrato_dto.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if cnpj is not UNSET:
            field_dict["cnpj"] = cnpj
        if ano is not UNSET:
            field_dict["ano"] = ano
        if sequencial_contrato is not UNSET:
            field_dict["sequencialContrato"] = sequencial_contrato
        if sequencial_instrumento_cobranca is not UNSET:
            field_dict["sequencialInstrumentoCobranca"] = sequencial_instrumento_cobranca
        if tipo_instrumento_cobranca is not UNSET:
            field_dict["tipoInstrumentoCobranca"] = tipo_instrumento_cobranca
        if numero_instrumento_cobranca is not UNSET:
            field_dict["numeroInstrumentoCobranca"] = numero_instrumento_cobranca
        if data_emissao_documento is not UNSET:
            field_dict["dataEmissaoDocumento"] = data_emissao_documento
        if observacao is not UNSET:
            field_dict["observacao"] = observacao
        if chave_n_fe is not UNSET:
            field_dict["chaveNFe"] = chave_n_fe
        if fonte_n_fe is not UNSET:
            field_dict["fonteNFe"] = fonte_n_fe
        if data_consulta_n_fe is not UNSET:
            field_dict["dataConsultaNFe"] = data_consulta_n_fe
        if status_response_n_fe is not UNSET:
            field_dict["statusResponseNFe"] = status_response_n_fe
        if json_response_n_fe is not UNSET:
            field_dict["jsonResponseNFe"] = json_response_n_fe
        if nota_fiscal_eletronica is not UNSET:
            field_dict["notaFiscalEletronica"] = nota_fiscal_eletronica
        if data_inclusao is not UNSET:
            field_dict["dataInclusao"] = data_inclusao
        if data_atualizacao is not UNSET:
            field_dict["dataAtualizacao"] = data_atualizacao
        if recuperar_contrato_dto is not UNSET:
            field_dict["recuperarContratoDTO"] = recuperar_contrato_dto

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.nota_fiscal_eletronica_consulta_dto import NotaFiscalEletronicaConsultaDTO
        from ..models.recuperar_contrato_dto import RecuperarContratoDTO
        from ..models.tipo_instrumento_cobranca_dto import TipoInstrumentoCobrancaDTO

        d = dict(src_dict)
        cnpj = d.pop("cnpj", UNSET)

        ano = d.pop("ano", UNSET)

        sequencial_contrato = d.pop("sequencialContrato", UNSET)

        sequencial_instrumento_cobranca = d.pop("sequencialInstrumentoCobranca", UNSET)

        _tipo_instrumento_cobranca = d.pop("tipoInstrumentoCobranca", UNSET)
        tipo_instrumento_cobranca: Union[Unset, TipoInstrumentoCobrancaDTO]
        if isinstance(_tipo_instrumento_cobranca, Unset):
            tipo_instrumento_cobranca = UNSET
        else:
            tipo_instrumento_cobranca = TipoInstrumentoCobrancaDTO.from_dict(_tipo_instrumento_cobranca)

        numero_instrumento_cobranca = d.pop("numeroInstrumentoCobranca", UNSET)

        _data_emissao_documento = d.pop("dataEmissaoDocumento", UNSET)
        data_emissao_documento: Union[Unset, datetime.date]
        if isinstance(_data_emissao_documento, Unset):
            data_emissao_documento = UNSET
        else:
            data_emissao_documento = isoparse(_data_emissao_documento).date()

        observacao = d.pop("observacao", UNSET)

        chave_n_fe = d.pop("chaveNFe", UNSET)

        fonte_n_fe = d.pop("fonteNFe", UNSET)

        _data_consulta_n_fe = d.pop("dataConsultaNFe", UNSET)
        data_consulta_n_fe: Union[Unset, datetime.datetime]
        if isinstance(_data_consulta_n_fe, Unset):
            data_consulta_n_fe = UNSET
        else:
            data_consulta_n_fe = isoparse(_data_consulta_n_fe)

        status_response_n_fe = d.pop("statusResponseNFe", UNSET)

        json_response_n_fe = d.pop("jsonResponseNFe", UNSET)

        _nota_fiscal_eletronica = d.pop("notaFiscalEletronica", UNSET)
        nota_fiscal_eletronica: Union[Unset, NotaFiscalEletronicaConsultaDTO]
        if isinstance(_nota_fiscal_eletronica, Unset):
            nota_fiscal_eletronica = UNSET
        else:
            nota_fiscal_eletronica = NotaFiscalEletronicaConsultaDTO.from_dict(_nota_fiscal_eletronica)

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

        _recuperar_contrato_dto = d.pop("recuperarContratoDTO", UNSET)
        recuperar_contrato_dto: Union[Unset, RecuperarContratoDTO]
        if isinstance(_recuperar_contrato_dto, Unset):
            recuperar_contrato_dto = UNSET
        else:
            recuperar_contrato_dto = RecuperarContratoDTO.from_dict(_recuperar_contrato_dto)

        consultar_instrumento_cobranca_dto = cls(
            cnpj=cnpj,
            ano=ano,
            sequencial_contrato=sequencial_contrato,
            sequencial_instrumento_cobranca=sequencial_instrumento_cobranca,
            tipo_instrumento_cobranca=tipo_instrumento_cobranca,
            numero_instrumento_cobranca=numero_instrumento_cobranca,
            data_emissao_documento=data_emissao_documento,
            observacao=observacao,
            chave_n_fe=chave_n_fe,
            fonte_n_fe=fonte_n_fe,
            data_consulta_n_fe=data_consulta_n_fe,
            status_response_n_fe=status_response_n_fe,
            json_response_n_fe=json_response_n_fe,
            nota_fiscal_eletronica=nota_fiscal_eletronica,
            data_inclusao=data_inclusao,
            data_atualizacao=data_atualizacao,
            recuperar_contrato_dto=recuperar_contrato_dto,
        )

        consultar_instrumento_cobranca_dto.additional_properties = d
        return consultar_instrumento_cobranca_dto

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
