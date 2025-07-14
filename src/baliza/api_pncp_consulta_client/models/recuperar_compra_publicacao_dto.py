import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.recuperar_compra_publicacao_dto_situacao_compra_id import RecuperarCompraPublicacaoDTOSituacaoCompraId
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.contratacao_fonte_orcamentaria_dto import ContratacaoFonteOrcamentariaDTO
    from ..models.recuperar_amparo_legal_dto import RecuperarAmparoLegalDTO
    from ..models.recuperar_orgao_entidade_dto import RecuperarOrgaoEntidadeDTO
    from ..models.recuperar_unidade_orgao_dto import RecuperarUnidadeOrgaoDTO


T = TypeVar("T", bound="RecuperarCompraPublicacaoDTO")


@_attrs_define
class RecuperarCompraPublicacaoDTO:
    """
    Attributes:
        informacao_complementar (Union[Unset, str]):
        processo (Union[Unset, str]):
        objeto_compra (Union[Unset, str]):
        link_sistema_origem (Union[Unset, str]):
        justificativa_presencial (Union[Unset, str]):
        unidade_sub_rogada (Union[Unset, RecuperarUnidadeOrgaoDTO]):
        orgao_sub_rogado (Union[Unset, RecuperarOrgaoEntidadeDTO]):
        valor_total_homologado (Union[Unset, float]):
        srp (Union[Unset, bool]):
        amparo_legal (Union[Unset, RecuperarAmparoLegalDTO]):
        data_abertura_proposta (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_encerramento_proposta (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        orgao_entidade (Union[Unset, RecuperarOrgaoEntidadeDTO]):
        ano_compra (Union[Unset, int]):
        sequencial_compra (Union[Unset, int]):
        data_inclusao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_publicacao_pncp (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        data_atualizacao (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        numero_compra (Union[Unset, str]):
        unidade_orgao (Union[Unset, RecuperarUnidadeOrgaoDTO]):
        modo_disputa_id (Union[Unset, int]):
        modalidade_id (Union[Unset, int]):
        data_atualizacao_global (Union[Unset, datetime.datetime]):  Example: 2025-06-17T15:16:15.
        link_processo_eletronico (Union[Unset, str]):
        numero_controle_pncp (Union[Unset, str]):
        valor_total_estimado (Union[Unset, float]):
        modo_disputa_nome (Union[Unset, str]):
        tipo_instrumento_convocatorio_codigo (Union[Unset, int]):
        tipo_instrumento_convocatorio_nome (Union[Unset, str]):
        fontes_orcamentarias (Union[Unset, list['ContratacaoFonteOrcamentariaDTO']]):
        situacao_compra_id (Union[Unset, RecuperarCompraPublicacaoDTOSituacaoCompraId]):
        situacao_compra_nome (Union[Unset, str]):
        usuario_nome (Union[Unset, str]):
        modalidade_nome (Union[Unset, str]):
    """

    informacao_complementar: Union[Unset, str] = UNSET
    processo: Union[Unset, str] = UNSET
    objeto_compra: Union[Unset, str] = UNSET
    link_sistema_origem: Union[Unset, str] = UNSET
    justificativa_presencial: Union[Unset, str] = UNSET
    unidade_sub_rogada: Union[Unset, "RecuperarUnidadeOrgaoDTO"] = UNSET
    orgao_sub_rogado: Union[Unset, "RecuperarOrgaoEntidadeDTO"] = UNSET
    valor_total_homologado: Union[Unset, float] = UNSET
    srp: Union[Unset, bool] = UNSET
    amparo_legal: Union[Unset, "RecuperarAmparoLegalDTO"] = UNSET
    data_abertura_proposta: Union[Unset, datetime.datetime] = UNSET
    data_encerramento_proposta: Union[Unset, datetime.datetime] = UNSET
    orgao_entidade: Union[Unset, "RecuperarOrgaoEntidadeDTO"] = UNSET
    ano_compra: Union[Unset, int] = UNSET
    sequencial_compra: Union[Unset, int] = UNSET
    data_inclusao: Union[Unset, datetime.datetime] = UNSET
    data_publicacao_pncp: Union[Unset, datetime.datetime] = UNSET
    data_atualizacao: Union[Unset, datetime.datetime] = UNSET
    numero_compra: Union[Unset, str] = UNSET
    unidade_orgao: Union[Unset, "RecuperarUnidadeOrgaoDTO"] = UNSET
    modo_disputa_id: Union[Unset, int] = UNSET
    modalidade_id: Union[Unset, int] = UNSET
    data_atualizacao_global: Union[Unset, datetime.datetime] = UNSET
    link_processo_eletronico: Union[Unset, str] = UNSET
    numero_controle_pncp: Union[Unset, str] = UNSET
    valor_total_estimado: Union[Unset, float] = UNSET
    modo_disputa_nome: Union[Unset, str] = UNSET
    tipo_instrumento_convocatorio_codigo: Union[Unset, int] = UNSET
    tipo_instrumento_convocatorio_nome: Union[Unset, str] = UNSET
    fontes_orcamentarias: Union[Unset, list["ContratacaoFonteOrcamentariaDTO"]] = UNSET
    situacao_compra_id: Union[Unset, RecuperarCompraPublicacaoDTOSituacaoCompraId] = UNSET
    situacao_compra_nome: Union[Unset, str] = UNSET
    usuario_nome: Union[Unset, str] = UNSET
    modalidade_nome: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        informacao_complementar = self.informacao_complementar

        processo = self.processo

        objeto_compra = self.objeto_compra

        link_sistema_origem = self.link_sistema_origem

        justificativa_presencial = self.justificativa_presencial

        unidade_sub_rogada: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.unidade_sub_rogada, Unset):
            unidade_sub_rogada = self.unidade_sub_rogada.to_dict()

        orgao_sub_rogado: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.orgao_sub_rogado, Unset):
            orgao_sub_rogado = self.orgao_sub_rogado.to_dict()

        valor_total_homologado = self.valor_total_homologado

        srp = self.srp

        amparo_legal: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.amparo_legal, Unset):
            amparo_legal = self.amparo_legal.to_dict()

        data_abertura_proposta: Union[Unset, str] = UNSET
        if not isinstance(self.data_abertura_proposta, Unset):
            data_abertura_proposta = self.data_abertura_proposta.isoformat()

        data_encerramento_proposta: Union[Unset, str] = UNSET
        if not isinstance(self.data_encerramento_proposta, Unset):
            data_encerramento_proposta = self.data_encerramento_proposta.isoformat()

        orgao_entidade: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.orgao_entidade, Unset):
            orgao_entidade = self.orgao_entidade.to_dict()

        ano_compra = self.ano_compra

        sequencial_compra = self.sequencial_compra

        data_inclusao: Union[Unset, str] = UNSET
        if not isinstance(self.data_inclusao, Unset):
            data_inclusao = self.data_inclusao.isoformat()

        data_publicacao_pncp: Union[Unset, str] = UNSET
        if not isinstance(self.data_publicacao_pncp, Unset):
            data_publicacao_pncp = self.data_publicacao_pncp.isoformat()

        data_atualizacao: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao, Unset):
            data_atualizacao = self.data_atualizacao.isoformat()

        numero_compra = self.numero_compra

        unidade_orgao: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.unidade_orgao, Unset):
            unidade_orgao = self.unidade_orgao.to_dict()

        modo_disputa_id = self.modo_disputa_id

        modalidade_id = self.modalidade_id

        data_atualizacao_global: Union[Unset, str] = UNSET
        if not isinstance(self.data_atualizacao_global, Unset):
            data_atualizacao_global = self.data_atualizacao_global.isoformat()

        link_processo_eletronico = self.link_processo_eletronico

        numero_controle_pncp = self.numero_controle_pncp

        valor_total_estimado = self.valor_total_estimado

        modo_disputa_nome = self.modo_disputa_nome

        tipo_instrumento_convocatorio_codigo = self.tipo_instrumento_convocatorio_codigo

        tipo_instrumento_convocatorio_nome = self.tipo_instrumento_convocatorio_nome

        fontes_orcamentarias: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.fontes_orcamentarias, Unset):
            fontes_orcamentarias = []
            for fontes_orcamentarias_item_data in self.fontes_orcamentarias:
                fontes_orcamentarias_item = fontes_orcamentarias_item_data.to_dict()
                fontes_orcamentarias.append(fontes_orcamentarias_item)

        situacao_compra_id: Union[Unset, str] = UNSET
        if not isinstance(self.situacao_compra_id, Unset):
            situacao_compra_id = self.situacao_compra_id.value

        situacao_compra_nome = self.situacao_compra_nome

        usuario_nome = self.usuario_nome

        modalidade_nome = self.modalidade_nome

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if informacao_complementar is not UNSET:
            field_dict["informacaoComplementar"] = informacao_complementar
        if processo is not UNSET:
            field_dict["processo"] = processo
        if objeto_compra is not UNSET:
            field_dict["objetoCompra"] = objeto_compra
        if link_sistema_origem is not UNSET:
            field_dict["linkSistemaOrigem"] = link_sistema_origem
        if justificativa_presencial is not UNSET:
            field_dict["justificativaPresencial"] = justificativa_presencial
        if unidade_sub_rogada is not UNSET:
            field_dict["unidadeSubRogada"] = unidade_sub_rogada
        if orgao_sub_rogado is not UNSET:
            field_dict["orgaoSubRogado"] = orgao_sub_rogado
        if valor_total_homologado is not UNSET:
            field_dict["valorTotalHomologado"] = valor_total_homologado
        if srp is not UNSET:
            field_dict["srp"] = srp
        if amparo_legal is not UNSET:
            field_dict["amparoLegal"] = amparo_legal
        if data_abertura_proposta is not UNSET:
            field_dict["dataAberturaProposta"] = data_abertura_proposta
        if data_encerramento_proposta is not UNSET:
            field_dict["dataEncerramentoProposta"] = data_encerramento_proposta
        if orgao_entidade is not UNSET:
            field_dict["orgaoEntidade"] = orgao_entidade
        if ano_compra is not UNSET:
            field_dict["anoCompra"] = ano_compra
        if sequencial_compra is not UNSET:
            field_dict["sequencialCompra"] = sequencial_compra
        if data_inclusao is not UNSET:
            field_dict["dataInclusao"] = data_inclusao
        if data_publicacao_pncp is not UNSET:
            field_dict["dataPublicacaoPncp"] = data_publicacao_pncp
        if data_atualizacao is not UNSET:
            field_dict["dataAtualizacao"] = data_atualizacao
        if numero_compra is not UNSET:
            field_dict["numeroCompra"] = numero_compra
        if unidade_orgao is not UNSET:
            field_dict["unidadeOrgao"] = unidade_orgao
        if modo_disputa_id is not UNSET:
            field_dict["modoDisputaId"] = modo_disputa_id
        if modalidade_id is not UNSET:
            field_dict["modalidadeId"] = modalidade_id
        if data_atualizacao_global is not UNSET:
            field_dict["dataAtualizacaoGlobal"] = data_atualizacao_global
        if link_processo_eletronico is not UNSET:
            field_dict["linkProcessoEletronico"] = link_processo_eletronico
        if numero_controle_pncp is not UNSET:
            field_dict["numeroControlePNCP"] = numero_controle_pncp
        if valor_total_estimado is not UNSET:
            field_dict["valorTotalEstimado"] = valor_total_estimado
        if modo_disputa_nome is not UNSET:
            field_dict["modoDisputaNome"] = modo_disputa_nome
        if tipo_instrumento_convocatorio_codigo is not UNSET:
            field_dict["tipoInstrumentoConvocatorioCodigo"] = tipo_instrumento_convocatorio_codigo
        if tipo_instrumento_convocatorio_nome is not UNSET:
            field_dict["tipoInstrumentoConvocatorioNome"] = tipo_instrumento_convocatorio_nome
        if fontes_orcamentarias is not UNSET:
            field_dict["fontesOrcamentarias"] = fontes_orcamentarias
        if situacao_compra_id is not UNSET:
            field_dict["situacaoCompraId"] = situacao_compra_id
        if situacao_compra_nome is not UNSET:
            field_dict["situacaoCompraNome"] = situacao_compra_nome
        if usuario_nome is not UNSET:
            field_dict["usuarioNome"] = usuario_nome
        if modalidade_nome is not UNSET:
            field_dict["modalidadeNome"] = modalidade_nome

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.contratacao_fonte_orcamentaria_dto import ContratacaoFonteOrcamentariaDTO
        from ..models.recuperar_amparo_legal_dto import RecuperarAmparoLegalDTO
        from ..models.recuperar_orgao_entidade_dto import RecuperarOrgaoEntidadeDTO
        from ..models.recuperar_unidade_orgao_dto import RecuperarUnidadeOrgaoDTO

        d = dict(src_dict)
        informacao_complementar = d.pop("informacaoComplementar", UNSET)

        processo = d.pop("processo", UNSET)

        objeto_compra = d.pop("objetoCompra", UNSET)

        link_sistema_origem = d.pop("linkSistemaOrigem", UNSET)

        justificativa_presencial = d.pop("justificativaPresencial", UNSET)

        _unidade_sub_rogada = d.pop("unidadeSubRogada", UNSET)
        unidade_sub_rogada: Union[Unset, RecuperarUnidadeOrgaoDTO]
        if isinstance(_unidade_sub_rogada, Unset):
            unidade_sub_rogada = UNSET
        else:
            unidade_sub_rogada = RecuperarUnidadeOrgaoDTO.from_dict(_unidade_sub_rogada)

        _orgao_sub_rogado = d.pop("orgaoSubRogado", UNSET)
        orgao_sub_rogado: Union[Unset, RecuperarOrgaoEntidadeDTO]
        if isinstance(_orgao_sub_rogado, Unset):
            orgao_sub_rogado = UNSET
        else:
            orgao_sub_rogado = RecuperarOrgaoEntidadeDTO.from_dict(_orgao_sub_rogado)

        valor_total_homologado = d.pop("valorTotalHomologado", UNSET)

        srp = d.pop("srp", UNSET)

        _amparo_legal = d.pop("amparoLegal", UNSET)
        amparo_legal: Union[Unset, RecuperarAmparoLegalDTO]
        if isinstance(_amparo_legal, Unset):
            amparo_legal = UNSET
        else:
            amparo_legal = RecuperarAmparoLegalDTO.from_dict(_amparo_legal)

        _data_abertura_proposta = d.pop("dataAberturaProposta", UNSET)
        data_abertura_proposta: Union[Unset, datetime.datetime]
        if isinstance(_data_abertura_proposta, Unset):
            data_abertura_proposta = UNSET
        else:
            data_abertura_proposta = isoparse(_data_abertura_proposta)

        _data_encerramento_proposta = d.pop("dataEncerramentoProposta", UNSET)
        data_encerramento_proposta: Union[Unset, datetime.datetime]
        if isinstance(_data_encerramento_proposta, Unset):
            data_encerramento_proposta = UNSET
        else:
            data_encerramento_proposta = isoparse(_data_encerramento_proposta)

        _orgao_entidade = d.pop("orgaoEntidade", UNSET)
        orgao_entidade: Union[Unset, RecuperarOrgaoEntidadeDTO]
        if isinstance(_orgao_entidade, Unset):
            orgao_entidade = UNSET
        else:
            orgao_entidade = RecuperarOrgaoEntidadeDTO.from_dict(_orgao_entidade)

        ano_compra = d.pop("anoCompra", UNSET)

        sequencial_compra = d.pop("sequencialCompra", UNSET)

        _data_inclusao = d.pop("dataInclusao", UNSET)
        data_inclusao: Union[Unset, datetime.datetime]
        if isinstance(_data_inclusao, Unset):
            data_inclusao = UNSET
        else:
            data_inclusao = isoparse(_data_inclusao)

        _data_publicacao_pncp = d.pop("dataPublicacaoPncp", UNSET)
        data_publicacao_pncp: Union[Unset, datetime.datetime]
        if isinstance(_data_publicacao_pncp, Unset):
            data_publicacao_pncp = UNSET
        else:
            data_publicacao_pncp = isoparse(_data_publicacao_pncp)

        _data_atualizacao = d.pop("dataAtualizacao", UNSET)
        data_atualizacao: Union[Unset, datetime.datetime]
        if isinstance(_data_atualizacao, Unset):
            data_atualizacao = UNSET
        else:
            data_atualizacao = isoparse(_data_atualizacao)

        numero_compra = d.pop("numeroCompra", UNSET)

        _unidade_orgao = d.pop("unidadeOrgao", UNSET)
        unidade_orgao: Union[Unset, RecuperarUnidadeOrgaoDTO]
        if isinstance(_unidade_orgao, Unset):
            unidade_orgao = UNSET
        else:
            unidade_orgao = RecuperarUnidadeOrgaoDTO.from_dict(_unidade_orgao)

        modo_disputa_id = d.pop("modoDisputaId", UNSET)

        modalidade_id = d.pop("modalidadeId", UNSET)

        _data_atualizacao_global = d.pop("dataAtualizacaoGlobal", UNSET)
        data_atualizacao_global: Union[Unset, datetime.datetime]
        if isinstance(_data_atualizacao_global, Unset):
            data_atualizacao_global = UNSET
        else:
            data_atualizacao_global = isoparse(_data_atualizacao_global)

        link_processo_eletronico = d.pop("linkProcessoEletronico", UNSET)

        numero_controle_pncp = d.pop("numeroControlePNCP", UNSET)

        valor_total_estimado = d.pop("valorTotalEstimado", UNSET)

        modo_disputa_nome = d.pop("modoDisputaNome", UNSET)

        tipo_instrumento_convocatorio_codigo = d.pop("tipoInstrumentoConvocatorioCodigo", UNSET)

        tipo_instrumento_convocatorio_nome = d.pop("tipoInstrumentoConvocatorioNome", UNSET)

        fontes_orcamentarias = []
        _fontes_orcamentarias = d.pop("fontesOrcamentarias", UNSET)
        for fontes_orcamentarias_item_data in _fontes_orcamentarias or []:
            fontes_orcamentarias_item = ContratacaoFonteOrcamentariaDTO.from_dict(fontes_orcamentarias_item_data)

            fontes_orcamentarias.append(fontes_orcamentarias_item)

        _situacao_compra_id = d.pop("situacaoCompraId", UNSET)
        situacao_compra_id: Union[Unset, RecuperarCompraPublicacaoDTOSituacaoCompraId]
        if isinstance(_situacao_compra_id, Unset):
            situacao_compra_id = UNSET
        else:
            situacao_compra_id = RecuperarCompraPublicacaoDTOSituacaoCompraId(_situacao_compra_id)

        situacao_compra_nome = d.pop("situacaoCompraNome", UNSET)

        usuario_nome = d.pop("usuarioNome", UNSET)

        modalidade_nome = d.pop("modalidadeNome", UNSET)

        recuperar_compra_publicacao_dto = cls(
            informacao_complementar=informacao_complementar,
            processo=processo,
            objeto_compra=objeto_compra,
            link_sistema_origem=link_sistema_origem,
            justificativa_presencial=justificativa_presencial,
            unidade_sub_rogada=unidade_sub_rogada,
            orgao_sub_rogado=orgao_sub_rogado,
            valor_total_homologado=valor_total_homologado,
            srp=srp,
            amparo_legal=amparo_legal,
            data_abertura_proposta=data_abertura_proposta,
            data_encerramento_proposta=data_encerramento_proposta,
            orgao_entidade=orgao_entidade,
            ano_compra=ano_compra,
            sequencial_compra=sequencial_compra,
            data_inclusao=data_inclusao,
            data_publicacao_pncp=data_publicacao_pncp,
            data_atualizacao=data_atualizacao,
            numero_compra=numero_compra,
            unidade_orgao=unidade_orgao,
            modo_disputa_id=modo_disputa_id,
            modalidade_id=modalidade_id,
            data_atualizacao_global=data_atualizacao_global,
            link_processo_eletronico=link_processo_eletronico,
            numero_controle_pncp=numero_controle_pncp,
            valor_total_estimado=valor_total_estimado,
            modo_disputa_nome=modo_disputa_nome,
            tipo_instrumento_convocatorio_codigo=tipo_instrumento_convocatorio_codigo,
            tipo_instrumento_convocatorio_nome=tipo_instrumento_convocatorio_nome,
            fontes_orcamentarias=fontes_orcamentarias,
            situacao_compra_id=situacao_compra_id,
            situacao_compra_nome=situacao_compra_nome,
            usuario_nome=usuario_nome,
            modalidade_nome=modalidade_nome,
        )

        recuperar_compra_publicacao_dto.additional_properties = d
        return recuperar_compra_publicacao_dto

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
