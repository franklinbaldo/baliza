from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    ano_pca: int,
    id_usuario: int,
    codigo_classificacao_superior: Union[Unset, str] = UNSET,
    cnpj: Union[Unset, str] = UNSET,
    pagina: int,
    tamanho_pagina: Union[Unset, int] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["anoPca"] = ano_pca

    params["idUsuario"] = id_usuario

    params["codigoClassificacaoSuperior"] = codigo_classificacao_superior

    params["cnpj"] = cnpj

    params["pagina"] = pagina

    params["tamanhoPagina"] = tamanho_pagina

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/pca/usuario",
        "params": params,
    }

    return _kwargs


def _parse_response(*, client: Union[AuthenticatedClient, Client], response: httpx.Response) -> Optional[Any]:
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: Union[AuthenticatedClient, Client], response: httpx.Response) -> Response[Any]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    ano_pca: int,
    id_usuario: int,
    codigo_classificacao_superior: Union[Unset, str] = UNSET,
    cnpj: Union[Unset, str] = UNSET,
    pagina: int,
    tamanho_pagina: Union[Unset, int] = UNSET,
) -> Response[Any]:
    """Consultar Itens de PCA por Ano do PCA, IdUsuario e Código de Classificação Superior

    Args:
        ano_pca (int):
        id_usuario (int):
        codigo_classificacao_superior (Union[Unset, str]):
        cnpj (Union[Unset, str]):
        pagina (int):
        tamanho_pagina (Union[Unset, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        ano_pca=ano_pca,
        id_usuario=id_usuario,
        codigo_classificacao_superior=codigo_classificacao_superior,
        cnpj=cnpj,
        pagina=pagina,
        tamanho_pagina=tamanho_pagina,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    ano_pca: int,
    id_usuario: int,
    codigo_classificacao_superior: Union[Unset, str] = UNSET,
    cnpj: Union[Unset, str] = UNSET,
    pagina: int,
    tamanho_pagina: Union[Unset, int] = UNSET,
) -> Response[Any]:
    """Consultar Itens de PCA por Ano do PCA, IdUsuario e Código de Classificação Superior

    Args:
        ano_pca (int):
        id_usuario (int):
        codigo_classificacao_superior (Union[Unset, str]):
        cnpj (Union[Unset, str]):
        pagina (int):
        tamanho_pagina (Union[Unset, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        ano_pca=ano_pca,
        id_usuario=id_usuario,
        codigo_classificacao_superior=codigo_classificacao_superior,
        cnpj=cnpj,
        pagina=pagina,
        tamanho_pagina=tamanho_pagina,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
