from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    data_inicial: str,
    data_final: str,
    cnpj_orgao: Union[Unset, str] = UNSET,
    codigo_unidade_administrativa: Union[Unset, str] = UNSET,
    usuario_id: Union[Unset, int] = UNSET,
    pagina: int,
    tamanho_pagina: Union[Unset, int] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["dataInicial"] = data_inicial

    params["dataFinal"] = data_final

    params["cnpjOrgao"] = cnpj_orgao

    params["codigoUnidadeAdministrativa"] = codigo_unidade_administrativa

    params["usuarioId"] = usuario_id

    params["pagina"] = pagina

    params["tamanhoPagina"] = tamanho_pagina

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/contratos",
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
    data_inicial: str,
    data_final: str,
    cnpj_orgao: Union[Unset, str] = UNSET,
    codigo_unidade_administrativa: Union[Unset, str] = UNSET,
    usuario_id: Union[Unset, int] = UNSET,
    pagina: int,
    tamanho_pagina: Union[Unset, int] = UNSET,
) -> Response[Any]:
    """Consultar Contratos por Data de Publicação

    Args:
        data_inicial (str):
        data_final (str):
        cnpj_orgao (Union[Unset, str]):
        codigo_unidade_administrativa (Union[Unset, str]):
        usuario_id (Union[Unset, int]):
        pagina (int):
        tamanho_pagina (Union[Unset, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        data_inicial=data_inicial,
        data_final=data_final,
        cnpj_orgao=cnpj_orgao,
        codigo_unidade_administrativa=codigo_unidade_administrativa,
        usuario_id=usuario_id,
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
    data_inicial: str,
    data_final: str,
    cnpj_orgao: Union[Unset, str] = UNSET,
    codigo_unidade_administrativa: Union[Unset, str] = UNSET,
    usuario_id: Union[Unset, int] = UNSET,
    pagina: int,
    tamanho_pagina: Union[Unset, int] = UNSET,
) -> Response[Any]:
    """Consultar Contratos por Data de Publicação

    Args:
        data_inicial (str):
        data_final (str):
        cnpj_orgao (Union[Unset, str]):
        codigo_unidade_administrativa (Union[Unset, str]):
        usuario_id (Union[Unset, int]):
        pagina (int):
        tamanho_pagina (Union[Unset, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        data_inicial=data_inicial,
        data_final=data_final,
        cnpj_orgao=cnpj_orgao,
        codigo_unidade_administrativa=codigo_unidade_administrativa,
        usuario_id=usuario_id,
        pagina=pagina,
        tamanho_pagina=tamanho_pagina,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
