"""Spojení se serverem wattery (HTTP) a párování. Čtení / zápis střídače je v přenosech (`transport_*.py`)."""
from __future__ import annotations

import asyncio

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import CLIENT_VER, SERVER_URL
from .edge import PAIR_PATH, client_caps
from .transport_solarman import make_reader, make_writer, read_sn, solarman_ready  # noqa: F401 — dřívější API

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)


def make_post(hass: HomeAssistant, base_url: str = SERVER_URL):
    session = async_get_clientsession(hass)

    async def post(path: str, body: dict, token: str | None = None) -> tuple:
        headers = {'Authorization': f'Bearer {token}'} if token else {}
        async with session.post(base_url + path, json=body, headers=headers, timeout=HTTP_TIMEOUT) as r:
            try:
                data = await r.json(content_type=None)
            except (aiohttp.ContentTypeError, ValueError):
                data = {}
            return r.status, data
    return post


async def pair(hass: HomeAssistant, code: str, sn: str, caps: dict | None = None) -> tuple:
    """→ (http kód, odpověď); výjimka = server nedostupný. `sn` = identita, kterou klient přečetl ze zařízení
    (SN střídače Deye, portal ID GX); `caps` podle připojení (výchozí Deye přes ha-solarman)."""
    body = {'code': code.strip().upper(), 'kind': 'ha', 'sn': sn, 'clientVer': CLIENT_VER,
            'label': 'Home Assistant', 'caps': caps or client_caps()}
    try:
        return await make_post(hass)(PAIR_PATH, body)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        raise ConnectionError(str(e)) from e
