"""Spojení s Home Assistantem: čtení přes ha-solarman a HTTP na server wattery."""
from __future__ import annotations

import asyncio

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import CLIENT_VER, SERVER_URL, SN_BLOCK, SOLARMAN
from .edge import PAIR_PATH, decode_sn

_SERVICE = {3: 'read_holding_registers', 4: 'read_input_registers'}
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)


def solarman_ready(hass: HomeAssistant) -> bool:
    return hass.services.has_service(SOLARMAN, _SERVICE[3])


def make_reader(hass: HomeAssistant, device_id: str):
    async def read_block(fc: int, addr: int, count: int) -> list:
        resp = await hass.services.async_call(
            SOLARMAN, _SERVICE[fc], {'device': device_id, 'address': addr, 'count': count},
            blocking=True, return_response=True)
        if not isinstance(resp, dict):
            raise ValueError('ha-solarman nevrátil hodnoty')
        regs = {int(k): v for k, v in resp.items()}
        missing = [a for a in range(addr, addr + count) if a not in regs]
        if missing:
            raise ValueError(f'chybí registry {missing[:3]}…')
        return [int(regs[a]) for a in range(addr, addr + count)]
    return read_block


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


async def read_sn(hass: HomeAssistant, device_id: str) -> str | None:
    fc, addr, count = SN_BLOCK
    return decode_sn(await make_reader(hass, device_id)(fc, addr, count))


async def pair(hass: HomeAssistant, code: str, sn: str) -> tuple:
    """→ (http kód, odpověď); výjimka = server nedostupný."""
    body = {'code': code.strip().upper(), 'kind': 'ha', 'sn': sn, 'clientVer': CLIENT_VER,
            'label': 'Home Assistant'}
    try:
        return await make_post(hass)(PAIR_PATH, body)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        raise ConnectionError(str(e)) from e
