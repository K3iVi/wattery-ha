"""Přenos Modbus přes integraci ha-solarman (Deye): čtení bloků, zápis FC16 a sériové číslo střídače.

Rozhraní přenosu Modbus (stejné pro další, např. Modbus TCP u Solaxu): `read_block(fc, addr, count)` → hodnoty u16,
`write_block(addr, values)` (chyba = výjimka) a `identity()` → identita zařízení pro párování (SN), nebo None.
"""
from __future__ import annotations

import asyncio

from homeassistant.core import HomeAssistant

from .const import SN_BLOCK, SOLARMAN
from .edge import decode_sn

_SERVICE = {3: 'read_holding_registers', 4: 'read_input_registers'}
_WRITE = 'write_multiple_registers'        # FC16 — Deye bere zápis jen takhle, i pro jeden registr
SOLARMAN_TIMEOUT_S = 20                    # zaseknuté čtení / zápis nesmí zastavit smyčku (příkazy čekají)


def solarman_ready(hass: HomeAssistant) -> bool:
    return hass.services.has_service(SOLARMAN, _SERVICE[3])


def make_reader(hass: HomeAssistant, device_id: str):
    async def read_block(fc: int, addr: int, count: int) -> list:
        async with asyncio.timeout(SOLARMAN_TIMEOUT_S):
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


def make_writer(hass: HomeAssistant, device_id: str):
    """Zápis bloku registrů (FC16) přes ha-solarman. Chyba = výjimka (server pak příkaz pošle přes Deye cloud)."""
    async def write_block(addr: int, values: list) -> None:
        if not hass.services.has_service(SOLARMAN, _WRITE):
            raise RuntimeError(f'ha-solarman nemá službu {_WRITE}')
        async with asyncio.timeout(SOLARMAN_TIMEOUT_S):
            await hass.services.async_call(
                SOLARMAN, _WRITE, {'device': device_id, 'address': addr, 'values': [int(v) for v in values]},
                blocking=True)
    return write_block


async def read_sn(hass: HomeAssistant, device_id: str) -> str | None:
    fc, addr, count = SN_BLOCK
    return decode_sn(await make_reader(hass, device_id)(fc, addr, count))


class SolarmanModbus:
    """Střídač Deye za zařízením ha-solarman (device_id z registru zařízení HA)."""

    def __init__(self, hass: HomeAssistant, device_id: str):
        self.device_id = device_id
        self.read_block = make_reader(hass, device_id)
        self.write_block = make_writer(hass, device_id)
        self._hass = hass

    async def identity(self) -> str | None:
        return await read_sn(self._hass, self.device_id)
