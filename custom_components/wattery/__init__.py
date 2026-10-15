"""wattery — lokální spojení: Home Assistant čte střídač přes ha-solarman a posílá data do wattery.

Server (monitoring.wattery.cz) určuje, které registry a jak často číst. Se zapnutým přepínačem „Řízení z wattery“
(výchozí vypnuto) provádí i příkazy platformy (okna TOU, proudy, max. prodej…) místo přes Deye cloud — jen registry
z povoleného profilu a se zpětným čtením (edge.py).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo

from . import api
from .const import BACKEND, CLIENT_VER, CONF_DEVICE, CONF_READ_PLAN, CONF_SN, CONF_TOKEN, DOMAIN, VERSION
from .edge import EdgeRunner, Revoked

_LOGGER = logging.getLogger(__name__)
PLATFORMS = [Platform.SENSOR, Platform.SWITCH]


@dataclass
class WatteryRuntime:
    runner: EdgeRunner
    listeners: list[Callable[[], None]] = field(default_factory=list)


type WatteryConfigEntry = ConfigEntry[WatteryRuntime]


def device_info(sn: str) -> DeviceInfo:
    return DeviceInfo(identifiers={(DOMAIN, sn)}, name=f'wattery {sn}', manufacturer='wattery',
                      model='Lokální spojení', sw_version=VERSION, configuration_url='https://monitoring.wattery.cz')


async def async_setup_entry(hass: HomeAssistant, entry: WatteryConfigEntry) -> bool:
    rt: WatteryRuntime

    def on_status(_runner: EdgeRunner) -> None:
        for cb in list(rt.listeners):
            cb()

    device = entry.data[CONF_DEVICE]
    runner = EdgeRunner(read_block=api.make_reader(hass, device), write_block=api.make_writer(hass, device),
                        post=api.make_post(hass), token=entry.data[CONF_TOKEN], sn=entry.data[CONF_SN],
                        read_plan=entry.data.get(CONF_READ_PLAN) or [], version=CLIENT_VER, backend=BACKEND,
                        on_status=on_status)
    rt = WatteryRuntime(runner=runner)
    entry.runtime_data = rt
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    async def loop() -> None:
        try:
            await runner.run()
        except Revoked:
            _LOGGER.warning('wattery: spojení bylo ve wattery odpojeno — spárujte znovu')
            entry.async_start_reauth(hass)

    entry.async_create_background_task(hass, loop(), 'wattery-edge')
    return True


async def async_unload_entry(hass: HomeAssistant, entry: WatteryConfigEntry) -> bool:
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
