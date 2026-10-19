"""wattery — lokální spojení: Home Assistant čte zařízení v místní síti a posílá data do wattery.

Server (monitoring.wattery.cz) určuje, co a jak často číst. Se zapnutým přepínačem „Řízení z wattery“
(výchozí vypnuto) provádí i příkazy platformy (okna TOU, proudy, max. prodej…) místo přes cloud výrobce — jen
v rámci povoleného profilu a se zpětným čtením (edge.py). Bez spojení se serverem po platnosti plánu zapíše jednou
neutrální rozvrh (offline pojistka, stav v HA Store). Od 0.4.0 posílá serveru, co umí (`caps`, protokol 2).

Připojení (od 0.5.0, jedna položka = jedno připojení):
- Deye přes integraci ha-solarman (Modbus; čtení i řízení) — položky z 0.4 a starších;
- Victron GX přímo přes MQTT v místní síti (`transport_mqtt_gx`; zatím jen čtení);
- jen čtení z entit integrace, kterou zákazník už má (`transport_ha_entities`, např. victron_gx) — bez přepínače
  řízení, žádné druhé spojení na zařízení.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.storage import Store

from . import api
from .const import (BACKENDS, CLIENT_VER, CONF_CERT, CONF_CONN, CONF_DEVICE, CONF_HOST, CONF_PASSWORD, CONF_PORT,
                    CONF_READ_PLAN, CONF_SN, CONF_SOURCE, CONF_SOURCE_DOMAIN, CONF_TLS, CONF_TOKEN, CONN_ENTITIES,
                    CONN_GX, CONN_SOLARMAN, DOMAIN, SOLARMAN, VERSION)
from .edge import EdgeRunner, Revoked
from .transport_ha_entities import HaEntities, hub_prefix
from .transport_mqtt_gx import GxMqtt
from .transport_solarman import SolarmanModbus

_LOGGER = logging.getLogger(__name__)
PLATFORMS = [Platform.SENSOR, Platform.SWITCH]
READ_ONLY_PLATFORMS = [Platform.SENSOR]      # entity jiné integrace: řídit se přes ně nedá → bez přepínače
STORAGE_VERSION = 1
GX_RETRY_MIN_S, GX_RETRY_MAX_S = 10, 300     # GX nedostupné → zkusit znovu (zdvojovat do 5 min)


@dataclass
class WatteryRuntime:
    runner: EdgeRunner
    listeners: list[Callable[[], None]] = field(default_factory=list)


type WatteryConfigEntry = ConfigEntry[WatteryRuntime]


def device_info(sn: str) -> DeviceInfo:
    return DeviceInfo(identifiers={(DOMAIN, sn)}, name=f'wattery {sn}', manufacturer='wattery',
                      model='Lokální spojení', sw_version=VERSION, configuration_url='https://monitoring.wattery.cz')


def conn_of(entry: ConfigEntry) -> str:
    return entry.data.get(CONF_CONN) or CONN_SOLARMAN


def platforms_of(entry: ConfigEntry) -> list:
    return READ_ONLY_PLATFORMS if conn_of(entry) == CONN_ENTITIES else PLATFORMS


def gx_client(data: dict) -> GxMqtt:
    return GxMqtt(data[CONF_HOST], data[CONF_PORT], tls=data.get(CONF_TLS, True),
                  password=data.get(CONF_PASSWORD) or '', portal=data.get(CONF_SN), cert_sha256=data.get(CONF_CERT))


async def _gx_loop(gx) -> None:
    """Drží spojení na GX (MQTT); po chybě se připojí znovu. Hodnoty čte EdgeRunner přes `read_item`."""
    delay, last = GX_RETRY_MIN_S, None
    while True:
        try:
            await gx.start()
            if last:
                _LOGGER.info('wattery: GX znovu připojeno')
            delay, last = GX_RETRY_MIN_S, None
            await gx.run()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — GxError, síť, TLS; zkusit znovu
            if str(e) != last:
                _LOGGER.warning('wattery: spojení s GX: %s', e)
            last = str(e)
        finally:
            await gx.close()
        await asyncio.sleep(delay)
        delay = min(GX_RETRY_MAX_S, delay * 2)


async def async_setup_entry(hass: HomeAssistant, entry: WatteryConfigEntry) -> bool:
    rt: WatteryRuntime

    def on_status(_runner: EdgeRunner) -> None:
        for cb in list(rt.listeners):
            cb()

    conn, data = conn_of(entry), entry.data
    read_block = write_block = gx = None
    transports: dict = {}
    integrations: tuple = ()
    if conn == CONN_SOLARMAN:
        dev = SolarmanModbus(hass, data[CONF_DEVICE])
        read_block, write_block, integrations = dev.read_block, dev.write_block, (SOLARMAN,)
    elif conn == CONN_GX:
        gx = transports['mqtt'] = gx_client(data)
    elif conn == CONN_ENTITIES:
        src = hass.config_entries.async_get_entry(data[CONF_SOURCE])
        transports['ha_entity'] = HaEntities(hass, data[CONF_SOURCE_DOMAIN], data[CONF_SOURCE],
                                             hub_prefix(src) if src else None)
        integrations = (data[CONF_SOURCE_DOMAIN],)
    else:
        _LOGGER.error('wattery: neznámý typ připojení %s', conn)
        return False

    # Offline pojistka (neutrální rozvrh + do kdy platí plán) musí přežít restart HA — výpadek může trvat déle.
    store: Store = Store(hass, STORAGE_VERSION, f'{DOMAIN}.{entry.entry_id}')
    state = await store.async_load() or {}

    def on_persist(st: dict) -> None:
        store.async_delay_save(lambda: st, 1)

    runner = EdgeRunner(read_block=read_block, write_block=write_block, post=api.make_post(hass),
                        token=data[CONF_TOKEN], sn=data[CONF_SN], read_plan=data.get(CONF_READ_PLAN) or [],
                        version=CLIENT_VER, backend=BACKENDS[conn], on_status=on_status, state=state,
                        on_persist=on_persist, transports=transports, integrations=integrations)
    rt = WatteryRuntime(runner=runner)
    entry.runtime_data = rt
    await hass.config_entries.async_forward_entry_setups(entry, platforms_of(entry))

    async def loop() -> None:
        try:
            await runner.run()
        except Revoked:
            _LOGGER.warning('wattery: spojení bylo ve wattery odpojeno — spárujte znovu')
            entry.async_start_reauth(hass)

    if gx is not None:
        entry.async_create_background_task(hass, _gx_loop(gx), 'wattery-gx')
    entry.async_create_background_task(hass, loop(), 'wattery-edge')
    return True


async def async_unload_entry(hass: HomeAssistant, entry: WatteryConfigEntry) -> bool:
    return await hass.config_entries.async_unload_platforms(entry, platforms_of(entry))
