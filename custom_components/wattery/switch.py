"""Přepínač „Řízení z wattery“: smí wattery přes tento Home Assistant zapisovat do střídače (výchozí vypnuto).

Vypnutý = Home Assistant jen čte; wattery řídí jako dřív přes Deye cloud. Stav přežije restart (RestoreEntity).
"""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from . import WatteryConfigEntry, device_info
from .const import CONF_SN

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: WatteryConfigEntry,
                            async_add_entities: AddEntitiesCallback) -> None:
    async_add_entities([WatteryControlSwitch(entry)])


class WatteryControlSwitch(SwitchEntity, RestoreEntity):
    _attr_has_entity_name = True
    _attr_translation_key = 'control'
    _attr_entity_category = EntityCategory.CONFIG
    _attr_should_poll = False
    _attr_icon = 'mdi:remote'

    def __init__(self, entry: WatteryConfigEntry) -> None:
        self._entry = entry
        sn = entry.data[CONF_SN]
        self._attr_unique_id = f'{sn}_control'
        self._attr_device_info = device_info(sn)

    @property
    def _runner(self):
        return self._entry.runtime_data.runner

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        last = await self.async_get_last_state()
        self._runner.set_control(bool(last and last.state == 'on'))
        if self._runner.control_enabled:
            _LOGGER.info('wattery: řízení z wattery zapnuté (obnoveno po restartu)')

    @property
    def is_on(self) -> bool:
        return self._runner.control_enabled

    async def async_turn_on(self, **kwargs: Any) -> None:
        self._runner.set_control(True)
        _LOGGER.info('wattery: řízení z wattery zapnuto')
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        self._runner.set_control(False)
        _LOGGER.info('wattery: řízení z wattery vypnuto — zapisuje se jen přes Deye cloud')
        self.async_write_ha_state()
