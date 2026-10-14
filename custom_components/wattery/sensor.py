"""Senzor stavu lokálního spojení (online / chyba / odpojeno)."""
from __future__ import annotations

from datetime import datetime, timezone

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import WatteryConfigEntry
from .const import CONF_SN, DOMAIN, VERSION
from .edge import ERROR, ONLINE, REVOKED, STARTING


async def async_setup_entry(hass: HomeAssistant, entry: WatteryConfigEntry,
                            async_add_entities: AddEntitiesCallback) -> None:
    async_add_entities([WatteryStatusSensor(entry)])


class WatteryStatusSensor(SensorEntity):
    _attr_has_entity_name = True
    _attr_translation_key = 'status'
    _attr_device_class = SensorDeviceClass.ENUM
    _attr_options = [STARTING, ONLINE, ERROR, REVOKED]
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_should_poll = False
    _attr_icon = 'mdi:link-variant'

    def __init__(self, entry: WatteryConfigEntry) -> None:
        self._entry = entry
        sn = entry.data[CONF_SN]
        self._attr_unique_id = f'{sn}_status'
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, sn)}, name=f'wattery {sn}', manufacturer='wattery',
                                            model='Lokální spojení', sw_version=VERSION,
                                            configuration_url='https://monitoring.wattery.cz')

    async def async_added_to_hass(self) -> None:
        listeners = self._entry.runtime_data.listeners
        listeners.append(self.async_write_ha_state)
        self.async_on_remove(lambda: listeners.remove(self.async_write_ha_state))

    @property
    def native_value(self) -> str:
        return self._entry.runtime_data.runner.status

    @property
    def extra_state_attributes(self) -> dict:
        r = self._entry.runtime_data.runner
        return {
            'last_sync': datetime.fromtimestamp(r.last_sync_at, timezone.utc).isoformat() if r.last_sync_at else None,
            'last_error': r.last_error,
            'read_errors': r.read_errors,
            'blocks': [f"{b['name']} {b['addr']}/{b['count']} / {b['everyS']} s" for b in r.plan],
        }
