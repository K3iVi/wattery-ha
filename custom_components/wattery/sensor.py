"""Senzory: stav lokálního spojení (online / chyba / odpojeno) a plán baterie (0.6.0, A9 — akce teď + atributy)."""
from __future__ import annotations

import time
from datetime import datetime, timezone

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import WatteryConfigEntry, device_info
from .const import CONF_SN
from .edge import ERROR, ONLINE, PLAN_ACTIONS, REVOKED, STARTING, plan_slot, plan_window
from .plan_entity import PlanEntity, iso

PLAN_NEXT = 6                 # kolik dalších oken v atributu `next`


async def async_setup_entry(hass: HomeAssistant, entry: WatteryConfigEntry,
                            async_add_entities: AddEntitiesCallback) -> None:
    async_add_entities([WatteryStatusSensor(entry), WatteryPlanSensor(entry)])


class WatteryPlanSensor(PlanEntity, SensorEntity):
    """Co má baterie podle plánu wattery dělat teď (charge_grid / discharge_grid / hold / no_export / self_use).
    Jen informace pro automatizace — zapisovat do střídače smí jen přepínač „Řízení z wattery“."""
    _attr_translation_key = 'plan'
    _attr_device_class = SensorDeviceClass.ENUM
    _attr_options = list(PLAN_ACTIONS)
    _attr_icon = 'mdi:battery-clock'
    _unrecorded_attributes = frozenset({'next', 'valid_until', 'computed_at'})

    def __init__(self, entry: WatteryConfigEntry) -> None:
        super().__init__(entry, 'plan')

    @property
    def native_value(self) -> str | None:
        p = self.plan
        i = plan_slot(p, time.time())
        return p['slots']['action'][i] if i is not None else None

    @property
    def extra_state_attributes(self) -> dict:
        p, now = self.plan, time.time()
        i = plan_slot(p, now)
        if i is None:
            return {}
        wi = plan_window(p, now)
        w = p['windows'][wi] if wi is not None else None
        sl = p['slots']
        return {
            'until': w['to'] if w else None,               # konec akce (okna se stejnou akcí)
            'power_w': sl['powerW'][i],                    # výkon nákupu do / prodeje z baterie (W, celá elektrárna)
            'target_soc': w['socEnd'] if w else None,      # SOC na konci okna
            'reserve_soc': p['reservePct'],
            'price_buy': sl['buy'][i], 'price_sell': sl['sell'][i], 'currency': p['currency'],
            'valid_until': iso(p['validUntil']), 'computed_at': iso(p['computedAt']),
            'next': [{'start': x['from'], 'end': x['to'], 'action': x['action'], 'power_w': x['powerW'],
                      'target_soc': x['socEnd']} for x in p['windows'][wi + 1:wi + 1 + PLAN_NEXT]] if w else [],
        }


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
        self._attr_device_info = device_info(sn)

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
            'control': r.control_enabled,
            'server_control': r.server_control,
            'commands': r.commands_done,
            'last_command': r.last_command,
            # offline pojistka: do kdy platí plán ve střídači bez spojení se serverem, kdy se naposled zapsal neutrál
            'fallback_until': (datetime.fromtimestamp(r.fallback['deadline'], timezone.utc).isoformat()
                               if r.fallback else None),
            'fallback_applied': (datetime.fromtimestamp(r.fallback_applied_at, timezone.utc).isoformat()
                                 if r.fallback_applied_at else None),
            # protokol 2: profil zápisu od serveru a pojistky příkazů (obnova, do kdy platí)
            'profile': r.profile_id,
            'leases': [f"{x['id']} do {datetime.fromtimestamp(x['deadline'], timezone.utc).isoformat()}"
                       for x in r.leases.values()],
        }
