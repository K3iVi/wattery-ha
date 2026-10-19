"""Plán baterie jako data (0.6.0, A9): společný základ senzoru a kalendáře.

Plán drží EdgeRunner (`battery_plan`, z odpovědi sync; uložený v HA Store, platí do `valid_until`). Entity se
obnoví při každém sync a navíc přesně na hranici čtvrthodiny / konce platnosti, ať automatizace vidí změnu akce
včas i bez dalšího sync. Po `valid_until` bez nového plánu jsou nedostupné (návod: nedostupné = samospotřeba).
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from homeassistant.core import CALLBACK_TYPE, callback
from homeassistant.helpers.event import async_track_point_in_utc_time

from . import WatteryConfigEntry, device_info
from .const import CONF_SN
from .edge import PLAN_SLOT_S, plan_slot


def iso(ts) -> str | None:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat() if ts is not None else None


class PlanEntity:
    """Mixin před třídou entity HA (SensorEntity, CalendarEntity)."""
    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, entry: WatteryConfigEntry, suffix: str) -> None:
        self._entry = entry
        sn = entry.data[CONF_SN]
        self._attr_unique_id = f'{sn}_{suffix}'
        self._attr_device_info = device_info(sn)
        self._unsub_timer: CALLBACK_TYPE | None = None

    @property
    def plan(self) -> dict | None:
        return self._entry.runtime_data.runner.battery_plan

    @property
    def available(self) -> bool:
        return plan_slot(self.plan, time.time()) is not None

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        listeners = self._entry.runtime_data.listeners
        listeners.append(self._refresh)
        self.async_on_remove(lambda: listeners.remove(self._refresh))
        self.async_on_remove(self._cancel_timer)
        self._arm()

    @callback
    def _refresh(self) -> None:
        self._arm()
        self.async_write_ha_state()

    @callback
    def _cancel_timer(self) -> None:
        if self._unsub_timer:
            self._unsub_timer()
            self._unsub_timer = None

    @callback
    def _arm(self) -> None:
        """Časovač na příští změnu: začátek dalšího slotu, nejdál konec platnosti plánu."""
        self._cancel_timer()
        p, now = self.plan, time.time()
        i = plan_slot(p, now)
        if i is not None:
            t = p['slots']['t']
            nxt = min(t[i + 1] if i + 1 < len(t) else t[-1] + PLAN_SLOT_S, p['validUntil'])
        elif p and now < p['slots']['t'][0] < p['validUntil']:
            nxt = p['slots']['t'][0]               # plán začíná až za chvíli
        else:
            return
        self._unsub_timer = async_track_point_in_utc_time(
            self.hass, self._on_timer, datetime.fromtimestamp(nxt + 0.5, timezone.utc))

    @callback
    def _on_timer(self, _now) -> None:
        self._unsub_timer = None
        self._refresh()
