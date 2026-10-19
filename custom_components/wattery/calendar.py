"""Kalendář plánu baterie (0.6.0, A9): okna nákupu, prodeje, držení baterie a omezení přetoku jako události.

Automatizace se spouští triggerem kalendáře (začátek / konec okna, i s předstihem) bez šablon; `description`
události nese strojově čitelné `action`, `power_w`, `target_soc`. Okna samospotřeby se nezobrazují.
"""
from __future__ import annotations

import time
from datetime import datetime

from homeassistant.components.calendar import CalendarEntity, CalendarEvent
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.util import dt as dt_util

from . import WatteryConfigEntry
from .plan_entity import PlanEntity

TEXT = {
    'cs': {'charge_grid': 'Nabíjení ze sítě', 'discharge_grid': 'Prodej z baterie',
           'hold': 'Podržet baterii (spotřeba ze sítě)', 'no_export': 'Bez přetoku do sítě (záporná cena)'},
    'en': {'charge_grid': 'Grid charging', 'discharge_grid': 'Battery export',
           'hold': 'Hold battery (load from grid)', 'no_export': 'No grid export (negative price)'},
}


def summary(w: dict, lang: str) -> str:
    """„Nabíjení ze sítě 5 kW → 80 %“ / „Grid charging 5 kW → 80%“."""
    cs = lang == 'cs'
    out = TEXT[lang][w['action']]
    if w['action'] in ('charge_grid', 'discharge_grid') and w.get('powerW'):
        kw = f"{w['powerW'] / 1000:.1f}".rstrip('0').rstrip('.')
        out += f" {kw.replace('.', ',') if cs else kw} kW"
    if w['action'] in ('charge_grid', 'discharge_grid') and w.get('socEnd') is not None:
        out += f" → {round(w['socEnd'])}{' %' if cs else '%'}"
    return out


async def async_setup_entry(hass: HomeAssistant, entry: WatteryConfigEntry,
                            async_add_entities: AddEntitiesCallback) -> None:
    async_add_entities([WatteryPlanCalendar(entry)])


class WatteryPlanCalendar(PlanEntity, CalendarEntity):
    _attr_translation_key = 'plan'
    _attr_icon = 'mdi:calendar-clock'

    def __init__(self, entry: WatteryConfigEntry) -> None:
        super().__init__(entry, 'plan_calendar')

    def _events(self) -> list[CalendarEvent]:
        p = self.plan
        if not p or time.time() >= p['validUntil']:
            return []
        lang = 'cs' if (self.hass.config.language or '').lower().startswith('cs') else 'en'
        return [CalendarEvent(start=datetime.fromisoformat(w['from']), end=datetime.fromisoformat(w['to']),
                              summary=summary(w, lang), uid=f"{p['id']}-{w['fromTs']}",
                              description=f"action: {w['action']}\npower_w: {w['powerW']}\ntarget_soc: {w['socEnd']}")
                for w in p['windows'] if w['action'] != 'self_use']

    @property
    def event(self) -> CalendarEvent | None:
        """Probíhající událost, jinak nejbližší příští."""
        now = dt_util.now()
        evs = [e for e in self._events() if e.end > now]
        return min(evs, key=lambda e: e.start) if evs else None

    async def async_get_events(self, hass: HomeAssistant, start_date: datetime,
                               end_date: datetime) -> list[CalendarEvent]:
        return [e for e in self._events() if e.end > start_date and e.start < end_date]
