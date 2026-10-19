"""Přenos `ha_entity` (jen čtení): stavy entit integrace, kterou zákazník už má (victron_gx, solax_modbus …).

Žádné druhé spojení na střídač — nic se nepere o dongle ani sběrnici; data jsou tak čerstvá, jak často čte jejich
integrace. Klient je hloupý: pošle entity zvolené položky (config entry) té integrace, mapování na klíče dělá
adaptér na serveru (`edge_victron.ha_entities_to_values`, `edge_solax.decode_ha_entities`).

Položka plánu `{type: 'ha_entity', name, integration, everyS}` → `{values: {entity_id: {state, unit, uid}}, prefix?}`
(uid = unique_id z registru entit — entity_id si uživatel může přejmenovat, unique_id ne).
"""
from __future__ import annotations

import json
import logging
import re

from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

_LOGGER = logging.getLogger(__name__)

ENTITY_DOMAINS = ('sensor', 'binary_sensor', 'number')   # hodnoty; select / switch / button nic nečtou
MAX_ENTITIES = 500             # stejně jako server (HA_MAX_ENTITIES)
MAX_BYTES = 44 * 1024          # položka se musí vejít pod strop jednoho sync (edge.READS_BUDGET 48 kB)
_PREFIX_KEY = {'solax_modbus': 'name'}   # název hubu v nastavení integrace = začátek unique_id jejích entit
_VICTRON_PORTAL = re.compile(r'^([0-9a-f]{12})_')


def hub_prefix(entry) -> str | None:
    """Název hubu z nastavení integrace (víc střídačů v jednom HA → server vezme jen entity toho hubu)."""
    key = _PREFIX_KEY.get(entry.domain)
    v = (entry.options.get(key) or entry.data.get(key)) if key else None
    return str(v)[:60] if isinstance(v, str) and v.strip() else None


class HaEntities:
    """Entity jedné položky (config entry) integrace `domain`."""

    def __init__(self, hass: HomeAssistant, domain: str, entry_id: str, prefix: str | None = None):
        self._hass = hass
        self.domain, self.entry_id, self.prefix = domain, entry_id, prefix
        self._warned = False

    def entities(self) -> list:
        reg = er.async_get(self._hass)
        out = [e for e in er.async_entries_for_config_entry(reg, self.entry_id)
               if e.platform == self.domain and e.domain in ENTITY_DOMAINS and not e.disabled_by]
        return sorted(out, key=lambda e: e.entity_id)

    def snapshot(self) -> dict:
        vals, size = {}, 0
        for e in self.entities():
            st = self._hass.states.get(e.entity_id)
            if st is None or st.state in (STATE_UNAVAILABLE, STATE_UNKNOWN, ''):
                continue
            unit = st.attributes.get('unit_of_measurement')
            rec = {'state': st.state[:60], 'unit': str(unit)[:30] if unit else None, 'uid': str(e.unique_id)[:200]}
            size += len(e.entity_id) + len(json.dumps(rec)) + 6
            if len(vals) >= MAX_ENTITIES or size > MAX_BYTES:
                if not self._warned:
                    _LOGGER.warning('wattery: integrace %s má víc entit, než se vejde do jednoho přenosu — '
                                    'posílám prvních %s', self.domain, len(vals))
                    self._warned = True
                break
            vals[e.entity_id] = rec
        return vals

    def read_item(self, item: dict) -> dict | None:
        if str(item.get('integration') or self.domain) != self.domain:
            return None                              # položka pro jinou integraci, než má tahle položka HA
        out = {'values': self.snapshot()}
        if self.prefix:
            out['prefix'] = self.prefix
        return out

    def identity(self) -> str | None:
        """Identita zařízení pro párování z unique_id entit (jen kde ji integrace nese): victron_gx = portal ID."""
        if self.domain != 'victron_gx':
            return None
        portals = {m.group(1) for e in self.entities() if (m := _VICTRON_PORTAL.match(str(e.unique_id).lower()))}
        return next(iter(portals)) if len(portals) == 1 else None
