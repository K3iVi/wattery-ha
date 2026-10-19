"""Párování: volba připojení (značky) + kód z aplikace wattery.

- Deye přes Solarman: zařízení ha-solarman, identita = SN přečtené ze střídače.
- Victron GX přímo: adresa GX v místní síti (+ heslo MQTT, zůstává v HA); zkusí TLS 8883, pak 1883. Identita =
  portal ID z GX, otisk certifikátu se uloží (při změně se klient odpojí).
- Solax přes LAN dongle / převodník (Modbus TCP): adresa, port, jednotka, rámování; identita = SN ze střídače
  (holding 0x0000). Dongle pustí jediné spojení → tam, kde už je solax_modbus na stejné adrese (nebo přes jádro HA
  / sériově), se nepřipojujeme a nabídne se čtení z jejích entit.
- Jen čtení z jiné integrace: položka integrace, kterou zákazník už má (ENTITY_SOURCES); identita z unique_id jejích
  entit (victron_gx), nebo ji určí server (solax_modbus — G4 SN v entitách nemá; server spáruje jen k jedinému
  zařízení té značky na elektrárně). Žádné druhé spojení na zařízení, žádné řízení.
"""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.helpers import selector

from . import api
from .const import (CONF_CERT, CONF_CLIENT, CONF_CODE, CONF_CONN, CONF_DEVICE, CONF_FRAMING, CONF_HOST, CONF_PASSWORD,
                    CONF_PORT, CONF_READ_PLAN, CONF_SN, CONF_SOURCE, CONF_SOURCE_DOMAIN, CONF_STATION, CONF_TLS,
                    CONF_TOKEN, CONF_UNIT, CONN_ENTITIES, CONN_GX, CONN_MODBUS_TCP, CONN_SOLARMAN, DOMAIN,
                    ENTITY_SOURCES, GX_HOST_DEFAULT, SOLARMAN, SOLAX_MODBUS)
from .edge import client_caps
from .transport_ha_entities import SERVER_BINDS, HaEntities
from .transport_modbus_tcp import DEFAULT_PORT as MODBUS_PORT
from .transport_modbus_tcp import DEFAULT_UNIT, FRAMINGS, conflict_info, probe
from .transport_mqtt_gx import PORT_PLAIN, PORT_TLS, GxAuthError, GxCertError, GxError, GxMqtt
from .transport_solarman import read_sn, solarman_ready

_LOGGER = logging.getLogger(__name__)
_PAIR_ERRORS = {'bad-code': 'bad_code', 'sn-mismatch': 'sn_mismatch', 'unsupported': 'unsupported',
                'client-unsupported': 'unsupported'}
_TITLES = {CONN_SOLARMAN: 'wattery – střídač {sn}', CONN_GX: 'wattery – Victron GX', CONN_ENTITIES: 'wattery – {src}',
           CONN_MODBUS_TCP: 'wattery – střídač {sn}'}
_MB_ERRORS = {'connect': 'mb_connect', 'busy': 'mb_busy', 'timeout': 'mb_timeout', 'identity': 'mb_identity',
              'exception': 'mb_identity'}       # výjimka Modbus na čtení SN = jiné zařízení / jiná mapa


async def gx_probe(host: str, password: str, port: int | None = None, tls: bool | None = None,
                   cert: str | None = None) -> dict:
    """Spojení na GX: TLS 8883, při nedostupnosti 1883 (jako victron_gx) → {port, tls, portal, cert}.
    GxError ven: odmítnuté heslo / jiné zařízení (otisk) / nedostupné."""
    tries = [(port, bool(tls))] if port else [(PORT_TLS, True), (PORT_PLAIN, False)]
    last: Exception = GxError('GX nedostupné')
    for p, t in tries:
        gx = GxMqtt(host, p, tls=t, password=password, cert_sha256=cert)
        try:
            await gx.start()
            return {CONF_PORT: p, CONF_TLS: t, CONF_SN: gx.portal, CONF_CERT: gx.cert_sha256 if t else None}
        except (GxError, OSError) as e:
            last = e
            if isinstance(e, (GxAuthError, GxCertError)):
                break                                   # heslo / otisk: s 1883 by to bylo jen horší
        finally:
            await gx.close()
    raise last if isinstance(last, GxError) else GxError(str(last))


class WatteryConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    # ── společné ─────────────────────────────────────────────────────────────
    async def _server_pair(self, code: str, ident: str, caps: dict) -> tuple[dict | None, dict]:
        """→ (data záznamu od serveru nebo None, chyby)."""
        try:
            code_http, res = await api.pair(self.hass, code, ident, caps)
        except ConnectionError:
            return None, {'base': 'cannot_connect'}
        if code_http != 200 or not res.get('token'):
            _LOGGER.warning('wattery: párování odmítnuto (%s): %s', code_http, res)
            return None, {'base': _PAIR_ERRORS.get(res.get('error'), 'pair_failed')}
        return {CONF_TOKEN: res['token'], CONF_SN: str(res.get('sn') or ident), CONF_STATION: res.get('stationId'),
                CONF_CLIENT: res.get('clientId'), CONF_READ_PLAN: res.get('readPlan') or []}, {}

    async def _identity(self, conn: str, data: dict) -> tuple[dict | None, dict]:
        """Identita zařízení podle připojení (+ u GX zjištěný port, TLS, otisk) → (pole záznamu, chyby)."""
        if conn == CONN_SOLARMAN:
            try:
                sn = await read_sn(self.hass, data[CONF_DEVICE])
            except Exception as e:  # noqa: BLE001 — ha-solarman hází různé výjimky (timeout, spojení)
                _LOGGER.warning('wattery: čtení SN ze střídače selhalo: %s', e)
                sn = None
            return ({CONF_SN: sn}, {}) if sn else (None, {'base': 'read_failed'})
        if conn == CONN_GX:
            try:
                found = await gx_probe(data[CONF_HOST], data.get(CONF_PASSWORD) or '', data.get(CONF_PORT),
                                       data.get(CONF_TLS), data.get(CONF_CERT))
            except GxError as e:
                _LOGGER.warning('wattery: GX %s: %s', data[CONF_HOST], e)
                err = 'gx_auth' if isinstance(e, GxAuthError) else 'gx_cert' if isinstance(e, GxCertError) else (
                    'gx_portal' if 'portal' in str(e) else 'gx_failed')
                return None, {'base': err}
            return found, {}
        if conn == CONN_MODBUS_TCP:
            res = await probe(data[CONF_HOST], data[CONF_PORT], data[CONF_UNIT], data[CONF_FRAMING])
            if not res.get('ok'):
                _LOGGER.warning('wattery: Modbus TCP %s:%s: %s', data[CONF_HOST], data[CONF_PORT], res.get('error'))
                return None, {'base': _MB_ERRORS.get(res.get('reason'), 'mb_failed')}
            return {CONF_SN: res['sn']}, {}
        src = HaEntities(self.hass, data[CONF_SOURCE_DOMAIN], data[CONF_SOURCE])
        ident = src.identity()
        if not ident and data[CONF_SOURCE_DOMAIN] in SERVER_BINDS:
            return {CONF_SN: ''}, {}                     # identitu vrátí server v odpovědi na párování
        return ({CONF_SN: ident}, {}) if ident else (None, {'base': 'no_identity'})

    def _caps(self, conn: str, data: dict) -> dict:
        if conn == CONN_GX:
            return client_caps(('mqtt',), modbus=False)
        if conn == CONN_ENTITIES:
            return client_caps(('ha_entity',), modbus=False, integrations=(data[CONF_SOURCE_DOMAIN],))
        if conn == CONN_MODBUS_TCP:
            return client_caps()
        return client_caps(integrations=(SOLARMAN,))

    async def _pair(self, conn: str, data: dict, code: str, new: bool = True) -> tuple[dict | None, dict]:
        found, errors = await self._identity(conn, data)
        if not found:
            return None, errors
        rec = dict(data, **found)
        if new:      # už spárované zařízení → konec dřív, než server spotřebuje kód a založí dalšího klienta
            await self.async_set_unique_id(rec[CONF_SN] or f'{data[CONF_SOURCE_DOMAIN]}-{data[CONF_SOURCE]}')
            self._abort_if_unique_id_configured()
        res, errors = await self._server_pair(code, rec[CONF_SN], self._caps(conn, rec))
        return (dict(rec, **res, **{CONF_CONN: conn}) if res else None), errors

    def _finish(self, conn: str, rec: dict, title_src: str = '') -> ConfigFlowResult:
        return self.async_create_entry(title=_TITLES[conn].format(sn=rec[CONF_SN], src=title_src), data=rec)

    # ── volba připojení ──────────────────────────────────────────────────────
    def _entity_sources(self) -> list:
        return [e for d in ENTITY_SOURCES for e in self.hass.config_entries.async_entries(d) if not e.disabled_by]

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        # Kdo už má solax_modbus / victron_gx, toho čteme z entit (§5) → ta volba před Modbus TCP (dongle = 1 spojení).
        opts = ['solarman', 'gx'] + (['entities'] if self._entity_sources() else []) + ['modbus_tcp']
        return self.async_show_menu(step_id='user', menu_options=opts)

    async def async_step_solarman(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if not solarman_ready(self.hass):
            return self.async_abort(reason='no_solarman')
        errors: dict = {}
        if user_input is not None:
            rec, errors = await self._pair(CONN_SOLARMAN, {CONF_DEVICE: user_input[CONF_DEVICE]},
                                           user_input[CONF_CODE])
            if rec:
                return self._finish(CONN_SOLARMAN, rec)
        schema = vol.Schema({
            vol.Required(CONF_CODE): selector.TextSelector(),
            vol.Required(CONF_DEVICE): selector.DeviceSelector(selector.DeviceSelectorConfig(integration=SOLARMAN)),
        })
        return self.async_show_form(step_id='solarman', data_schema=schema, errors=errors)

    async def async_step_gx(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict = {}
        if user_input is not None:
            data = {CONF_HOST: user_input[CONF_HOST].strip(), CONF_PASSWORD: user_input.get(CONF_PASSWORD) or ''}
            rec, errors = await self._pair(CONN_GX, data, user_input[CONF_CODE])
            if rec:
                return self._finish(CONN_GX, rec)
        defaults = user_input or {}
        schema = vol.Schema({
            vol.Required(CONF_HOST, default=defaults.get(CONF_HOST, GX_HOST_DEFAULT)): selector.TextSelector(),
            vol.Optional(CONF_PASSWORD): selector.TextSelector(
                selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)),
            vol.Required(CONF_CODE): selector.TextSelector(),
        })
        return self.async_show_form(step_id='gx', data_schema=schema, errors=errors)

    async def async_step_modbus_tcp(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict = {}
        ph = {'name': '', 'host': '', 'port': ''}
        if user_input is not None:
            data = {CONF_HOST: user_input[CONF_HOST].strip(), CONF_PORT: int(user_input[CONF_PORT]),
                    CONF_UNIT: int(user_input[CONF_UNIT]), CONF_FRAMING: user_input[CONF_FRAMING]}
            # Už spárovaný dongle drží naše spojení (pustí jen jedno) → probe by hlásil „obsazeno“; skončit dřív.
            self._async_abort_entries_match({k: data[k] for k in (CONF_HOST, CONF_PORT, CONF_UNIT)})
            opts = [dict(e.data, **e.options) for e in self.hass.config_entries.async_entries(SOLAX_MODBUS)
                    if not e.disabled_by]
            clash = conflict_info(opts, data[CONF_HOST], data[CONF_PORT])
            if clash:           # dongle pustí jedno spojení — druhé by shodilo solax_modbus (§5: čtení z entit)
                errors = {'base': f'mb_conflict_{clash["code"]}'}
                ph = {k: str(clash[k]) for k in ph}
            else:
                rec, errors = await self._pair(CONN_MODBUS_TCP, data, user_input[CONF_CODE])
                if rec:
                    return self._finish(CONN_MODBUS_TCP, rec)
        d = user_input or {}
        schema = vol.Schema({
            vol.Required(CONF_HOST, default=d.get(CONF_HOST, '')): selector.TextSelector(),
            vol.Required(CONF_PORT, default=d.get(CONF_PORT, MODBUS_PORT)): selector.NumberSelector(
                selector.NumberSelectorConfig(min=1, max=65535, mode=selector.NumberSelectorMode.BOX)),
            vol.Required(CONF_UNIT, default=d.get(CONF_UNIT, DEFAULT_UNIT)): selector.NumberSelector(
                selector.NumberSelectorConfig(min=1, max=247, mode=selector.NumberSelectorMode.BOX)),
            vol.Required(CONF_FRAMING, default=d.get(CONF_FRAMING, FRAMINGS[0])): selector.SelectSelector(
                selector.SelectSelectorConfig(options=list(FRAMINGS), translation_key='framing')),
            vol.Required(CONF_CODE): selector.TextSelector(),
        })
        return self.async_show_form(step_id='modbus_tcp', data_schema=schema, errors=errors,
                                    description_placeholders=ph)

    async def async_step_entities(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        sources = {e.entry_id: e for e in self._entity_sources()}
        if not sources:
            return self.async_abort(reason='no_sources')
        errors: dict = {}
        if user_input is not None and user_input[CONF_SOURCE] in sources:
            src = sources[user_input[CONF_SOURCE]]
            data = {CONF_SOURCE: src.entry_id, CONF_SOURCE_DOMAIN: src.domain}
            rec, errors = await self._pair(CONN_ENTITIES, data, user_input[CONF_CODE])
            if rec:
                return self._finish(CONN_ENTITIES, rec, title_src=src.title)
        options = [selector.SelectOptionDict(value=e.entry_id, label=f'{e.title} ({e.domain})')
                   for e in sources.values()]
        schema = vol.Schema({
            vol.Required(CONF_SOURCE): selector.SelectSelector(selector.SelectSelectorConfig(options=options)),
            vol.Required(CONF_CODE): selector.TextSelector(),
        })
        return self.async_show_form(step_id='entities', data_schema=schema, errors=errors)

    # ── nové spárování (server spojení odpojil) ──────────────────────────────
    async def async_step_reauth(self, entry_data: dict[str, Any]) -> ConfigFlowResult:
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        entry = self._get_reauth_entry()
        errors: dict = {}
        if user_input is not None:
            conn = entry.data.get(CONF_CONN) or CONN_SOLARMAN
            keep = {k: v for k, v in entry.data.items() if k in (CONF_DEVICE, CONF_HOST, CONF_PASSWORD, CONF_PORT,
                                                                   CONF_TLS, CONF_CERT, CONF_SOURCE,
                                                                   CONF_SOURCE_DOMAIN, CONF_UNIT, CONF_FRAMING)}
            rec, errors = await self._pair(conn, keep, user_input[CONF_CODE], new=False)
            if rec:
                if rec[CONF_SN] != entry.data.get(CONF_SN):
                    return self.async_abort(reason='other_inverter')
                if not entry.data.get(CONF_CONN):
                    rec.pop(CONF_CONN, None)            # položka z 0.4: tvar dat beze změny
                return self.async_update_reload_and_abort(entry, data=rec)
        return self.async_show_form(step_id='reauth_confirm', errors=errors,
                                    data_schema=vol.Schema({vol.Required(CONF_CODE): selector.TextSelector()}))
