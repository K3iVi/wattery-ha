"""Párování: kód z aplikace wattery + výběr střídače (zařízení ha-solarman)."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.helpers import selector

from . import api
from .const import (CONF_CLIENT, CONF_CODE, CONF_DEVICE, CONF_READ_PLAN, CONF_SN, CONF_STATION, CONF_TOKEN,
                    DOMAIN, SOLARMAN)

_LOGGER = logging.getLogger(__name__)
_PAIR_ERRORS = {'bad-code': 'bad_code', 'sn-mismatch': 'sn_mismatch', 'unsupported': 'unsupported'}


class WatteryConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def _pair(self, device_id: str, code: str) -> tuple[dict | None, dict, str | None]:
        """→ (data záznamu nebo None, chyby, SN)."""
        try:
            sn = await api.read_sn(self.hass, device_id)
        except Exception as e:  # noqa: BLE001 — ha-solarman hází různé výjimky (timeout, spojení)
            _LOGGER.warning('wattery: čtení SN ze střídače selhalo: %s', e)
            return None, {'base': 'read_failed'}, None
        if not sn:
            return None, {'base': 'read_failed'}, None
        try:
            code_http, res = await api.pair(self.hass, code, sn)
        except ConnectionError:
            return None, {'base': 'cannot_connect'}, sn
        if code_http != 200 or not res.get('token'):
            _LOGGER.warning('wattery: párování odmítnuto (%s): %s', code_http, res)
            return None, {'base': _PAIR_ERRORS.get(res.get('error'), 'pair_failed')}, sn
        return {CONF_TOKEN: res['token'], CONF_DEVICE: device_id, CONF_SN: sn,
                CONF_STATION: res.get('stationId'), CONF_CLIENT: res.get('clientId'),
                CONF_READ_PLAN: res.get('readPlan') or []}, {}, sn

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if not api.solarman_ready(self.hass):
            return self.async_abort(reason='no_solarman')
        errors: dict = {}
        if user_input is not None:
            data, errors, sn = await self._pair(user_input[CONF_DEVICE], user_input[CONF_CODE])
            if data:
                await self.async_set_unique_id(sn)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(title=f'wattery – střídač {sn}', data=data)
        schema = vol.Schema({
            vol.Required(CONF_CODE): selector.TextSelector(),
            vol.Required(CONF_DEVICE): selector.DeviceSelector(selector.DeviceSelectorConfig(integration=SOLARMAN)),
        })
        return self.async_show_form(step_id='user', data_schema=schema, errors=errors)

    async def async_step_reauth(self, entry_data: dict[str, Any]) -> ConfigFlowResult:
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        entry = self._get_reauth_entry()
        errors: dict = {}
        if user_input is not None:
            data, errors, sn = await self._pair(entry.data[CONF_DEVICE], user_input[CONF_CODE])
            if data:
                if sn != entry.data.get(CONF_SN):
                    return self.async_abort(reason='other_inverter')
                return self.async_update_reload_and_abort(entry, data=data)
        return self.async_show_form(step_id='reauth_confirm', errors=errors,
                                    data_schema=vol.Schema({vol.Required(CONF_CODE): selector.TextSelector()}))
