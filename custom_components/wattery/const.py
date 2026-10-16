"""Konstanty integrace wattery."""
DOMAIN = 'wattery'
VERSION = '0.3.0'
CLIENT_VER = f'ha-int/{VERSION}'
BACKEND = 'ha-solarman'
SERVER_URL = 'https://monitoring.wattery.cz'
SOLARMAN = 'solarman'                      # integrace ha-solarman (David Rapan), čte střídač
SN_BLOCK = (3, 3, 5)                       # (fc, adresa, počet) — sériové číslo Deye v ASCII

CONF_TOKEN = 'token'
CONF_DEVICE = 'device'                     # device_id zařízení ha-solarman
CONF_SN = 'sn'
CONF_CODE = 'code'
CONF_READ_PLAN = 'read_plan'
CONF_STATION = 'station_id'
CONF_CLIENT = 'client_id'
