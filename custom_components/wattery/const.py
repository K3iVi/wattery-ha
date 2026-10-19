"""Konstanty integrace wattery."""
DOMAIN = 'wattery'
VERSION = '0.5.0'
CLIENT_VER = f'ha-int/{VERSION}'
BACKEND = 'ha-solarman'                   # backend v sync podle připojení (BACKENDS)
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

# Připojení (od 0.5.0): jedna položka integrace = jedno připojení
CONF_CONN = 'conn'                         # chybí = Deye přes ha-solarman (položky z 0.4 a starších)
CONN_SOLARMAN, CONN_GX, CONN_ENTITIES, CONN_MODBUS_TCP = 'solarman', 'gx_mqtt', 'ha_entities', 'modbus_tcp'
BACKENDS = {CONN_SOLARMAN: BACKEND, CONN_GX: 'ha-gx-mqtt', CONN_ENTITIES: 'ha-entities',
            CONN_MODBUS_TCP: 'ha-modbus-tcp'}
CONF_HOST = 'host'                         # GX: adresa v místní síti
CONF_PORT = 'port'
CONF_TLS = 'tls'
CONF_PASSWORD = 'password'                 # GX: heslo MQTT (zůstává v HA, na server nejde)
CONF_CERT = 'cert_sha256'                  # GX: otisk certifikátu z prvního spojení (při změně se klient odpojí)
CONF_SOURCE = 'source_entry'               # entity: config entry integrace, ze které se čte
CONF_SOURCE_DOMAIN = 'source_domain'
GX_HOST_DEFAULT = 'venus.local'
CONF_UNIT = 'unit'                         # Modbus TCP: adresa jednotky za donglem / převodníkem
CONF_FRAMING = 'framing'                   # Modbus TCP: 'tcp' (MBAP) | 'rtu' (RTU přes TCP)
SOLAX_MODBUS = 'solax_modbus'
ENTITY_SOURCES = ('victron_gx', 'solax_modbus')   # integrace, ze kterých umí server číst entity
