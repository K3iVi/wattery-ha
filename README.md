# wattery pro Home Assistant

Lokální spojení elektrárny s aplikací [wattery](https://monitoring.wattery.cz): Home Assistant čte střídač
po domácí síti (přes integraci [Solarman / ha-solarman](https://github.com/davidrapan/ha-solarman)) a posílá
data do wattery — rychleji a spolehlivěji než přes výrobcův cloud.

- **Co číst, určuje server wattery** — nové hodnoty přibudou bez aktualizace integrace.
- **Řízení je volitelné a výchozí vypnuté** (přepínač *Řízení z wattery*, viz níže).
- Podporované střídače: Deye SG04LP3 / SG05LP3 (další přibudou).

## Instalace

1. Mějte nastavenou integraci **Solarman** (ha-solarman) se svým střídačem.
2. HACS → tři tečky vpravo nahoře → **Vlastní repozitáře** → `https://github.com/K3iVi/wattery-ha`, typ **Integrace**.
3. Najděte **wattery** → Stáhnout → restartujte Home Assistant.
4. V aplikaci wattery: **Nastavení → Lokální spojení → Spárovat zařízení** — zobrazí se kód (platí 15 minut).
5. V Home Assistantu: **Nastavení → Zařízení a služby → Přidat integraci → wattery** → zvolte, jak se zařízení
   čte (Deye přes Solarman, Victron GX, Solax přes LAN dongle, nebo jen čtení z jiné integrace — viz 0.5.0), zadejte kód a vyberte zařízení.

Stav spojení ukazuje senzor **Lokální spojení** (Online / Chyba / Odpojeno). Když spojení ve wattery odpojíte,
Home Assistant nabídne spárovat znovu.

## Řízení z wattery (od verze 0.2.0)

Přepínač **Řízení z wattery** (u zařízení wattery, sekce Konfigurace) povolí, aby wattery posílala příkazy pro
obchodování se spotovou elektřinou (okna TOU, nabíjecí proudy, max. prodej do sítě, Low Batt SOC, čas střídače)
přes Home Assistant místo přes Deye cloud.

- **Výchozí vypnuto.** Vypnutý přepínač = Home Assistant jen čte a wattery řídí jako dřív přes Deye cloud.
  Obchodování jako celek se zapíná a vypíná v aplikaci wattery.
- **Jen povolené registry.** Integrace zapíše jen nastavení, která wattery u střídače Deye skutečně řídí, v
  povolených mezích (seznam `DEYE_PROFILE` v `custom_components/wattery/edge.py`). Cokoli jiného odmítne,
  i kdyby to server poslal.
- **Každý zápis se hned přečte zpět** a výsledek jde do wattery (audit každého příkazu).
- **Pojistky:** nejvýš 15 zápisů za minutu a 60 za hodinu, mezi zápisy aspoň 1 s, příkaz starší než 10 s
  se neprovede.
- Když Home Assistant neběží nebo příkaz neprovede, wattery ho pošle přes Deye cloud.

Stav ukazují atributy senzoru *Lokální spojení*: `control` (přepínač), `server_control` (řízení na straně wattery),
`commands` a `last_command`.

## Offline pojistka (od verze 0.3.0)

Když wattery obchoduje přes Home Assistant, posílá s každou odpovědí i **neutrální rozvrh** (jedno okno držení
baterie na rezervě, bez prodeje do sítě) a dobu, do kdy platí plán ve střídači (nejvýš 12 h, nejdéle do konce dnů
s cenami). Home Assistant si ho uloží (přežije restart).

- Když se Home Assistant **nespojí se serverem wattery** (síť, výpadek serveru) a platnost plánu mezitím vyprší,
  zapíše neutrální rozvrh **jednou** — stejnou cestou a se stejnými kontrolami jako ostatní příkazy (přepínač
  *Řízení z wattery* musí být zapnutý, jen povolené registry, zpětné čtení). Střídač tak neobchoduje podle starého
  plánu se zastaralými cenami.
- Když wattery zápis odmítne (např. střídač už k elektrárně nepatří), pojistka se nespustí — jen při výpadku.
- Po obnovení spojení Home Assistant zápis nahlásí, wattery ho zaznamená a plán hned přepíše aktuálním.

Atributy senzoru: `fallback_until` (do kdy platí plán) a `fallback_applied` (kdy se naposled zapsal neutrál).

## Verze 0.4.0 — příprava na další značky

U střídačů Deye se nic nemění (stejné registry, limity, zpětné čtení i offline pojistka). Integrace nově hlásí
serveru, co umí (přenosy, vestavěné profily zápisu v `custom_components/wattery/profiles/`), a umí:

- **profil zápisu podle zařízení** — vestavěný profil platí jen tehdy, když identita zařízení podle profilu sedí
  na spárované (sériové číslo), jinak integrace zápisy odmítne; zařízení, které wattery označí jako jen ke čtení
  (bez profilu zápisu), integrace jen čte;
- **pojistky příkazů s obnovou** — příkaz, který střídač drží jen chvíli (např. dálkové řízení), integrace
  obnovuje, dokud ho wattery prodlužuje; bez spojení se serverem ho po vypršení jednou ukončí a po obnovení
  spojení to nahlásí;
- **čtení po zápisu podle zadání serveru** (u střídačů s oddělenou mapou čtení a zápisu).

Atributy senzoru: `profile` (profil zápisu) a `leases` (pojistky příkazů a do kdy platí).

## Verze 0.5.0 — volba připojení

Při přidání integrace se nejdřív vybírá, jak Home Assistant zařízení čte (jedna položka integrace = jedno připojení):

- **Střídač Deye přes Solarman** — jako dosud (čtení i volitelné řízení). Položky z dřívějších verzí fungují beze změny.
- **Victron GX (Cerbo) přímo v místní síti** — adresa GX (výchozí `venus.local`) a heslo z „Zabezpečení místní sítě“
  na GX. Integrace zkusí šifrované spojení (8883) a při nedostupnosti 1883; heslo i adresa zůstávají v Home
  Assistantu. Při prvním spojení si uloží otisk certifikátu GX — když se na adrese později ozve jiné zařízení,
  spojení odmítne. Zatím jen čtení; Victron je v pilotním provozu (kód pro Victron vydá wattery, až bude u vaší
  elektrárny zapnutý).
- **Střídač Solax přes LAN dongle nebo převodník (Modbus TCP)** — adresa donglu (Pocket WiFi 3.0 / LAN) nebo
  převodníku RS485 → Ethernet, port (502), adresa jednotky (1) a rámování (Modbus TCP, u převodníku v režimu
  „transparent“ RTU přes TCP). Sériové číslo se přečte ze střídače. Dongle pustí jen jedno spojení: když už máte
  integraci SolaX Modbus na stejné adrese (nebo přes Modbus jádra HA či sériově), integrace se nepřipojí a nabídne
  čtení z jejích entit. Řízení přepínačem až po zapnutí řízení Solaxu u vaší elektrárny ve wattery.
- **Jen čtení z jiné integrace** (nabízí se, když máte v Home Assistantu Victron GX nebo SolaX Modbus) — wattery
  čte hodnoty z entit integrace, kterou už máte. Žádné další spojení na zařízení (dongle Solaxu pustí jen jedno),
  žádné řízení (bez přepínače). U Solaxu G4 entity sériové číslo nenesou — wattery integraci spáruje se střídačem
  elektrárny, jen když je na ní jediný Solax.

Při opakovaném párování stejného zařízení integrace skončí dřív, než kód pošle do wattery.

## Verze 0.6.0 — plán baterie jako data

Když má elektrárna ve wattery zapnuté obchodování na spotu, posílá wattery do Home Assistantu i plán baterie
(po čtvrthodinách do konce dne se známými cenami, přepočet zhruba jednou za hodinu). Plán je jen informace —
do střídače zapisuje dál jen přepínač „Řízení z wattery“. Kdo chce řídit sám, provede plán vlastní automatizací.

- **Senzor „Plán baterie“** — co má baterie dělat teď: `charge_grid` (nabíjení ze sítě), `discharge_grid` (prodej
  z baterie), `hold` (podržet baterii, spotřeba ze sítě), `no_export` (bez přetoku do sítě — záporná cena),
  `self_use` (samospotřeba). Atributy: `until` (konec akce), `power_w` (výkon nákupu / prodeje za celou
  elektrárnu), `target_soc` (SOC na konci akce), `reserve_soc`, `price_buy` / `price_sell` a `currency`,
  `valid_until`, `computed_at`, `next` (dalších 6 akcí).
- **Kalendář „Plán baterie“** — akce kromě samospotřeby jako události („Nabíjení ze sítě 5 kW → 80 %“). Automatizace
  stačí spustit triggerem kalendáře (začátek / konec, i s předstihem); popis události nese `action`, `power_w`
  a `target_soc`.
- **Služba `wattery.get_plan`** — celý plán po čtvrthodinách (akce, výkon, SOC, ceny) do skriptu (odpověď služby).

Plán přežije restart Home Assistantu a platí do `valid_until` (3 hodiny od výpočtu). Bez nového plánu jsou senzor
i kalendář nedostupné — doporučujeme pak vrátit střídač do samospotřeby. Když obchodování ve wattery vypnete,
plán zmizí.

```yaml
automation:
  - alias: Nabíjení ze sítě podle wattery
    triggers:
      - trigger: calendar
        event: start
        entity_id: calendar.wattery_2400000001_plan_baterie
    conditions:
      - condition: template
        value_template: "{{ 'action: charge_grid' in trigger.calendar_event.description }}"
    actions:
      - action: script.nabijeni_ze_site      # váš skript pro střídač
```

---

## English

Local connection between a solar plant and the [wattery](https://monitoring.wattery.cz) app. Home Assistant
reads the inverter on the local network via [ha-solarman](https://github.com/davidrapan/ha-solarman) and sends
raw registers to wattery; the server decides which registers to read (no update needed for new values).
Install via HACS (custom repository, category Integration), then add the **wattery** integration with a pairing
code from the wattery app.

**Control (0.2.0+, off by default):** the *Control from wattery* switch lets wattery send its spot-trading commands
(TOU windows, charge currents, max export power, Low Batt SOC, inverter clock) through Home Assistant instead of the
Deye cloud. Only whitelisted registers within limits (`DEYE_PROFILE`), every write is read back, max 15 writes/min
and 60/h. Switch off = read-only; wattery then controls via the Deye cloud as before.

**Offline fallback (0.3.0+):** while trading through Home Assistant, wattery sends a neutral schedule (hold the battery
at reserve, no export) and how long the current plan stays valid (max 12 h). If Home Assistant cannot reach wattery
(network or server outage, not a refusal) after that time, it writes the neutral schedule once, with the same checks
as other commands (switch on, whitelist, read-back), and reports it once the connection is back. State survives
restarts; sensor attributes `fallback_until` and `fallback_applied`.

**0.4.0:** no change for Deye. The integration now tells the server what it supports (`caps`: transports, built-in
write profiles in `profiles/`), applies a write profile only when the device identity matches the paired serial
(a device the server marks read-only gets no writes at all), keeps short-lived commands alive while wattery extends them (and ends them once if the server is unreachable), and
reads back what the server specifies. Sensor attributes `profile` and `leases`.

**0.5.0:** adding the integration starts with the connection type: Deye via Solarman (as before; existing entries
unchanged), Victron GX directly on the local network (GX address, MQTT password from the GX local network security
setting; TLS 8883 with fallback to 1883, the password and address stay in Home Assistant, the GX certificate
fingerprint is pinned on first connect; read-only for now, Victron is in pilot), a Solax inverter via a LAN dongle
or an RS485 → Ethernet converter (Modbus TCP or RTU over TCP; the serial number is read from the inverter; the dongle
accepts a single connection, so the integration refuses an address the SolaX Modbus integration already uses and
offers its entities instead), or read-only from the entities of an
integration you already have (Victron GX or SolaX Modbus; no extra connection to the device, no control switch; a
Solax G4 has no serial number in its entities, so wattery pairs it only with a plant that has a single Solax).

**0.6.0 — battery plan as data:** when spot trading is on for the plant, wattery also sends the battery plan
(quarter-hours until the end of the last day with known prices, recomputed about hourly). It is information only;
writing to the inverter still requires the *Control from wattery* switch. A *Battery plan* sensor (now:
`charge_grid` / `discharge_grid` / `hold` / `no_export` / `self_use`; attributes `until`, `power_w`, `target_soc`,
`reserve_soc`, prices, `valid_until`, `next`), a *Battery plan* calendar (all actions except self-consumption as
events, with `action`, `power_w`, `target_soc` in the description — use a calendar trigger) and the
`wattery.get_plan` service (whole plan per quarter-hour as a response). The plan survives restarts and is valid
for 3 hours; without a new one the entities become unavailable (fall back to self-consumption).

## Vývoj

- `python tests/test_edge.py` — testy jádra bez Home Assistantu (čtení, řízení, offline pojistka, shoda profilu se serverem,
  když je vedle checkout deye-monitor; volitelně E2E proti lokálnímu serveru, viz soubor).
- `python tests/server_e2e.py` — klient proti skutečnému serveru deye-monitor v jednom procesu (dočasná DB, bez sítě).
- `<venv s homeassistant>/python tests/ha_core_control.py` — přepínač, zápis a offline pojistka (HA Store) uvnitř
  Home Assistant core.
- `<venv>/python tests/ha_core_flow.py` — volba připojení: Victron GX proti simulátoru (1883 i TLS s otiskem),
  čtení z entit jiné integrace, Solax přes LAN dongle proti simulátoru (`tests/solax_dongle_sim.py`: obsazený
  dongle, jiná jednotka, souběh se SolaX Modbus), opakované a nové spárování.
- `<venv>/python tests/ha_core_plan.py` — plán baterie: senzor, kalendář, služba `wattery.get_plan`, obnova z HA
  Store, konec platnosti.
- `<venv>/python tests/ha_core_e2e.py <url> <kód> <sn>` — párování a čtení proti lokálnímu serveru.
