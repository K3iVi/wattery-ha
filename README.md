# wattery pro Home Assistant

Lokální spojení elektrárny s aplikací [wattery](https://monitoring.wattery.cz): Home Assistant čte střídač
po domácí síti (přes integraci [Solarman / ha-solarman](https://github.com/davidrapan/ha-solarman)) a posílá
data do wattery — rychleji a spolehlivěji než přes výrobcův cloud.

- **Jen čte.** Tahle verze do střídače nic nezapisuje.
- **Co číst, určuje server wattery** — nové hodnoty přibudou bez aktualizace integrace.
- Podporované střídače: Deye SG04LP3 / SG05LP3 (další přibudou).

## Instalace

1. Mějte nastavenou integraci **Solarman** (ha-solarman) se svým střídačem.
2. HACS → tři tečky vpravo nahoře → **Vlastní repozitáře** → `https://github.com/K3iVi/wattery-ha`, typ **Integrace**.
3. Najděte **wattery** → Stáhnout → restartujte Home Assistant.
4. V aplikaci wattery: **Nastavení → Lokální spojení → Spárovat zařízení** — zobrazí se kód (platí 15 minut).
5. V Home Assistantu: **Nastavení → Zařízení a služby → Přidat integraci → wattery** → zadejte kód a vyberte střídač.

Stav spojení ukazuje senzor **Lokální spojení** (Online / Chyba / Odpojeno). Když spojení ve wattery odpojíte,
Home Assistant nabídne spárovat znovu.

---

## English

Local connection between a solar plant and the [wattery](https://monitoring.wattery.cz) app. Home Assistant
reads the inverter on the local network via [ha-solarman](https://github.com/davidrapan/ha-solarman) and sends
raw registers to wattery. Read-only; the server decides which registers to read (no update needed for new values).
Install via HACS (custom repository, category Integration), then add the **wattery** integration with a pairing
code from the wattery app.

## Vývoj

`python tests/test_edge.py` — testy jádra bez Home Assistantu (volitelně E2E proti lokálnímu serveru, viz soubor).
