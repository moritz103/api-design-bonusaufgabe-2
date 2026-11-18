# Resilienztests (Abschnitt 2.7)

Alle Szenarien sind gezielt reproduzierbar und automatisiert:

```bash
python -m tools.run_scenarios          # alle 6 Szenarien, startet/stoppt die Services selbst
python -m tools.run_scenarios 4 5      # nur Ausfall + Deadline
```

Die Logs der Services liegen danach unter `logs/`. Über die Correlation-ID lässt sich jeder Aufruf durch alle Services verfolgen. Fehler werden durch **Fehlerinjektion** ausgelöst (Service beenden, `--delay`, ungültige Eingaben) und nicht durch Zufall.

Übersicht:

| # | Szenario | Code beim Consumer |
|---|---|---|
| 1 | Nicht existierende Session / Ladesäule | `NOT_FOUND` |
| 2 | Fachlich ungültige Daten | `INVALID_ARGUMENT` |
| 3 | Ladesäule belegt / gesperrt | `FAILED_PRECONDITION` |
| 4 | **Ausfall ChargePointService** | `UNAVAILABLE` |
| 5 | **TariffService überschreitet Deadline** | `DEADLINE_EXCEEDED` |
| 6 | Streaming-Verbindung unterbrochen | `UNAVAILABLE` → Wiederverbindung |

---

## Szenario 1 – Nicht existierende session_id / Ladesäule

`python client.py get cs-0000000000` und `python client.py create cp-999`

1. **Auslöser:** Der Consumer fragt eine Session-ID bzw. eine Ladesäule an, die es nicht gibt.
2. **Entstehung:**
   - Session: im ChargingSessionService (`_get_or_abort`)
   - Ladesäule: im ChargePointService bei `ReserveChargePoint`
3. **Erzeugter Code:** `NOT_FOUND` in beiden Fällen.
4. **Code beim Consumer:** `NOT_FOUND`. Der SessionService reicht den fachlichen Fehler des ChargePointService bewusst durch, weil der Consumer ihn mit der falschen ID verursacht hat.
5. **Reaktion des Consumers:** Er gibt Fehlermeldung, Correlation-ID und den Hinweis „ID prüfen“ aus. Exit-Code 1, kein Retry.
6. **Auswirkung auf andere Services:** keine. Es wird keine Session gespeichert und nichts reserviert, und der TariffService wird gar nicht erst aufgerufen.
7. **Weiterarbeit:** ja, sofort. Der nächste gültige Aufruf funktioniert.

## Szenario 2 – Fachlich ungültige Daten

- `python client.py create cp-002 --id-tag ""`
- `python client.py list --page-size -5`
- `python simulator.py <session> --invalid` (Zählerstand sinkt im Stream)

1. **Auslöser:** Die Requests verletzen die fachlichen Regeln des Datenmodells:
   - `id_tag` muss 1–64 Zeichen lang sein.
   - `page_size` darf nicht negativ sein.
   - `energy_wh` ist kumulativ und darf nicht sinken.
2. **Entstehung:**
   - ChargingSessionService (Validierung vor jedem Downstream-Aufruf)
   - MeteringService (`_validate` im Client-Stream)
3. **Erzeugter Code:** `INVALID_ARGUMENT`.
4. **Code beim Consumer:** `INVALID_ARGUMENT`. Bei `ReportMeterValues` bricht der Server den gesamten Client-Stream ab.
5. **Reaktion des Consumers:** „Eingabe korrigieren – Wiederholung ohne Änderung ist zwecklos.“ Kein Retry. Der Simulator meldet den Abbruch.
6. **Auswirkung auf andere Services:** keine. Weil früh validiert wird (*fail fast*), werden ChargePoint- und TariffService nicht belastet. Die Messwerte, die vor dem ungültigen Wert gültig waren, bleiben im Metering gespeichert.
7. **Weiterarbeit:** ja. Andere Streams und Sessions sind nicht betroffen.

## Szenario 3 – Ladesäule nicht reservierbar (belegt / gesperrt)

Zweimal `python client.py create cp-004`, außerdem `python client.py create cp-003` (cp-003 ist `OUT_OF_ORDER`).

1. **Auslöser:** Die Säule ist bereits einer anderen Session zugeordnet bzw. gesperrt.
2. **Entstehung:** im ChargePointService (`ReserveChargePoint`, Prüfung unter Lock, daher keine Race Condition bei gleichzeitigen Anfragen).
3. **Erzeugter Code:** `FAILED_PRECONDITION`, mit Begründung „bereits belegt (Session …)“ bzw. „gesperrt“.
4. **Code beim Consumer:** `FAILED_PRECONDITION` (durchgereicht).
5. **Reaktion des Consumers:** „Andere Ladesäule wählen oder Zustand prüfen“. Kein blindes Retry.
6. **Auswirkung auf andere Services:** Die bestehende Belegung bleibt unverändert, und der TariffService wird nicht aufgerufen.
7. **Weiterarbeit:** ja. Nach `stop` der ersten Session ist cp-004 wieder reservierbar.

## Szenario 4 – Ausfall des ChargePointService (Service-Ausfall)

ChargePointService beenden, dann `python client.py create cp-001`, dann den Service neu starten und den Befehl wiederholen.

1. **Auslöser:** Der ChargePointService-Prozess wird vor dem service-übergreifenden Aufruf beendet.
2. **Entstehung:** im ChargingSessionService, in seiner Rolle als *Client* des ChargePointService. Der gRPC-Kanal bekommt keine Verbindung.
3. **Erzeugter Code:** `UNAVAILABLE` (vom gRPC-Client-Stack im SessionService).
4. **Code beim Consumer:** `UNAVAILABLE` mit eigener Meldung „ChargePointService nicht erreichbar – bitte später erneut versuchen“. Begründung: Der Fehler ist vorübergehend, und ein Retry ist sinnvoll. Es ist weder ein Bug (`INTERNAL`) noch ein fachlicher Zustand (`FAILED_PRECONDITION`). Details in ENTWURF.md, Abschnitt 2.4.
5. **Reaktion des Consumers:** Hinweis „später erneut versuchen“, Exit-Code 2 (wiederholbar).
6. **Auswirkung auf andere Services:**
   - Es wird *keine* halbfertige Session gespeichert und nichts reserviert. Der Test prüft, dass es 0 aktive Sessions gibt.
   - TariffService und Metering arbeiten unverändert weiter.
   - `get`/`list` des SessionService funktionieren weiterhin (partielle Verfügbarkeit).
7. **Weiterarbeit:** ja. Nach dem Neustart verbindet sich der gRPC-Kanal automatisch neu, und `create cp-001` gelingt.

## Szenario 5 – TariffService überschreitet die Deadline

TariffService mit `--delay 3` starten (Antwort nach 3 s), das Zeitbudget im SessionService beträgt 1,5 s. Dann `python client.py create cp-001`.

1. **Auslöser:** Die Tarifberechnung dauert gezielt länger als die Deadline des Aufrufers.
2. **Entstehung:**
   - Die Deadline läuft im ChargingSessionService beim Aufruf `CalculateTariff(timeout=1.5)` ab.
   - Der TariffService erkennt per `context.is_active()`, dass der Aufrufer abgebrochen hat, und verwirft die Berechnung.
3. **Erzeugter Code:** `DEADLINE_EXCEEDED` (clientseitig im SessionService, nach rund 1,5 s und nicht erst nach 3 s).
4. **Code beim Consumer:** `DEADLINE_EXCEEDED`, mit Meldung „Ladevorgang cs-… nicht gestartet: TariffService hat nicht innerhalb des Zeitbudgets geantwortet“. Die Session-ID kommt zusätzlich in der Trailing Metadata `x-session-id`.
5. **Reaktion des Consumers:** Er meldet „Vorgang wurde nicht gestartet, erneut versuchen“ und gibt die Session-ID aus. Exit-Code 2.
6. **Auswirkung auf andere Services:**
   - **Kompensation:** Der SessionService gibt cp-001 per `ReleaseChargePoint` wieder frei. Der Test prüft, dass die Säule danach `AVAILABLE` ist.
   - Die Session wird als `FAILED` mit Grund gespeichert und ist über `get` nachvollziehbar.
   - Der TariffService bricht seine Arbeit ab.
7. **Weiterarbeit:** ja. Nach dem Neustart des TariffService ohne Verzögerung startet ein neuer Ladevorgang normal.

## Szenario 6 – Streaming-Verbindung wird während der Übertragung unterbrochen

`python client.py watch <session> --history` und `python simulator.py <session>` laufen. Der MeteringService wird beendet und neu gestartet, danach sendet die Säule erneut.

1. **Auslöser:** Der MeteringService fällt während zweier laufender Streams aus: Client-Stream der Säule und Server-Stream des Consumers.
2. **Entstehung:** auf der Transportebene (HTTP/2-Verbindung bricht ab). Erkannt wird der Fehler in beiden Clients.
3. **Erzeugter Code:** `UNAVAILABLE` („Socket closed“ bzw. „failed to connect“).
4. **Code beim Consumer:** `UNAVAILABLE` im Simulator (Client Streaming) und im `watch`-Consumer (Server Streaming).
5. **Reaktion des Consumers:**
   - `watch` verbindet sich mit exponentiellem Backoff neu (1 s, 2 s, …, maximal 5 Versuche).
   - Er abonniert erneut mit `include_history` und überspringt bereits angezeigte Werte anhand des Zeitstempels.
   - Nach dem Neustart empfängt er wieder Werte, bis die Säule ihre Übertragung abschließt.
   - Der Simulator meldet den Abbruch, und die Säule sendet mit fortgesetztem Zählerstand (`--start-wh`) neu.
6. **Auswirkung auf andere Services:**
   - Der Session- und der ChargePointService sind nicht betroffen, die Session bleibt `ACTIVE`.
   - Die bis zum Ausfall im Speicher gehaltenen Messwerte sind verloren (In-Memory-Prototyp). `stop` rechnet nur die nach dem Neustart gemeldete Energie ab.
   - Genau hier läge der Vorteil eines Brokers mit persistenter Queue bzw. einer Datenbank (siehe ENTWURF.md, Abschnitt 2.3).
   - Fällt Metering beim `stop` aus, antwortet der SessionService mit `UNAVAILABLE` und lässt die Session `ACTIVE`, damit `stop` später wiederholt werden kann.
7. **Weiterarbeit:** ja. Neue Streams funktionieren sofort, und der Consumer erholt sich ohne manuellen Eingriff.
