# Entwurfsentscheidungen – Verteiltes Lademanagement mit gRPC

## 1. Architektur und Servicegrenzen

```
                 CreateChargingSession / Get / List /
                 CalculateSessionTariff / StopChargingSession        (Unary)
  CLI-Consumer ───────────────────────────────────────────────▶ ChargingSessionService :50052
  (client.py)                                                    │   Provider  ▲   Consumer ▼
      │                                                          │
      │ StreamMeterValues (Server Streaming)        ReserveChargePoint / ReleaseChargePoint /
      ▼                                             GetChargePoint (Unary, Deadline 2,0 s)
  MeteringService :50054 ◀── GetMeterSummary ──────────┤──────────────▶ ChargePointService :50051
      ▲                     (Unary, Deadline 2,0 s)     │
      │ ReportMeterValues                               └── CalculateTariff (Unary, Deadline 1,5 s)
      │ (Client Streaming)                                               ▼
  Ladesäule (simulator.py)                                       TariffService :50053
```

| Service | Verantwortung (fachlicher Kontext) | Daten |
|---|---|---|
| **ChargePointService** | Stammdaten und Belegungszustand der Ladesäulen, Reservieren/Freigeben | Ladesäulen |
| **ChargingSessionService** | Lebenszyklus eines Ladevorgangs (Orchestrator): anlegen, Tarif zuordnen, beenden und abrechnen | Ladevorgänge |
| **TariffService** | Preisbildung, Tarife ändern sich im Betrieb (`UpdateTariff`) | Tariftabelle mit Version |
| **MeteringService** | Messwerte annehmen, validieren, live verteilen, zusammenfassen | Messreihen je Session |

**Begründung der Schnitte:**
- Jeder Service besitzt genau seine Daten (*Single Source of Truth*). Nur der ChargePointService ändert den Belegungszustand einer Säule. Damit ist die Doppelbelegung an einer einzigen Stelle (unter einem Lock) ausgeschlossen.
- Tarife ändern sich unabhängig vom Ladevorgang und haben andere Stakeholder (Pricing). Der TariffService kann deshalb ohne Änderung des Session-Vertrags weiterentwickelt werden.
- Messwerte haben ein ganz anderes Lastprofil: Es fallen viele kleine Nachrichten in Echtzeit an, und zwar über Streams. Der MeteringService trennt dieses Profil von der transaktionalen Session-Verwaltung. Fällt Metering aus, können Ladevorgänge trotzdem weiter angelegt werden.
- **Doppelrolle:** Der ChargingSessionService ist Provider für den CLI-Consumer und gleichzeitig Consumer von drei Services. `CreateChargingSession` erfordert zwingend die Kommunikation mit ChargePoint und Tariff.
- *Alternative:* Ein einziger Monolith wäre einfacher, böte aber weder unabhängige Skalierung (Metering) noch eine Fehlerisolierung. Eine Choreografie über Events statt eines Orchestrators würde den Ablauf „Reservieren → Tarif → Start“ schwer nachvollziehbar machen. Weil jeder Schritt synchron auf das Ergebnis des vorherigen angewiesen ist, passt RPC hier besser.

**Ablauf `CreateChargingSession`** (`services/charging_session_service.py`):
1. Validierung (`charge_point_id`, `id_tag`), Session-ID vergeben (`cs-…`, Status `PENDING`)
2. `ReserveChargePoint`: Fehler werden übersetzt (siehe 2.4); noch nichts zu kompensieren
3. `CalculateTariff` mit **Deadline 1,5 s**. Bei einem Fehler folgt die **Kompensation** `ReleaseChargePoint`, die Session wird als `FAILED` gespeichert und der Fehler geht an den Consumer
4. Tarif zuordnen → `ACTIVE`

## 2.1 API-Vertrag ChargingSessionService

Datei: `protos/emobility/v1/charging_session.proto`.

| Entscheidung | Begründung |
|---|---|
| Paket `emobility.v1` | Versionierung im Paketnamen: Ein inkompatibler Bruch hieße `v2`. Kompatible Änderungen bleiben in `v1` (siehe 2.6). |
| `session_id` als `string`, vom **Provider** vergeben (`cs-<hex>`) | Opake ID: Der Consumer darf keine Struktur annehmen, und das Format kann später zu einer UUID oder einer DB-ID werden. So gibt es keine Kollisionen durch Consumer-IDs. |
| Enum `ChargingSessionStatus` mit `…_UNSPECIFIED = 0` | Der proto3-Default 0 bedeutet „nicht gesetzt“ und ist nie ein gültiger fachlicher Zustand. Präfix je Wert, weil Enum-Werte in Protobuf im Paket-Scope liegen. |
| Zustände `PENDING → ACTIVE → COMPLETED`, `FAILED` | Bilden den Ablauf mit zwei Downstream-Aufrufen ab. `FAILED` hält fehlgeschlagene Starts nachvollziehbar fest. |
| `Tariff tariff = 4` (eingebettete Message) | Ordnet den berechneten Tarif zu. Gespeichert wird eine **Kopie** des Tarifs (mit versionierter `tariff_id`), damit spätere Tarifänderungen eine laufende Abrechnung nicht verändern. |
| Preise als `Money {currency_code, units, nanos}` | **Kein float/double**: Binäre Fließkommazahlen können 0,59 € nicht exakt darstellen. Das Muster stammt von `google.type.Money`. |
| Leistung `uint32 max_power_w`, Energie `uint64 energy_wh` | Ganzzahlige SI-Basiseinheiten: eindeutig, ohne Rundungsfehler, der Wertebereich reicht aus. Die Einheit steht im Feldnamen. |
| Zeitpunkte als `google.protobuf.Timestamp` | Standardtyp, zeitzonenneutral (UTC), in allen Sprachen unterstützt. |
| Eigene Request/Response-Messages je RPC | Jede Operation kann unabhängig um Felder erweitert werden, ohne andere RPCs zu beeinflussen (AIP-Stil). |
| `Create`/`Get`/`Stop` geben die Ressource `ChargingSession` zurück | Der Consumer erhält den vollständigen Zustand inklusive vergebener ID und Tarif und braucht keinen zweiten Aufruf. |
| **Pagination** `page_size` + opaker `page_token` → `next_page_token` | Cursor-basiert statt Seitennummer. Das Token ist Base64-kodiert und für den Consumer bedeutungslos, die interne Umsetzung (Offset, später DB-Cursor) kann sich ändern. Default 10, maximal 100. Ein leerer `next_page_token` markiert die letzte Seite. |
| `CalculateSessionTariff(session_id) → Tariff` | „Tarif für einen Ladevorgang ermitteln“: Der aktuell gültige Tarif wird beim TariffService erfragt und der Session zugeordnet (z. B. nach `UpdateTariff`). Für `COMPLETED`/`FAILED` gilt `FAILED_PRECONDITION`. |
| Namen | Methoden nach `Verb + Ressource` (`CreateChargingSession`), Felder in `snake_case`, Services im Singular. Das entspricht den Google API Design Guidelines. |
| Feldnummern | 1–15 (ein Byte Tag) für häufige Felder, keine Lücken außer bei bewusst reservierten Nummern. |

## 2.2 ChargePointService

`ChargePoint`: `charge_point_id`, `station_id`, `display_name`, `connector_type` (Enum), `max_power_w`, `status`, `current_session_id`.

- **Verfügbarkeitsstatus** `AVAILABLE`, `OCCUPIED`, `OUT_OF_ORDER`. Nur `AVAILABLE` ist reservierbar.
- **Reserve-/Belegungssemantik:** `ReserveChargePoint(charge_point_id, session_id)` bindet die Säule exklusiv an genau einen Ladevorgang.
  - *Idempotent:* Ein wiederholter Aufruf mit derselben `session_id` (z. B. Retry nach Timeout) ist erfolgreich und keine Doppelbelegung.
  - `ReleaseChargePoint` darf nur die belegende Session aufrufen, sonst gibt es `PERMISSION_DENIED`. Ist die Säule bereits frei, ist der Aufruf ebenfalls idempotent erfolgreich.
- `connector_type` und `max_power_w` braucht der TariffService zur Preisbildung (AC vs. DC vs. HPC).

## 2.3 RPC-Muster, Streaming und Paradigmen-Abgrenzung

| RPC | Muster | Begründung |
|---|---|---|
| Create/Get/List/Stop, Reserve/Release, CalculateTariff, GetMeterSummary | **Unary** | Klassisches Request/Response mit genau einer Antwort. Leicht mit Deadlines und Status Codes abzusichern. |
| `ReportMeterValues(stream MeterValue) → ReportMeterValuesResponse` | **Client Streaming** | Die Ladesäule schickt fortlaufend viele Messwerte über *eine* HTTP/2-Verbindung, ohne Overhead pro Wert. Am Ende gibt es eine Quittung (Anzahl, Zählerstand). Ist ein Wert ungültig, wird der ganze Stream mit `INVALID_ARGUMENT` abgebrochen. |
| `StreamMeterValues(session_id) → stream MeterValue` | **Server Streaming** | Der Consumer abonniert die Messwerte eines Ladevorgangs live, statt zu pollen. Der Stream endet, wenn die Ladesäule ihre Übertragung abschließt. |
| Bidirectional Streaming | *nicht verwendet* | Wäre sinnvoll für eine dauerhafte Steuerverbindung zur Ladesäule (Messwerte hoch, Steuerbefehle wie „Leistung drosseln“ runter, vergleichbar mit OCPP). Für unsere Anwendungsfälle ist keine Gegenrichtung im selben Stream nötig. Das erhöht nur die Komplexität (Flusskontrolle, Fehlerbehandlung). |

**Warum gRPC-Streaming für `StreamMeterValues`?**
- Es gibt *einen* bekannten Consumer pro Stream, der gezielt *einen* Ladevorgang beobachtet: eine Punkt-zu-Punkt-Beziehung mit fachlichem Request (`session_id`).
- Der Stream hat eine klare Lebensdauer (Ladevorgang) und typisierte Nachrichten aus demselben Vertrag. HTTP/2-Flusskontrolle sorgt für Backpressure.
- Fehler kommen direkt beim Consumer als Status Code an (`UNAVAILABLE` → Wiederverbindung), und es ist keine zusätzliche Infrastruktur nötig.

**Wann eher Broker/Messaging (z. B. RabbitMQ, Kafka)?**
- **Event Broadcasting an viele unabhängige Consumer**: Wenn Abrechnung, Monitoring, Lastmanagement und Data-Warehouse jeden Messwert erhalten sollen, müsste der MeteringService bei gRPC jeden Consumer kennen und bedienen. Ein Broker entkoppelt Producer und Consumer (Publish/Subscribe).
- **Zeitliche Entkopplung und Zustellgarantie**: Ein Consumer, der gerade offline ist, soll Werte später nachholen (persistente Queues, Retention, Replay). Bei gRPC gehen Werte während einer Unterbrechung verloren. Im Prototyp hält der Server sie nur im Speicher (siehe Szenario 6).
- **Lastspitzen puffern** und Consumer unabhängig skalieren (Consumer Groups).
- Fazit: gRPC passt für gezielte Abfragen und Abos mit Rückkanal und Fehlercode. Ein Broker passt für fachliche Ereignisse („Ladevorgang beendet“), die viele unbekannte Abnehmer interessieren.

## 2.4 Fehlerbehandlung

| Situation | Entsteht in | Status Code | Begründung |
|---|---|---|---|
| Ladevorgang existiert nicht | ChargingSessionService | `NOT_FOUND` | Ressource fehlt |
| Ladesäule existiert nicht | ChargePointService → durchgereicht | `NOT_FOUND` | Fachlicher Fehler, vom Consumer verursacht (falsche ID), also gleiche Bedeutung für ihn |
| Ladesäule belegt / gesperrt | ChargePointService → durchgereicht | `FAILED_PRECONDITION` | Request ist gültig, aber der Systemzustand erlaubt ihn nicht. Ein Retry ohne Zustandsänderung ist zwecklos. Nicht `ALREADY_EXISTS` (die Session existiert ja nicht) und nicht `UNAVAILABLE` (das würde Retry suggerieren). |
| Ungültige Daten (leeres `id_tag`, `page_size < 0`, ungültiger `page_token`, sinkender Zählerstand, Temperatur außerhalb [-40, 120] °C) | jeweiliger Provider | `INVALID_ARGUMENT` | Unabhängig vom Systemzustand ungültig |
| Fremde Session will Säule freigeben | ChargePointService | `PERMISSION_DENIED` | |
| Downstream-Service nicht erreichbar | ChargingSessionService | **`UNAVAILABLE`** | siehe unten |
| Downstream-Service zu langsam | ChargingSessionService | `DEADLINE_EXCEEDED` | siehe 2.5 |
| Sonstige unerwartete Downstream-Fehler | ChargingSessionService | `INTERNAL` | Interna nicht nach außen geben |

**Service-Ausfall (ChargePointService nicht erreichbar):** Der ChargingSessionService erhält vom gRPC-Client `UNAVAILABLE` (Verbindung abgelehnt). Er gibt **`UNAVAILABLE`** mit eigener Fehlermeldung an den ursprünglichen Consumer zurück:
- `UNAVAILABLE` bedeutet laut gRPC-Spezifikation „vorübergehend, Retry mit Backoff sinnvoll“. Genau das trifft zu: Der Request war korrekt, und nach dem Neustart des Services gelingt er (Szenario 4).
- `INTERNAL` würde einen Bug im SessionService suggerieren und Retries verhindern. `FAILED_PRECONDITION` würde einen fachlichen Zustand behaupten, der nicht besteht.
- Der Code wird **nicht blind durchgereicht**: Die zentrale Funktion `translate()` entscheidet je Code. Fachliche Fehler (`NOT_FOUND`, `FAILED_PRECONDITION`, `INVALID_ARGUMENT`) gehen durch, weil der Consumer sie verursacht hat. Technische Fehler werden mit einer eigenen Meldung („ChargePointService nicht erreichbar“) neu formuliert. Unbekannte Codes werden zu `INTERNAL`.
- Konsistenz: Fällt der ChargePointService beim Reservieren aus, ist noch nichts belegt und es wird keine Session gespeichert. Fällt der Tarif aus, wird die Reservierung kompensiert.

**Consumer** (`client.py`, `handle_rpc_error`): fängt `grpc.RpcError` ab und wertet `err.code()`, `err.details()` und die Trailing Metadata (`x-session-id`) aus.
- Er gibt je Code eine passende Handlungsempfehlung aus.
- Exit-Code 2 kennzeichnet wiederholbare Fehler (`UNAVAILABLE`, `DEADLINE_EXCEEDED`), Exit-Code 1 kennzeichnet nicht wiederholbare Fehler.
- `watch` verbindet sich bei `UNAVAILABLE` automatisch mit exponentiellem Backoff neu.

## 2.5 Deadlines und Metadaten

**Deadlines:**
- **Wo:** Im ChargingSessionService bei jedem Downstream-Aufruf über `timeout=` (`_call()`):
  - TariffService 1,5 s (*Zeitbudget für den Preis*)
  - ChargePointService 2,0 s
  - MeteringService 2,0 s

  Der CLI-Consumer setzt selbst 5 s.
- **Deadline-Propagation:** `budget()` nimmt das Minimum aus eigenem Budget und Restzeit des eingehenden Aufrufs (`context.time_remaining()`). Ein Downstream-Aufruf wartet also nie länger, als der ursprüngliche Consumer noch wartet.
- **Auswirkung der Überschreitung:** Nach 1,5 s bricht gRPC den Tarifaufruf clientseitig mit `DEADLINE_EXCEEDED` ab, und der SessionService blockiert nicht weiter. Er gibt die reservierte Ladesäule wieder frei (Kompensation), speichert die Session als `FAILED` mit Grund und antwortet.
- **Status Code:** Es entsteht `DEADLINE_EXCEEDED` im SessionService (als Client des TariffService). Weitergegeben wird **`DEADLINE_EXCEEDED`** mit der Meldung „Ladevorgang cs-… nicht gestartet: TariffService hat nicht innerhalb des Zeitbudgets geantwortet“. Der Consumer weiß damit, dass der Vorgang nicht gestartet wurde und ein Retry sinnvoll ist. Die Session-ID kommt per Trailing Metadata `x-session-id` mit.
- **Auswirkung auf den TariffService:** Er erkennt per `context.is_active()`, dass der Aufrufer abgebrochen hat, und verwirft die Berechnung (Log „Aufrufer hat abgebrochen“). So wird keine Arbeit für eine Antwort verschwendet, die niemand mehr liest.
- `StreamMeterValues` läuft bewusst **ohne** Deadline, weil seine Laufzeit der Dauer des Ladevorgangs entspricht.

**Metadaten:**
- `x-correlation-id` wird vom Consumer pro Befehl erzeugt und vom SessionService bei *allen* Downstream-Aufrufen weitergegeben. Fehlt sie, erzeugt der Provider eine.
- `x-client-id` ist der Aufrufer: `cli-consumer`, `charge-point-cp-001`, `legacy-app-v1`, `charging-session-service`.
- Provider-Seite: Der `MetadataLoggingInterceptor` (`common/grpc_util.py`) liest die Werte bei jedem RPC aus und protokolliert sie. Alle Service-Logs eines Vorgangs lassen sich so über dieselbe ID verfolgen:
  ```
  charging-session [req-97493f04] <- CreateChargingSession  (client=cli-consumer)
  charge-point     [req-97493f04] <- ReserveChargePoint  (client=charging-session-service)
  tariff           [req-97493f04] <- CalculateTariff  (client=charging-session-service)
  ```
- **Warum Metadata statt Request-Feld?** Correlation-ID und Client-ID sind *technische Querschnittsinformationen* und kein Teil des fachlichen Vertrags:
  - Sie betreffen alle RPCs gleichermaßen und würden sonst in jeder Request-Message dupliziert.
  - Sie werden von Infrastruktur ausgewertet (Interceptor, Tracing, Gateway, Auth) und nicht von der Fachlogik.
  - Sie lassen sich ergänzen oder ändern, ohne einen `.proto`-Vertrag anzupassen.

  Dasselbe gilt für Authentifizierung (`authorization`-Header).

## 2.6 API-Evolution und Kompatibilität

Der Vertrag `charging_session.proto` wurde von **Version 1** (eingefroren in `protos_legacy/`) zu **Version 2** (`protos/`) weiterentwickelt.

**Version 1 (ursprünglich):**
```proto
enum ChargingSessionStatus {
  CHARGING_SESSION_STATUS_UNSPECIFIED = 0;
  CHARGING_SESSION_STATUS_PENDING = 1;
  CHARGING_SESSION_STATUS_ACTIVE = 2;
  CHARGING_SESSION_STATUS_COMPLETED = 3;
  CHARGING_SESSION_STATUS_FAILED = 4;
}
message ChargingSession {
  ...
  string vehicle_plate = 5;
  ...
  string failure_reason = 11;
}
message CreateChargingSessionRequest {
  string charge_point_id = 1;
  string id_tag = 2;
  string vehicle_plate = 3;
}
```

**Version 2 (weiterentwickelt):**
```proto
enum ChargingSessionStatus {
  ...
  CHARGING_SESSION_STATUS_FAILED = 4;
  CHARGING_SESSION_STATUS_SUSPENDED = 5;   // (1) neuer Enum-Wert
}
message ChargingSession {
  ...
  reserved 5;                              // (2) Feld entfernt, Nummer + Name gesperrt
  reserved "vehicle_plate";
  ...
  string failure_reason = 11;
  string customer_id = 12;                 // (3) neues Feld
}
message CreateChargingSessionRequest {
  string charge_point_id = 1;
  string id_tag = 2;
  reserved 3;                              // (2) Feld entfernt
  reserved "vehicle_plate";
  string customer_id = 4;                  // (3) Request erweitert
}
```

| Änderung | Auswirkung auf bestehende Consumer | Kompatibilität |
|---|---|---|
| (1) Enum-Wert `SUSPENDED = 5` | Ein v1-Consumer kennt den Namen nicht. Python liefert den Rohwert 5, Java `UNRECOGNIZED`. Er muss mit unbekannten Werten rechnen (Default-Zweig). | Neue Nummer, bestehende Nummern bleiben gleich. Der Wire-Format-kompatible Wert geht nicht verloren. |
| (2) `vehicle_plate` entfernt (Datenschutz: Kennzeichen sind personenbezogen und fachlich nicht nötig) | Ein v1-Consumer darf das Feld weiter senden. Der v2-Provider behandelt es als unbekanntes Feld und ignoriert es. In Antworten fehlt es, also liest v1 `""`. | `reserved 5` / `reserved 3` und der reservierte Name verhindern, dass ein späterer Entwickler die Nummer mit **anderem Typ oder anderer Bedeutung** wiederverwendet. Sonst würden alte Consumer Daten falsch interpretieren. protoc lehnt eine Wiederverwendung mit einem Fehler ab. |
| (3) `customer_id` (Feld 12 bzw. 4) | Keine: Alte Consumer senden es nicht (Default `""` = „kein Kunde“) und ignorieren es in Antworten. | Neue, nie benutzte Feldnummer. Das Feld ist optional, und der Default hat eine sinnvolle Bedeutung. |

**Nachweis:** `python legacy_client.py` nutzt die **v1-Stubs** (`legacy_gen/`) gegen den v2-Provider. Er sendet `vehicle_plate`, legt erfolgreich einen Ladevorgang an und beendet ihn wieder. Inkompatibel (und deshalb vermieden) wären: Feldnummer oder Feldtyp ändern, ein Feld umbenennen und dabei die Nummer wiederverwenden, ein Pflicht-Semantik-Feld ohne sinnvollen Default hinzufügen, RPCs umbenennen. Solche Brüche bräuchten ein neues Paket `emobility.v2` parallel zu `v1`.

## Grenzen und mögliche Verbesserungen
- Datenhaltung nur im Speicher: Bei einem Neustart gehen Sessions bzw. Messwerte verloren (sichtbar in Szenario 6). Abhilfe wäre eine Datenbank oder ein Event-Log.
- Die Kompensation im Create-Ablauf ist eine einfache Saga ohne Persistenz. Ein Absturz zwischen Reserve und Release würde eine Säule blockieren. Abhilfe wäre eine Reservierung mit Ablaufzeit (TTL) im ChargePointService.
- Keine automatischen Retries im SessionService. Für idempotente Aufrufe (Reserve mit derselben `session_id`) wäre eine gRPC-Retry-Policy per Service Config möglich.
- Keine TLS/Authentifizierung (`insecure_channel`). In Produktion bräuchte es mTLS zwischen den Services und ein Token im `authorization`-Metadata.
- Die Blockiergebühr wird im Tarif geliefert, aber noch nicht abgerechnet.
