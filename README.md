# Bonusaufgabe 2 – Verteiltes Lademanagement mit gRPC und Protocol Buffers

API-Design und Systemintegration, WS 2026/27. Vier Python-Services kommunizieren über gRPC, ihre Verträge sind mit Protocol Buffers definiert.

- Entwurfsentscheidungen und Begründungen: [`ENTWURF.md`](ENTWURF.md)
- Resilienztests: [`scenarios.md`](scenarios.md)

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m tools.gen_protos      # optional – die generierten Stubs sind eingecheckt
```

## Start (je ein Terminal, im Projektverzeichnis)

```bash
python -m services.charge_point_service      # :50051
python -m services.charging_session_service  # :50052
python -m services.tariff_service            # :50053   (--delay 3 für den Deadline-Test)
python -m services.metering_service          # :50054
```

## Consumer

```bash
python client.py demo                        # Happy Path: anlegen -> Messwerte live -> beenden
python client.py points                      # Ladesäulen und Belegung
python client.py create cp-001 --id-tag RFID-4711 --customer-id K-100
python client.py get <session_id>
python client.py list --page-size 2          # Pagination über alle Seiten
python client.py tariff <session_id>         # Tarif neu ermitteln und zuordnen
python client.py set-tariff DC_HPC 0.79      # Tarifänderung im Betrieb
python client.py watch <session_id>          # Server Streaming
python simulator.py <session_id> --cp cp-001 # Ladesäule: Client Streaming
python client.py stop <session_id>           # beenden + abrechnen
python legacy_client.py                      # v1-Consumer gegen v2-Provider (API-Evolution)
python -m tools.run_scenarios                # alle Resilienztests automatisch
```

Ports, Adressen und Deadlines lassen sich per Umgebungsvariable ändern (`common/config.py`), z. B. `TARIFF_DEADLINE_S=0.5`.

## Struktur

| Pfad | Inhalt |
|---|---|
| `protos/emobility/v1/*.proto` | API-Verträge (aktuell, v2 von `charging_session.proto`) |
| `protos_legacy/emobility/v1/charging_session.proto` | ursprüngliche Version 1 (eingefroren) |
| `emobility/v1/`, `legacy_gen/` | generierte Stubs |
| `services/charging_session_service.py` | Orchestrator: Doppelrolle, Deadlines, Fehlerübersetzung, Kompensation |
| `services/charge_point_service.py` | Ladesäulen, Reserve/Release (idempotent) |
| `services/tariff_service.py` | Tarifberechnung, Tarifänderung, `--delay` |
| `services/metering_service.py` | Client und Server Streaming, Validierung |
| `common/grpc_util.py` | Metadata-Interceptor, Deadline-Propagation, Money/Timestamp-Helfer |
| `client.py` | CLI-Consumer mit zentraler `RpcError`-Behandlung und Stream-Wiederverbindung |
| `simulator.py` | Ladesäulen-Simulator (Client Streaming) |
| `tools/run_scenarios.py` | reproduzierbare Fehlerszenarien |

## RPC-Muster im Überblick

| Muster | RPC |
|---|---|
| Unary | alle RPCs von ChargingSession-, ChargePoint- und TariffService, `GetMeterSummary` |
| Client Streaming | `MeteringService.ReportMeterValues` |
| Server Streaming | `MeteringService.StreamMeterValues` |
