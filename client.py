"""Kommandozeilen-Consumer.

    python client.py points
    python client.py create cp-001 --id-tag RFID-4711 [--customer-id K-100]
    python client.py get <session_id>
    python client.py list [--page-size 2] [--status ACTIVE]
    python client.py tariff <session_id>
    python client.py watch <session_id> [--history]
    python client.py stop <session_id>
    python client.py set-tariff DC_FAST 0.65
    python client.py demo [--cp cp-001]

Jeder Aufruf schickt eine neue Correlation-ID und die Client-ID als gRPC-Metadata.
Fehler werden als grpc.RpcError erkannt und je nach Status Code behandelt.
"""
from __future__ import annotations

import argparse
import sys
import threading
import time

import grpc

from common import config
from common.grpc_util import fmt_money, fmt_ts, money, new_client_metadata
from emobility.v1 import charge_point_pb2 as cp_pb2
from emobility.v1 import charge_point_pb2_grpc as cp_grpc
from emobility.v1 import charging_session_pb2 as cs_pb2
from emobility.v1 import charging_session_pb2_grpc as cs_grpc
from emobility.v1 import metering_pb2, metering_pb2_grpc
from emobility.v1 import tariff_pb2, tariff_pb2_grpc

CLIENT_ID = "cli-consumer"


def short(enum_type, value: int, prefix: str) -> str:
    return enum_type.Name(value).removeprefix(prefix)


def print_session(s: cs_pb2.ChargingSession) -> None:
    print(f"  Session   {s.session_id}  [{short(cs_pb2.ChargingSessionStatus, s.status, 'CHARGING_SESSION_STATUS_')}]")
    print(f"  Säule     {s.charge_point_id}   id_tag={s.id_tag}   customer={s.customer_id or '-'}")
    if s.tariff.tariff_id:
        print(f"  Tarif     {s.tariff.tariff_id}: {fmt_money(s.tariff.price_per_kwh)}/kWh")
    print(f"  Zeit      {fmt_ts(s.created_at)} – {fmt_ts(s.ended_at)}")
    if s.status == cs_pb2.CHARGING_SESSION_STATUS_COMPLETED:
        print(f"  Energie   {s.energy_delivered_wh / 1000:.3f} kWh   Kosten {fmt_money(s.total_cost)}")
    if s.failure_reason:
        print(f"  Grund     {s.failure_reason}")


def handle_rpc_error(err: grpc.RpcError, md) -> int:
    """Zentrale Fehlerbehandlung des Consumers: Status Code -> passende Reaktion."""
    code = err.code()
    cid = dict(md).get(config.MD_CORRELATION_ID)
    print(f"FEHLER {code.name}: {err.details()}   (correlation-id={cid})", file=sys.stderr)
    trailing = dict(err.trailing_metadata() or ())
    if config.MD_SESSION_ID in trailing:
        print(f"  betroffene Session: {trailing[config.MD_SESSION_ID]}", file=sys.stderr)
    hints = {
        grpc.StatusCode.NOT_FOUND: "ID prüfen (python client.py list / points).",
        grpc.StatusCode.INVALID_ARGUMENT: "Eingabe korrigieren – Wiederholung ohne Änderung ist zwecklos.",
        grpc.StatusCode.FAILED_PRECONDITION: "Andere Ladesäule wählen oder Zustand prüfen.",
        grpc.StatusCode.UNAVAILABLE: "Dienst vorübergehend nicht erreichbar – später erneut versuchen.",
        grpc.StatusCode.DEADLINE_EXCEEDED: "Zeitbudget überschritten – Vorgang wurde nicht gestartet, erneut versuchen.",
    }
    print(f"  -> {hints.get(code, 'Unerwarteter Fehler.')}", file=sys.stderr)
    return 2 if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED) else 1


class Api:
    def __init__(self):
        self.sessions = cs_grpc.ChargingSessionServiceStub(grpc.insecure_channel(config.CHARGING_SESSION_ADDR))
        self.points = cp_grpc.ChargePointServiceStub(grpc.insecure_channel(config.CHARGE_POINT_ADDR))
        self.tariffs = tariff_pb2_grpc.TariffServiceStub(grpc.insecure_channel(config.TARIFF_ADDR))
        self.metering = metering_pb2_grpc.MeteringServiceStub(grpc.insecure_channel(config.METERING_ADDR))


# ============================================================================ Befehle
def cmd_points(api: Api, args, md):
    resp = api.points.ListChargePoints(cp_pb2.ListChargePointsRequest(), timeout=config.CLIENT_DEADLINE_S,
                                       metadata=md)
    for p in resp.charge_points:
        print(f"  {p.charge_point_id}  {short(cp_pb2.ChargePointStatus, p.status, 'CHARGE_POINT_STATUS_'):13}"
              f" {short(cp_pb2.ConnectorType, p.connector_type, 'CONNECTOR_TYPE_'):7}"
              f" {p.max_power_w / 1000:>5.0f} kW  {p.display_name}  {p.current_session_id}")


def cmd_create(api: Api, args, md):
    s = api.sessions.CreateChargingSession(
        cs_pb2.CreateChargingSessionRequest(charge_point_id=args.charge_point_id, id_tag=args.id_tag,
                                            customer_id=args.customer_id),
        timeout=config.CLIENT_DEADLINE_S, metadata=md)
    print("Ladevorgang angelegt:")
    print_session(s)
    return s


def cmd_get(api: Api, args, md):
    print_session(api.sessions.GetChargingSession(
        cs_pb2.GetChargingSessionRequest(session_id=args.session_id), timeout=config.CLIENT_DEADLINE_S, metadata=md))


def cmd_list(api: Api, args, md):
    status = getattr(cs_pb2, f"CHARGING_SESSION_STATUS_{args.status}") if args.status else 0
    token, page = "", 1
    while True:
        resp = api.sessions.ListChargingSessions(
            cs_pb2.ListChargingSessionsRequest(page_size=args.page_size, page_token=token, status_filter=status),
            timeout=config.CLIENT_DEADLINE_S, metadata=md)
        print(f"--- Seite {page} ({len(resp.charging_sessions)} von insgesamt {resp.total_size}), "
              f"next_page_token={resp.next_page_token or '<leer>'}")
        for s in resp.charging_sessions:
            print(f"  {s.session_id}  {short(cs_pb2.ChargingSessionStatus, s.status, 'CHARGING_SESSION_STATUS_'):10}"
                  f" {s.charge_point_id}  {s.tariff.tariff_id or '-'}")
        token, page = resp.next_page_token, page + 1
        if not token or args.single_page:
            break


def cmd_tariff(api: Api, args, md):
    t = api.sessions.CalculateSessionTariff(
        cs_pb2.CalculateSessionTariffRequest(session_id=args.session_id), timeout=config.CLIENT_DEADLINE_S,
        metadata=md)
    print(f"  Tarif {t.tariff_id} ({short(tariff_pb2.TariffType, t.tariff_type, 'TARIFF_TYPE_')}): "
          f"{fmt_money(t.price_per_kwh)}/kWh, Blockiergebühr {fmt_money(t.blocking_fee_per_minute)}/min "
          f"ab {t.blocking_fee_after_minutes} min")


def cmd_set_tariff(api: Api, args, md):
    t = api.tariffs.UpdateTariff(
        tariff_pb2.UpdateTariffRequest(tariff_type=getattr(tariff_pb2, f"TARIFF_TYPE_{args.tariff_type}"),
                                       price_per_kwh=money(args.price)),
        timeout=config.CLIENT_DEADLINE_S, metadata=md)
    print(f"  Tarif geändert: {t.tariff_id} = {fmt_money(t.price_per_kwh)}/kWh")


def cmd_stop(api: Api, args, md):
    s = api.sessions.StopChargingSession(cs_pb2.StopChargingSessionRequest(session_id=args.session_id),
                                         timeout=config.CLIENT_DEADLINE_S, metadata=md)
    print("Ladevorgang beendet:")
    print_session(s)


def watch(api: Api, session_id: str, md, history: bool = False, max_retries: int = 5) -> int:
    """Server Streaming mit Wiederverbindung: bei UNAVAILABLE wird mit Backoff neu abonniert,
    bereits empfangene Werte werden anhand des Zeitstempels übersprungen."""
    last_ts = None
    retries = 0
    while True:
        stream = api.metering.StreamMeterValues(
            metering_pb2.StreamMeterValuesRequest(session_id=session_id, include_history=history or last_ts is not None),
            metadata=md)  # bewusst ohne Deadline: Stream läuft so lange wie der Ladevorgang
        try:
            for v in stream:
                ts = (v.timestamp.seconds, v.timestamp.nanos)
                if last_ts is not None and ts <= last_ts:
                    continue
                last_ts = ts
                retries = 0
                print(f"  {fmt_ts(v.timestamp)}  {v.energy_wh / 1000:7.3f} kWh  {v.power_w / 1000:6.1f} kW  "
                      f"{v.temperature_celsius:5.1f} °C  SoC {v.state_of_charge_percent:3d} %")
            print("  Stream beendet (Ladesäule hat Übertragung abgeschlossen).")
            return 0
        except grpc.RpcError as err:
            if err.code() == grpc.StatusCode.CANCELLED:
                return 0
            if err.code() != grpc.StatusCode.UNAVAILABLE or retries >= max_retries:
                return handle_rpc_error(err, md)
            retries += 1
            wait = min(0.5 * 2 ** retries, 8)
            print(f"  Stream unterbrochen ({err.code().name}) – Wiederverbindung {retries}/{max_retries} "
                  f"in {wait:.1f}s …", file=sys.stderr)
            time.sleep(wait)


def cmd_watch(api: Api, args, md):
    return watch(api, args.session_id, md, args.history)


def cmd_demo(api: Api, args, md):
    """Happy Path: anlegen -> Messwerte streamen (Client Streaming) + live mitlesen (Server Streaming) -> beenden."""
    from simulator import simulate

    print("== 1. Ladevorgang anlegen (Session -> ChargePoint + Tariff)")
    s = cmd_create(api, argparse.Namespace(charge_point_id=args.cp, id_tag="RFID-DEMO", customer_id="K-100"), md)
    print("\n== 2. Ladesäule sendet Messwerte (Client Streaming), Consumer liest live mit (Server Streaming)")
    sim = threading.Thread(target=simulate, kwargs=dict(session_id=s.session_id, charge_point_id=args.cp,
                                                        count=args.count, interval=0.4, quiet=True), daemon=True)
    watcher = threading.Thread(target=watch, args=(api, s.session_id, md, True))
    watcher.start()
    time.sleep(0.3)
    sim.start()
    sim.join()
    watcher.join(timeout=5)
    print("\n== 3. Ladevorgang beenden (Session -> Metering + ChargePoint)")
    cmd_stop(api, argparse.Namespace(session_id=s.session_id), md)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("points").set_defaults(fn=cmd_points)
    p = sub.add_parser("create"); p.add_argument("charge_point_id"); p.add_argument("--id-tag", default="RFID-4711")
    p.add_argument("--customer-id", default=""); p.set_defaults(fn=cmd_create)
    for name, fn in (("get", cmd_get), ("tariff", cmd_tariff), ("stop", cmd_stop)):
        p = sub.add_parser(name); p.add_argument("session_id"); p.set_defaults(fn=fn)
    p = sub.add_parser("watch"); p.add_argument("session_id"); p.add_argument("--history", action="store_true")
    p.set_defaults(fn=cmd_watch)
    p = sub.add_parser("list"); p.add_argument("--page-size", type=int, default=3)
    p.add_argument("--status", choices=["PENDING", "ACTIVE", "COMPLETED", "FAILED", "SUSPENDED"])
    p.add_argument("--single-page", action="store_true"); p.set_defaults(fn=cmd_list)
    p = sub.add_parser("set-tariff"); p.add_argument("tariff_type", choices=["AC_STANDARD", "DC_FAST", "DC_HPC"])
    p.add_argument("price"); p.set_defaults(fn=cmd_set_tariff)
    p = sub.add_parser("demo"); p.add_argument("--cp", default="cp-001"); p.add_argument("--count", type=int, default=10)
    p.set_defaults(fn=cmd_demo)
    args = ap.parse_args()

    api = Api()
    md = new_client_metadata(CLIENT_ID)
    try:
        rc = args.fn(api, args, md)
        return rc if isinstance(rc, int) else 0
    except grpc.RpcError as err:
        return handle_rpc_error(err, md)


if __name__ == "__main__":
    sys.exit(main())
