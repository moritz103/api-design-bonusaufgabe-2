"""TariffService – berechnet Tarife, Tarife können im Betrieb geändert werden (Port 50053).

    python -m services.tariff_service [--delay 3]

--delay verzögert CalculateTariff gezielt (reproduzierbare Deadline-Überschreitung).
"""
from __future__ import annotations

import argparse
import threading
import time
from decimal import Decimal

import grpc

from common import config
from common.grpc_util import correlation_id, fmt_money, make_server, money, now_ts, setup_logging, to_decimal
from emobility.v1 import charge_point_pb2 as cp_pb2
from emobility.v1 import tariff_pb2, tariff_pb2_grpc

log = setup_logging("tariff")

T = tariff_pb2.TariffType


class TariffServicer(tariff_pb2_grpc.TariffServiceServicer):
    def __init__(self, delay_s: float = 0.0):
        self._delay_s = delay_s
        self._lock = threading.Lock()
        created = now_ts()
        # Tariftabelle: Typ -> (Preis/kWh, Version, gültig ab)
        self._table = {
            T.TARIFF_TYPE_AC_STANDARD: [Decimal("0.49"), 1, created],
            T.TARIFF_TYPE_DC_FAST: [Decimal("0.59"), 1, created],
            T.TARIFF_TYPE_DC_HPC: [Decimal("0.69"), 1, created],
        }

    @staticmethod
    def _classify(request) -> int:
        if request.connector_type == cp_pb2.CONNECTOR_TYPE_TYPE2:
            return T.TARIFF_TYPE_AC_STANDARD
        if request.max_power_w >= 150_000:
            return T.TARIFF_TYPE_DC_HPC
        return T.TARIFF_TYPE_DC_FAST

    def _slow_down(self, context, cid: str) -> bool:
        """Verzögert in kleinen Schritten und prüft, ob der Aufrufer noch wartet.
        Gibt False zurück, wenn der Aufrufer abgebrochen hat (Deadline abgelaufen)."""
        log.info("[%s]    simuliere langsame Berechnung (%.1fs)", cid, self._delay_s)
        end = time.monotonic() + self._delay_s
        while time.monotonic() < end:
            if not context.is_active():
                log.warning("[%s]    Aufrufer hat abgebrochen (Deadline abgelaufen) – Berechnung verworfen", cid)
                return False
            time.sleep(0.05)
        return True

    def CalculateTariff(self, request, context):
        cid = correlation_id(context)
        if not request.charge_point_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "charge_point_id darf nicht leer sein")
        if request.max_power_w == 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "max_power_w muss > 0 sein")
        if self._delay_s and not self._slow_down(context, cid):
            context.abort(grpc.StatusCode.CANCELLED, "Aufrufer nicht mehr aktiv")

        ttype = self._classify(request)
        with self._lock:
            price, version, valid_from = self._table[ttype]
        price_md = money(price)
        name = tariff_pb2.TariffType.Name(ttype).removeprefix("TARIFF_TYPE_").lower().replace("_", "-")
        tariff = tariff_pb2.Tariff(
            tariff_id=f"{name}@v{version}",
            tariff_type=ttype,
            price_per_kwh=price_md,
            blocking_fee_per_minute=money("0.10"),
            blocking_fee_after_minutes=240 if ttype == T.TARIFF_TYPE_AC_STANDARD else 45,
            valid_from=valid_from,
            quoted_at=now_ts(),
        )
        log.info("[%s]    Tarif %s: %s/kWh", cid, tariff.tariff_id, fmt_money(price_md))
        return tariff

    def UpdateTariff(self, request, context):
        cid = correlation_id(context)
        if request.tariff_type not in self._table:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unbekannter oder fehlender tariff_type")
        new_price = to_decimal(request.price_per_kwh)
        if new_price <= 0 or new_price > 5:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "price_per_kwh muss in (0, 5] EUR liegen")
        with self._lock:
            entry = self._table[request.tariff_type]
            entry[0], entry[1], entry[2] = new_price, entry[1] + 1, now_ts()
            price, version, valid_from = entry
        name = tariff_pb2.TariffType.Name(request.tariff_type)
        log.info("[%s]    Tarifänderung %s -> %s EUR/kWh (v%d)", cid, name, new_price, version)
        return tariff_pb2.Tariff(
            tariff_id=f"{name.removeprefix('TARIFF_TYPE_').lower().replace('_', '-')}@v{version}",
            tariff_type=request.tariff_type, price_per_kwh=money(price), valid_from=valid_from,
            quoted_at=now_ts(),
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=config.TARIFF_PORT)
    ap.add_argument("--delay", type=float, default=0.0, help="Verzögerung von CalculateTariff in s")
    args = ap.parse_args()

    server = make_server(log)
    tariff_pb2_grpc.add_TariffServiceServicer_to_server(TariffServicer(args.delay), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    log.info("TariffService läuft auf Port %d (delay=%.1fs)", args.port, args.delay)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
