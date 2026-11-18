"""Simuliert eine Ladesäule, die Messwerte per Client Streaming an den MeteringService sendet.

    python simulator.py <session_id> [--cp cp-001] [--count 20] [--interval 0.5] [--invalid]

--start-wh setzt den Zählerstand fort (z. B. nach Neustart des MeteringService).
--invalid sendet nach einigen Werten einen sinkenden Zählerstand -> INVALID_ARGUMENT.
"""
from __future__ import annotations

import argparse
import random
import sys
import time

import grpc

from common import config
from common.grpc_util import new_client_metadata, now_ts
from emobility.v1 import metering_pb2, metering_pb2_grpc


def meter_values(session_id: str, charge_point_id: str, count: int, interval: float, invalid: bool, quiet: bool,
                 start_wh: int = 0):
    rng = random.Random(42)
    energy = float(start_wh)
    soc = 20
    for i in range(count):
        power = int(rng.uniform(90_000, 140_000))
        energy += power * interval / 3600 * 60  # Zeitraffer: 1 s Simulation = 1 min Laden
        soc = min(100, soc + 2)
        value = metering_pb2.MeterValue(
            session_id=session_id, charge_point_id=charge_point_id, timestamp=now_ts(),
            energy_wh=int(energy), power_w=power, temperature_celsius=round(rng.uniform(30, 45), 1),
            state_of_charge_percent=soc)
        if invalid and i == 3:
            value.energy_wh = 1  # Zähler "springt zurück" -> fachlich ungültig
        if not quiet:
            print(f"  -> {value.energy_wh:6d} Wh  {value.power_w / 1000:6.1f} kW")
        yield value
        time.sleep(interval)


def simulate(session_id: str, charge_point_id: str = "cp-001", count: int = 20, interval: float = 0.5,
             invalid: bool = False, quiet: bool = False, start_wh: int = 0) -> metering_pb2.ReportMeterValuesResponse:
    stub = metering_pb2_grpc.MeteringServiceStub(grpc.insecure_channel(config.METERING_ADDR))
    return stub.ReportMeterValues(
        meter_values(session_id, charge_point_id, count, interval, invalid, quiet, start_wh),
        metadata=new_client_metadata(f"charge-point-{charge_point_id}"),
        timeout=count * interval + 10,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("session_id")
    ap.add_argument("--cp", default="cp-001")
    ap.add_argument("--count", type=int, default=20)
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--invalid", action="store_true")
    ap.add_argument("--start-wh", type=int, default=0, help="Zählerstand zu Beginn (Wiederaufnahme)")
    args = ap.parse_args()
    try:
        resp = simulate(args.session_id, args.cp, args.count, args.interval, args.invalid, start_wh=args.start_wh)
    except grpc.RpcError as err:
        print(f"FEHLER {err.code().name}: {err.details()}", file=sys.stderr)
        return 1
    print(f"Quittung: {resp.accepted_count} Werte akzeptiert, Zählerstand {resp.energy_wh} Wh")
    return 0


if __name__ == "__main__":
    sys.exit(main())
