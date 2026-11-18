"""MeteringService – nimmt Messwerte entgegen und stellt sie live bereit (Port 50054).

    python -m services.metering_service

RPC-Muster:
- ReportMeterValues  : Client Streaming  (Ladesäule -> Metering)
- StreamMeterValues  : Server Streaming  (Metering -> Consumer, live)
- GetMeterSummary    : Unary             (ChargingSessionService beim Beenden)
"""
from __future__ import annotations

import argparse
import threading
from dataclasses import dataclass, field

import grpc

from common import config
from common.grpc_util import correlation_id, make_server, setup_logging
from emobility.v1 import metering_pb2, metering_pb2_grpc

log = setup_logging("metering")


@dataclass
class SessionSeries:
    values: list = field(default_factory=list)
    completed: bool = False


class MeteringServicer(metering_pb2_grpc.MeteringServiceServicer):
    def __init__(self):
        self._series: dict[str, SessionSeries] = {}
        self._cond = threading.Condition()

    # ------------------------------------------------------------------ Validierung
    @staticmethod
    def _validate(value, previous) -> str | None:
        if not value.session_id:
            return "session_id darf nicht leer sein"
        if not value.timestamp.seconds:
            return "timestamp fehlt"
        if not -40.0 <= value.temperature_celsius <= 120.0:
            return f"temperature_celsius={value.temperature_celsius} außerhalb [-40, 120]"
        if value.state_of_charge_percent > 100:
            return f"state_of_charge_percent={value.state_of_charge_percent} > 100"
        if previous is not None:
            if value.session_id != previous.session_id:
                return "alle Werte eines Streams müssen zur selben session_id gehören"
            if value.energy_wh < previous.energy_wh:
                return (f"energy_wh ist kumulativ und darf nicht sinken "
                        f"({previous.energy_wh} -> {value.energy_wh})")
        return None

    # ------------------------------------------------------------------ Client Streaming
    def ReportMeterValues(self, request_iterator, context):
        cid = correlation_id(context)
        previous = None
        count = 0
        session_id = ""
        for value in request_iterator:
            error = self._validate(value, previous)
            if error:
                log.warning("[%s]    ungültiger Messwert: %s", cid, error)
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, error)
            session_id = value.session_id
            with self._cond:
                self._series.setdefault(session_id, SessionSeries()).values.append(value)
                self._cond.notify_all()
            previous = value
            count += 1
        if count == 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Stream enthielt keine Messwerte")
        with self._cond:
            self._series[session_id].completed = True
            self._cond.notify_all()
        log.info("[%s]    %d Messwerte für %s empfangen, Zähler %d Wh",
                 cid, count, session_id, previous.energy_wh)
        return metering_pb2.ReportMeterValuesResponse(
            session_id=session_id, accepted_count=count, energy_wh=previous.energy_wh)

    # ------------------------------------------------------------------ Server Streaming
    def StreamMeterValues(self, request, context):
        cid = correlation_id(context)
        if not request.session_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "session_id darf nicht leer sein")
        sid = request.session_id
        with self._cond:
            series = self._series.setdefault(sid, SessionSeries())
            next_index = 0 if request.include_history else len(series.values)
        log.info("[%s]    Abonnement für %s gestartet (ab Index %d)", cid, sid, next_index)

        sent = 0
        while context.is_active():
            with self._cond:
                self._cond.wait_for(
                    lambda: len(series.values) > next_index or series.completed or not context.is_active(),
                    timeout=1.0,
                )
                batch = series.values[next_index:]
                next_index += len(batch)
                done = series.completed and next_index >= len(series.values)
            for value in batch:
                yield value
                sent += 1
            if done:
                break
        log.info("[%s]    Abonnement für %s beendet (%d Werte gesendet, aktiv=%s)",
                 cid, sid, sent, context.is_active())

    # ------------------------------------------------------------------ Unary
    def GetMeterSummary(self, request, context):
        if not request.session_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "session_id darf nicht leer sein")
        with self._cond:
            series = self._series.get(request.session_id)
            values = list(series.values) if series else []
            completed = series.completed if series else False
        summary = metering_pb2.MeterSummary(session_id=request.session_id, completed=completed,
                                            value_count=len(values))
        if values:
            summary.energy_wh = values[-1].energy_wh
            summary.max_power_w = max(v.power_w for v in values)
            summary.max_temperature_celsius = max(v.temperature_celsius for v in values)
            summary.first_timestamp.CopyFrom(values[0].timestamp)
            summary.last_timestamp.CopyFrom(values[-1].timestamp)
        return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=config.METERING_PORT)
    args = ap.parse_args()

    server = make_server(log, max_workers=32)  # Streams belegen je einen Worker-Thread
    metering_pb2_grpc.add_MeteringServiceServicer_to_server(MeteringServicer(), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    log.info("MeteringService läuft auf Port %d", args.port)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
