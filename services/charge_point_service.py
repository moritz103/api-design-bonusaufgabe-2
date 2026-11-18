"""ChargePointService – verwaltet Ladesäulen und deren Belegung (Port 50051).

    python -m services.charge_point_service [--delay 0]
"""
from __future__ import annotations

import argparse
import threading
import time

import grpc

from common import config
from common.grpc_util import correlation_id, make_server, setup_logging
from emobility.v1 import charge_point_pb2 as cp_pb2
from emobility.v1 import charge_point_pb2_grpc as cp_grpc

log = setup_logging("charge-point")

S = cp_pb2.ChargePointStatus
C = cp_pb2.ConnectorType


def seed() -> dict[str, cp_pb2.ChargePoint]:
    points = [
        cp_pb2.ChargePoint(charge_point_id="cp-001", station_id="st-wolfenbuettel-01",
                           display_name="Salzdahlumer Str. – HPC links", connector_type=C.CONNECTOR_TYPE_CCS,
                           max_power_w=150_000, status=S.CHARGE_POINT_STATUS_AVAILABLE),
        cp_pb2.ChargePoint(charge_point_id="cp-002", station_id="st-wolfenbuettel-01",
                           display_name="Salzdahlumer Str. – AC", connector_type=C.CONNECTOR_TYPE_TYPE2,
                           max_power_w=22_000, status=S.CHARGE_POINT_STATUS_AVAILABLE),
        cp_pb2.ChargePoint(charge_point_id="cp-003", station_id="st-braunschweig-02",
                           display_name="Hauptbahnhof – DC", connector_type=C.CONNECTOR_TYPE_CCS,
                           max_power_w=50_000, status=S.CHARGE_POINT_STATUS_OUT_OF_ORDER),
        cp_pb2.ChargePoint(charge_point_id="cp-004", station_id="st-braunschweig-02",
                           display_name="Hauptbahnhof – HPC", connector_type=C.CONNECTOR_TYPE_CCS,
                           max_power_w=300_000, status=S.CHARGE_POINT_STATUS_AVAILABLE),
    ]
    return {p.charge_point_id: p for p in points}


class ChargePointServicer(cp_grpc.ChargePointServiceServicer):
    def __init__(self, delay_s: float = 0.0):
        self._points = seed()
        self._lock = threading.Lock()
        self._delay_s = delay_s

    # ------------------------------------------------------------------ Hilfen
    def _get_or_abort(self, cp_id: str, context) -> cp_pb2.ChargePoint:
        if not cp_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "charge_point_id darf nicht leer sein")
        point = self._points.get(cp_id)
        if point is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Ladesäule '{cp_id}' existiert nicht")
        return point

    def _maybe_delay(self):
        if self._delay_s:
            time.sleep(self._delay_s)

    # ------------------------------------------------------------------ RPCs
    def GetChargePoint(self, request, context):
        with self._lock:
            point = self._get_or_abort(request.charge_point_id, context)
            return _copy(point)

    def ListChargePoints(self, request, context):
        with self._lock:
            points = [p for p in self._points.values()
                      if request.status_filter in (S.CHARGE_POINT_STATUS_UNSPECIFIED, p.status)]
            return cp_pb2.ListChargePointsResponse(charge_points=points)

    def ReserveChargePoint(self, request, context):
        cid = correlation_id(context)
        self._maybe_delay()
        if not request.session_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "session_id darf nicht leer sein")
        with self._lock:
            point = self._get_or_abort(request.charge_point_id, context)
            if point.status == S.CHARGE_POINT_STATUS_OUT_OF_ORDER:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              f"Ladesäule '{point.charge_point_id}' ist gesperrt (OUT_OF_ORDER)")
            if point.status == S.CHARGE_POINT_STATUS_OCCUPIED:
                if point.current_session_id == request.session_id:  # idempotente Wiederholung
                    return _copy(point)
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              f"Ladesäule '{point.charge_point_id}' ist bereits belegt "
                              f"(Session {point.current_session_id})")
            point.status = S.CHARGE_POINT_STATUS_OCCUPIED
            point.current_session_id = request.session_id
            log.info("[%s]    %s reserviert für %s", cid, point.charge_point_id, request.session_id)
            return _copy(point)

    def ReleaseChargePoint(self, request, context):
        cid = correlation_id(context)
        with self._lock:
            point = self._get_or_abort(request.charge_point_id, context)
            if point.status != S.CHARGE_POINT_STATUS_OCCUPIED:
                return _copy(point)  # idempotent: bereits frei
            if point.current_session_id != request.session_id:
                context.abort(grpc.StatusCode.PERMISSION_DENIED,
                              f"Ladesäule ist von Session {point.current_session_id} belegt, "
                              f"nicht von {request.session_id}")
            point.status = S.CHARGE_POINT_STATUS_AVAILABLE
            point.current_session_id = ""
            log.info("[%s]    %s freigegeben", cid, point.charge_point_id)
            return _copy(point)


def _copy(point: cp_pb2.ChargePoint) -> cp_pb2.ChargePoint:
    """Kopie zurückgeben, damit gRPC nicht außerhalb des Locks den internen Zustand serialisiert."""
    copy = cp_pb2.ChargePoint()
    copy.CopyFrom(point)
    return copy


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=config.CHARGE_POINT_PORT)
    ap.add_argument("--delay", type=float, default=0.0,
                    help="künstliche Verzögerung für ReserveChargePoint (Fehlerinjektion)")
    args = ap.parse_args()

    server = make_server(log)
    cp_grpc.add_ChargePointServiceServicer_to_server(ChargePointServicer(args.delay), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    log.info("ChargePointService läuft auf Port %d (delay=%.1fs)", args.port, args.delay)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
