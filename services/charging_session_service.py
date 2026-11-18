"""ChargingSessionService – verwaltet Ladevorgänge (Port 50052).

    python -m services.charging_session_service

Doppelrolle: Provider gegenüber dem CLI-Consumer und Consumer gegenüber
ChargePointService (Reserve/Release), TariffService (CalculateTariff) und
MeteringService (GetMeterSummary).
"""
from __future__ import annotations

import argparse
import base64
import threading
import uuid
from decimal import Decimal

import grpc

from common import config
from common.grpc_util import (budget, correlation_id, fmt_money, make_server, money, now_ts,
                              outgoing_metadata, setup_logging, to_decimal)
from emobility.v1 import charge_point_pb2 as cp_pb2
from emobility.v1 import charge_point_pb2_grpc as cp_grpc
from emobility.v1 import charging_session_pb2 as cs_pb2
from emobility.v1 import charging_session_pb2_grpc as cs_grpc
from emobility.v1 import metering_pb2, metering_pb2_grpc
from emobility.v1 import tariff_pb2, tariff_pb2_grpc

log = setup_logging("charging-session")

SERVICE_ID = "charging-session-service"
St = cs_pb2.ChargingSessionStatus


class DownstreamError(Exception):
    """Fehler eines aufgerufenen Services, bereits in die eigene Fehlersemantik übersetzt."""

    def __init__(self, code: grpc.StatusCode, details: str):
        super().__init__(details)
        self.code = code
        self.details = details


def translate(err: grpc.RpcError, service: str) -> DownstreamError:
    """Übersetzt den Status Code eines Downstream-Aufrufs in den Code, den WIR an
    unseren Consumer zurückgeben. Fachliche Fehler werden durchgereicht (der Consumer
    hat sie verursacht), technische Fehler werden als solche gekennzeichnet."""
    code = err.code()
    if code in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.FAILED_PRECONDITION,
                grpc.StatusCode.INVALID_ARGUMENT):
        return DownstreamError(code, err.details())
    if code == grpc.StatusCode.UNAVAILABLE:
        return DownstreamError(grpc.StatusCode.UNAVAILABLE,
                               f"{service} nicht erreichbar – bitte später erneut versuchen")
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return DownstreamError(grpc.StatusCode.DEADLINE_EXCEEDED,
                               f"{service} hat nicht innerhalb des Zeitbudgets geantwortet")
    return DownstreamError(grpc.StatusCode.INTERNAL, f"unerwarteter Fehler von {service}: {code.name}")


class ChargingSessionServicer(cs_grpc.ChargingSessionServiceServicer):
    def __init__(self):
        self._sessions: dict[str, cs_pb2.ChargingSession] = {}  # dict behält Einfügereihenfolge
        self._lock = threading.Lock()
        self._cp = cp_grpc.ChargePointServiceStub(grpc.insecure_channel(config.CHARGE_POINT_ADDR))
        self._tariff = tariff_pb2_grpc.TariffServiceStub(grpc.insecure_channel(config.TARIFF_ADDR))
        self._metering = metering_pb2_grpc.MeteringServiceStub(grpc.insecure_channel(config.METERING_ADDR))

    # ================================================================== Downstream-Aufrufe
    def _call(self, service: str, rpc, request, context, cid: str, own_budget: float):
        timeout = budget(context, own_budget)
        try:
            return rpc(request, timeout=timeout, metadata=outgoing_metadata(cid, SERVICE_ID))
        except grpc.RpcError as err:
            log.warning("[%s]    -> %s fehlgeschlagen: %s (%s)", cid, service, err.code().name, err.details())
            raise translate(err, service) from None

    def _reserve(self, cp_id, sid, context, cid) -> cp_pb2.ChargePoint:
        return self._call("ChargePointService", self._cp.ReserveChargePoint,
                          cp_pb2.ReserveChargePointRequest(charge_point_id=cp_id, session_id=sid),
                          context, cid, config.CHARGE_POINT_DEADLINE_S)

    def _release(self, cp_id, sid, context, cid) -> None:
        """Kompensation: Freigabe darf selbst fehlschlagen, ohne den Originalfehler zu verdecken."""
        try:
            self._call("ChargePointService", self._cp.ReleaseChargePoint,
                       cp_pb2.ReleaseChargePointRequest(charge_point_id=cp_id, session_id=sid),
                       context, cid, config.CHARGE_POINT_DEADLINE_S)
        except DownstreamError as err:
            log.error("[%s]    Kompensation fehlgeschlagen, %s bleibt belegt: %s", cid, cp_id, err.details)

    def _calculate_tariff(self, point: cp_pb2.ChargePoint, customer_id, context, cid) -> tariff_pb2.Tariff:
        # DEADLINE: der Preis muss innerhalb TARIFF_DEADLINE_S feststehen, sonst startet der Vorgang nicht.
        return self._call("TariffService", self._tariff.CalculateTariff,
                          tariff_pb2.CalculateTariffRequest(
                              charge_point_id=point.charge_point_id, connector_type=point.connector_type,
                              max_power_w=point.max_power_w, customer_id=customer_id),
                          context, cid, config.TARIFF_DEADLINE_S)

    # ================================================================== Hilfen
    def _get_or_abort(self, sid: str, context) -> cs_pb2.ChargingSession:
        if not sid:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "session_id darf nicht leer sein")
        with self._lock:
            session = self._sessions.get(sid)
            if session is None:
                context.abort(grpc.StatusCode.NOT_FOUND, f"Ladevorgang '{sid}' existiert nicht")
            copy = cs_pb2.ChargingSession()
            copy.CopyFrom(session)
            return copy

    def _store(self, session: cs_pb2.ChargingSession) -> None:
        with self._lock:
            stored = cs_pb2.ChargingSession()
            stored.CopyFrom(session)
            self._sessions[session.session_id] = stored

    # ================================================================== RPCs
    def CreateChargingSession(self, request, context):
        cid = correlation_id(context)
        if not request.charge_point_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "charge_point_id darf nicht leer sein")
        if not request.id_tag or len(request.id_tag) > 64:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id_tag muss 1–64 Zeichen lang sein")

        session = cs_pb2.ChargingSession(
            session_id=f"cs-{uuid.uuid4().hex[:10]}",  # ID wird vom Provider vergeben
            charge_point_id=request.charge_point_id,
            status=St.CHARGING_SESSION_STATUS_PENDING,
            id_tag=request.id_tag,
            customer_id=request.customer_id,
            created_at=now_ts(),
        )
        sid = session.session_id

        # Schritt 1: Ladesäule reservieren (Consumer-Rolle gegenüber ChargePointService)
        try:
            point = self._reserve(request.charge_point_id, sid, context, cid)
        except DownstreamError as err:
            context.abort(err.code, err.details)  # nichts reserviert -> nichts zu kompensieren

        # Schritt 2: Tarif mit Deadline ermitteln (Consumer-Rolle gegenüber TariffService)
        try:
            tariff = self._calculate_tariff(point, request.customer_id, context, cid)
        except DownstreamError as err:
            self._release(point.charge_point_id, sid, context, cid)  # Kompensation
            session.status = St.CHARGING_SESSION_STATUS_FAILED
            session.failure_reason = err.details
            session.ended_at.CopyFrom(now_ts())
            self._store(session)  # fehlgeschlagener Versuch bleibt nachvollziehbar
            context.set_trailing_metadata(((config.MD_SESSION_ID, sid),))  # Consumer kann ihn abrufen
            log.warning("[%s]    %s FAILED: %s", cid, sid, err.details)
            context.abort(err.code, f"Ladevorgang {sid} nicht gestartet: {err.details}")

        session.tariff.CopyFrom(tariff)
        session.status = St.CHARGING_SESSION_STATUS_ACTIVE
        self._store(session)
        log.info("[%s]    %s ACTIVE an %s, Tarif %s (%s/kWh)", cid, sid, point.charge_point_id,
                 tariff.tariff_id, fmt_money(tariff.price_per_kwh))
        return session

    def GetChargingSession(self, request, context):
        return self._get_or_abort(request.session_id, context)

    def ListChargingSessions(self, request, context):
        if request.page_size < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "page_size darf nicht negativ sein")
        size = min(request.page_size or config.DEFAULT_PAGE_SIZE, config.MAX_PAGE_SIZE)
        offset = 0
        if request.page_token:
            try:
                prefix, value = base64.urlsafe_b64decode(request.page_token.encode()).decode().split(":")
                assert prefix == "offset"
                offset = int(value)
                assert offset >= 0
            except Exception:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "ungültiger page_token")
        with self._lock:
            matching = [s for s in self._sessions.values()
                        if request.status_filter in (St.CHARGING_SESSION_STATUS_UNSPECIFIED, s.status)]
        page = matching[offset:offset + size]
        next_offset = offset + len(page)
        next_token = (base64.urlsafe_b64encode(f"offset:{next_offset}".encode()).decode()
                      if next_offset < len(matching) else "")
        return cs_pb2.ListChargingSessionsResponse(
            charging_sessions=page, next_page_token=next_token, total_size=len(matching))

    def CalculateSessionTariff(self, request, context):
        cid = correlation_id(context)
        session = self._get_or_abort(request.session_id, context)
        if session.status not in (St.CHARGING_SESSION_STATUS_PENDING, St.CHARGING_SESSION_STATUS_ACTIVE,
                                  St.CHARGING_SESSION_STATUS_SUSPENDED):
            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                          f"Tarif kann für Status {St.Name(session.status)} nicht mehr ermittelt werden")
        try:
            point = self._call("ChargePointService", self._cp.GetChargePoint,
                               cp_pb2.GetChargePointRequest(charge_point_id=session.charge_point_id),
                               context, cid, config.CHARGE_POINT_DEADLINE_S)
            tariff = self._calculate_tariff(point, session.customer_id, context, cid)
        except DownstreamError as err:
            context.abort(err.code, err.details)
        with self._lock:
            self._sessions[session.session_id].tariff.CopyFrom(tariff)
        log.info("[%s]    %s neuer Tarif %s", cid, session.session_id, tariff.tariff_id)
        return tariff

    def StopChargingSession(self, request, context):
        cid = correlation_id(context)
        session = self._get_or_abort(request.session_id, context)
        if session.status == St.CHARGING_SESSION_STATUS_COMPLETED:
            return session  # idempotent
        if session.status != St.CHARGING_SESSION_STATUS_ACTIVE and \
                session.status != St.CHARGING_SESSION_STATUS_SUSPENDED:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                          f"Ladevorgang im Status {St.Name(session.status)} kann nicht beendet werden")
        try:
            summary = self._call("MeteringService", self._metering.GetMeterSummary,
                                 metering_pb2.GetMeterSummaryRequest(session_id=session.session_id),
                                 context, cid, config.METERING_DEADLINE_S)
        except DownstreamError as err:
            # Ohne Zählerstand keine Abrechnung -> Session bleibt ACTIVE, Consumer kann wiederholen.
            context.abort(err.code, err.details)

        self._release(session.charge_point_id, session.session_id, context, cid)
        energy_kwh = Decimal(summary.energy_wh) / 1000
        cost = (energy_kwh * to_decimal(session.tariff.price_per_kwh)).quantize(Decimal("0.01"))
        with self._lock:
            stored = self._sessions[session.session_id]
            stored.status = St.CHARGING_SESSION_STATUS_COMPLETED
            stored.energy_delivered_wh = summary.energy_wh
            stored.total_cost.CopyFrom(money(cost, session.tariff.price_per_kwh.currency_code or "EUR"))
            stored.ended_at.CopyFrom(now_ts())
            result = cs_pb2.ChargingSession()
            result.CopyFrom(stored)
        log.info("[%s]    %s COMPLETED: %.3f kWh, %s", cid, session.session_id, energy_kwh,
                 fmt_money(result.total_cost))
        return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=config.CHARGING_SESSION_PORT)
    args = ap.parse_args()

    server = make_server(log)
    cs_grpc.add_ChargingSessionServiceServicer_to_server(ChargingSessionServicer(), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    log.info("ChargingSessionService läuft auf Port %d (Tarif-Deadline %.1fs)", args.port,
             config.TARIFF_DEADLINE_S)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
