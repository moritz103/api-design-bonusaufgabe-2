"""Gemeinsame gRPC-Hilfsfunktionen: Logging, Metadata, Server-Interceptor, Zeit/Geld-Konvertierung."""
from __future__ import annotations

import datetime as dt
import logging
import uuid
from concurrent import futures
from decimal import ROUND_HALF_UP, Decimal

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from common import config
from emobility.v1 import common_pb2


# --------------------------------------------------------------------------- Logging
def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(name)-16s %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(name)


# --------------------------------------------------------------------------- Metadata
def metadata_dict(context: grpc.ServicerContext) -> dict[str, str]:
    return {k: v for k, v in context.invocation_metadata()}


def correlation_id(context: grpc.ServicerContext) -> str:
    """Correlation-ID des eingehenden Aufrufs (oder neue, falls der Consumer keine mitschickt)."""
    return metadata_dict(context).get(config.MD_CORRELATION_ID) or f"gen-{uuid.uuid4().hex[:8]}"


def outgoing_metadata(cid: str, client_id: str) -> list[tuple[str, str]]:
    """Metadata für einen Downstream-Aufruf: Correlation-ID wird weitergereicht,
    Client-ID ist der aufrufende Service selbst."""
    return [(config.MD_CORRELATION_ID, cid), (config.MD_CLIENT_ID, client_id)]


def new_client_metadata(client_id: str) -> list[tuple[str, str]]:
    return outgoing_metadata(f"req-{uuid.uuid4().hex[:8]}", client_id)


class MetadataLoggingInterceptor(grpc.ServerInterceptor):
    """Liest auf Provider-Seite die Metadata jedes eingehenden RPCs aus und protokolliert sie.
    So lassen sich Aufrufketten über Service-Grenzen per Correlation-ID nachverfolgen."""

    def __init__(self, log: logging.Logger):
        self._log = log

    def intercept_service(self, continuation, handler_call_details):
        md = dict(handler_call_details.invocation_metadata or ())
        method = handler_call_details.method.rsplit("/", 1)[-1]
        self._log.info(
            "[%s] <- %s  (client=%s)",
            md.get(config.MD_CORRELATION_ID, "-"), method, md.get(config.MD_CLIENT_ID, "unbekannt"),
        )
        return continuation(handler_call_details)


def budget(context: grpc.ServicerContext, own_budget_s: float) -> float:
    """Deadline für einen Downstream-Aufruf: eigenes Budget, aber nie länger als die
    Restzeit, die der ursprüngliche Consumer noch gewährt (Deadline-Propagation)."""
    remaining = context.time_remaining()
    if remaining is None:
        return own_budget_s
    return max(0.05, min(own_budget_s, remaining - 0.05))


def make_server(log: logging.Logger, max_workers: int = 16) -> grpc.Server:
    return grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        interceptors=[MetadataLoggingInterceptor(log)],
    )


# --------------------------------------------------------------------------- Zeit
def now_ts() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def fmt_ts(ts: Timestamp) -> str:
    if not ts.seconds and not ts.nanos:
        return "-"
    return ts.ToDatetime(tzinfo=dt.timezone.utc).astimezone().strftime("%H:%M:%S")


# --------------------------------------------------------------------------- Geld
NANO = Decimal("1000000000")


def money(amount: Decimal | str, currency: str = "EUR") -> common_pb2.Money:
    amount = Decimal(amount).quantize(Decimal("0.000000001"))
    units = int(amount)  # Richtung 0 abgeschnitten -> nanos hat gleiches Vorzeichen
    nanos = int((amount - units) * NANO)
    return common_pb2.Money(currency_code=currency, units=units, nanos=nanos)


def to_decimal(m: common_pb2.Money) -> Decimal:
    return Decimal(m.units) + Decimal(m.nanos) / NANO


def fmt_money(m: common_pb2.Money, places: str = "0.01") -> str:
    if not m.currency_code:
        return "-"
    return f"{to_decimal(m).quantize(Decimal(places), ROUND_HALF_UP)} {m.currency_code}"
