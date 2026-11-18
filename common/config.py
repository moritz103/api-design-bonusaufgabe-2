"""Zentrale Konfiguration: Adressen, Ports, Deadlines. Alles per Umgebungsvariable überschreibbar."""
import os


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


HOST = _env("EMOB_HOST", "localhost")

CHARGE_POINT_PORT = int(_env("CHARGE_POINT_PORT", "50051"))
CHARGING_SESSION_PORT = int(_env("CHARGING_SESSION_PORT", "50052"))
TARIFF_PORT = int(_env("TARIFF_PORT", "50053"))
METERING_PORT = int(_env("METERING_PORT", "50054"))

CHARGE_POINT_ADDR = f"{HOST}:{CHARGE_POINT_PORT}"
CHARGING_SESSION_ADDR = f"{HOST}:{CHARGING_SESSION_PORT}"
TARIFF_ADDR = f"{HOST}:{TARIFF_PORT}"
METERING_ADDR = f"{HOST}:{METERING_PORT}"

# Deadlines (Sekunden) für service-übergreifende Aufrufe des ChargingSessionService.
# Der Tarif muss innerhalb dieses Zeitbudgets feststehen, sonst startet der Ladevorgang nicht.
TARIFF_DEADLINE_S = float(_env("TARIFF_DEADLINE_S", "1.5"))
CHARGE_POINT_DEADLINE_S = float(_env("CHARGE_POINT_DEADLINE_S", "2.0"))
METERING_DEADLINE_S = float(_env("METERING_DEADLINE_S", "2.0"))

# Deadline, die der CLI-Consumer für seine eigenen Aufrufe setzt.
CLIENT_DEADLINE_S = float(_env("CLIENT_DEADLINE_S", "5.0"))

# Metadata-Schlüssel (gRPC verlangt Kleinbuchstaben)
MD_CORRELATION_ID = "x-correlation-id"
MD_CLIENT_ID = "x-client-id"
MD_SESSION_ID = "x-session-id"  # Trailing Metadata bei fehlgeschlagenem Create

DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 100
