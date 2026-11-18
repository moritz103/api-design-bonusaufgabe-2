"""Alter Consumer, der mit dem URSPRÜNGLICHEN Vertrag (v1, protos_legacy/) generiert wurde.

    python legacy_client.py [cp-002]

Zeigt, dass der weiterentwickelte Provider (v2) alte Consumer weiterhin bedient:
- v1 sendet noch vehicle_plate (Feld 3) -> v2 kennt das Feld nicht mehr (reserved) und ignoriert es,
- v2 sendet customer_id (Feld 12) -> v1 kennt das Feld nicht und ignoriert es,
- v2 kann den neuen Enum-Wert SUSPENDED (5) liefern -> v1 sieht nur die Zahl 5.
"""
import sys
from pathlib import Path

# v1-Stubs zuerst auf den Pfad: emobility.v1.charging_session_pb2 kommt dann aus legacy_gen/,
# die unveränderten common/tariff-Stubs aus dem Projektverzeichnis (Namespace-Package).
sys.path.insert(0, str(Path(__file__).resolve().parent / "legacy_gen"))

import grpc  # noqa: E402

from common import config  # noqa: E402
from common.grpc_util import new_client_metadata  # noqa: E402
from emobility.v1 import charging_session_pb2 as v1  # noqa: E402
from emobility.v1 import charging_session_pb2_grpc as v1_grpc  # noqa: E402


def main() -> int:
    cp = sys.argv[1] if len(sys.argv) > 1 else "cp-002"
    assert "vehicle_plate" in v1.ChargingSession.DESCRIPTOR.fields_by_name, "nicht die v1-Stubs geladen!"
    stub = v1_grpc.ChargingSessionServiceStub(grpc.insecure_channel(config.CHARGING_SESSION_ADDR))
    md = new_client_metadata("legacy-app-v1")
    try:
        s = stub.CreateChargingSession(
            v1.CreateChargingSessionRequest(charge_point_id=cp, id_tag="RFID-LEGACY", vehicle_plate="WF-AB 123"),
            timeout=config.CLIENT_DEADLINE_S, metadata=md)
    except grpc.RpcError as err:
        print(f"FEHLER {err.code().name}: {err.details()}")
        return 1
    print("v1-Consumer hat Ladevorgang angelegt:")
    print(f"  session_id    = {s.session_id}")
    print(f"  status        = {v1.ChargingSessionStatus.Name(s.status)}")
    print(f"  tariff        = {s.tariff.tariff_id}")
    print(f"  vehicle_plate = '{s.vehicle_plate}'  (v2 speichert das Feld nicht mehr -> Default)")
    print(f"  unbekannte Felder aus v2 (z. B. customer_id) werden ignoriert, Parsing funktioniert.")
    stub.StopChargingSession(v1.StopChargingSessionRequest(session_id=s.session_id),
                             timeout=config.CLIENT_DEADLINE_S, metadata=md)
    print("  -> wieder beendet, Ladesäule frei.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
