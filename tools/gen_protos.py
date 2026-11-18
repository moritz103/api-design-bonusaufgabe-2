"""Generiert die Python-Stubs aus den .proto-Verträgen.

    python -m tools.gen_protos

- protos/          -> ./emobility/v1/*_pb2*.py          (aktueller Vertrag, v2)
- protos_legacy/   -> ./legacy_gen/emobility/v1/...      (nur charging_session v1)
"""
from pathlib import Path

import grpc_tools
from grpc_tools import protoc

ROOT = Path(__file__).resolve().parent.parent
WKT = str(Path(grpc_tools.__file__).parent / "_proto")


def run(args: list[str]) -> None:
    rc = protoc.main(["protoc", f"-I{WKT}", *args])
    if rc != 0:
        raise SystemExit(f"protoc fehlgeschlagen: {args}")


def main() -> None:
    protos = sorted((ROOT / "protos").rglob("*.proto"))
    run([f"-I{ROOT / 'protos'}", f"--python_out={ROOT}", f"--grpc_python_out={ROOT}",
         *[str(p) for p in protos]])

    legacy_out = ROOT / "legacy_gen"
    legacy_out.mkdir(exist_ok=True)
    # Reihenfolge der -I wichtig: protos_legacy zuerst, damit charging_session.proto v1 gefunden wird;
    # common/tariff/charge_point kommen aus protos/ (unverändert) und werden NICHT neu generiert.
    run([f"-I{ROOT / 'protos_legacy'}", f"-I{ROOT / 'protos'}",
         f"--python_out={legacy_out}", f"--grpc_python_out={legacy_out}",
         str(ROOT / "protos_legacy" / "emobility" / "v1" / "charging_session.proto")])
    print("Stubs generiert.")


if __name__ == "__main__":
    main()
