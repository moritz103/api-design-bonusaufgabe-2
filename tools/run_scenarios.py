"""Führt die Resilienz-Szenarien aus scenarios.md reproduzierbar aus.

    python -m tools.run_scenarios            # alle Szenarien
    python -m tools.run_scenarios 4 5        # nur ausgewählte

Startet/stoppt die Services selbst als Subprozesse (Logs unter ./logs/).
Laufen bereits Services auf den Ports, vorher beenden.
"""
from __future__ import annotations

import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
LOGS = ROOT / "logs"
SERVICES = ["charge_point", "tariff", "metering", "charging_session"]
procs: dict[str, subprocess.Popen] = {}


# ------------------------------------------------------------------ Prozessverwaltung
def start(name: str, *extra: str) -> None:
    LOGS.mkdir(exist_ok=True)
    log = open(LOGS / f"{name}.log", "a")
    procs[name] = subprocess.Popen([PY, "-u", "-m", f"services.{name}_service", *extra],
                                   cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    time.sleep(0.8)


def stop(name: str) -> None:
    p = procs.pop(name, None)
    if p:
        p.terminate()
        p.wait(5)


def stop_all() -> None:
    for name in list(procs):
        stop(name)


def client(*args: str, show: bool = True) -> tuple[int, str]:
    r = subprocess.run([PY, "client.py", *args], cwd=ROOT, capture_output=True, text=True)
    out = (r.stdout + r.stderr).rstrip()
    if show:
        print(f"$ python client.py {' '.join(args)}\n{out}\n(exit {r.returncode})\n")
    return r.returncode, out


def session_id(out: str) -> str:
    m = re.search(r"cs-[0-9a-f]{10}", out)
    assert m, out
    return m.group(0)


def header(nr: int, title: str) -> None:
    print("\n" + "=" * 78 + f"\nSzenario {nr}: {title}\n" + "=" * 78)


def check(cond: bool, text: str) -> None:
    print(("  [OK]   " if cond else "  [FAIL] ") + text)
    if not cond:
        global failed
        failed = True


failed = False


# ------------------------------------------------------------------ Szenarien
def s1():
    header(1, "nicht existierende session_id / Ladesäule")
    rc, out = client("get", "cs-0000000000")
    check("NOT_FOUND" in out, "Consumer erhält NOT_FOUND für unbekannte Session")
    rc, out = client("create", "cp-999")
    check("NOT_FOUND" in out, "ChargePointService-NOT_FOUND wird vom SessionService durchgereicht")


def s2():
    header(2, "fachlich ungültige Daten")
    rc, out = client("create", "cp-002", "--id-tag", "")
    check("INVALID_ARGUMENT" in out, "leeres id_tag -> INVALID_ARGUMENT (Validierung im SessionService)")
    rc, out = client("list", "--page-size", "-5")
    check("INVALID_ARGUMENT" in out, "negative page_size -> INVALID_ARGUMENT")
    rc, out = client("create", "cp-002")
    sid = session_id(out)
    r = subprocess.run([PY, "simulator.py", sid, "--cp", "cp-002", "--count", "6", "--interval", "0.1",
                        "--invalid"], cwd=ROOT, capture_output=True, text=True)
    print(f"$ python simulator.py {sid} --invalid\n{(r.stdout + r.stderr).rstrip()}\n")
    check("INVALID_ARGUMENT" in r.stderr, "sinkender Zählerstand im Client-Stream -> INVALID_ARGUMENT")
    client("stop", sid, show=False)


def s3():
    header(3, "Ladesäule nicht reservierbar (belegt / gesperrt)")
    rc, out = client("create", "cp-004")
    sid = session_id(out)
    rc, out = client("create", "cp-004")
    check("FAILED_PRECONDITION" in out and "bereits belegt" in out, "belegte Säule -> FAILED_PRECONDITION")
    rc, out = client("create", "cp-003")
    check("FAILED_PRECONDITION" in out and "gesperrt" in out, "gesperrte Säule -> FAILED_PRECONDITION")
    client("stop", sid, show=False)


def s4():
    header(4, "Ausfall des ChargePointService während CreateChargingSession")
    stop("charge_point")
    print("  (ChargePointService beendet)\n")
    rc, out = client("create", "cp-001")
    check("UNAVAILABLE" in out, "Consumer erhält UNAVAILABLE (nicht INTERNAL) – Wiederholung sinnvoll")
    rc, out = client("list", "--status", "ACTIVE", show=False)
    check("insgesamt 0" in out, "es wurde kein halbfertiger Ladevorgang angelegt")
    start("charge_point")
    print("  (ChargePointService neu gestartet)\n")
    rc, out = client("create", "cp-001")
    check(rc == 0 and "ACTIVE" in out, "System arbeitet nach Neustart normal weiter")
    client("stop", session_id(out), show=False)


def s5():
    header(5, "TariffService überschreitet die Deadline (Tarif-Zeitbudget 1,5 s)")
    stop("tariff")
    start("tariff", "--delay", "3")
    print("  (TariffService mit --delay 3 gestartet)\n")
    t0 = time.monotonic()
    rc, out = client("create", "cp-001")
    elapsed = time.monotonic() - t0
    check("DEADLINE_EXCEEDED" in out, f"Consumer erhält DEADLINE_EXCEEDED nach {elapsed:.1f}s (nicht nach 3 s)")
    sid = session_id(out)
    rc, out = client("get", sid)
    check("FAILED" in out, "Ladevorgang ist als FAILED mit Grund protokolliert")
    rc, out = client("points", show=False)
    check(re.search(r"cp-001\s+AVAILABLE", out) is not None, "Kompensation: cp-001 wurde wieder freigegeben")
    stop("tariff")
    start("tariff")
    print("  (TariffService ohne Verzögerung neu gestartet)\n")
    rc, out = client("create", "cp-001")
    check(rc == 0 and "ACTIVE" in out, "System arbeitet danach normal weiter")
    client("stop", session_id(out), show=False)


def s6():
    header(6, "Streaming-Verbindung wird während der Übertragung unterbrochen")
    rc, out = client("create", "cp-002", show=False)
    sid = session_id(out)
    watcher = subprocess.Popen([PY, "-u", "client.py", "watch", sid, "--history"], cwd=ROOT, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True)
    sim = subprocess.Popen([PY, "simulator.py", sid, "--cp", "cp-002", "--count", "30", "--interval", "0.2"],
                           cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    time.sleep(2)
    stop("metering")
    print("  (MeteringService während laufender Streams beendet)")
    sim_err = sim.communicate(timeout=10)[1]
    time.sleep(1.5)
    start("metering")
    print("  (MeteringService neu gestartet, Ladesäule sendet erneut)\n")
    subprocess.run([PY, "simulator.py", sid, "--cp", "cp-002", "--count", "3", "--interval", "0.2",
                    "--start-wh", "4000"],
                   cwd=ROOT, capture_output=True)
    out = watcher.communicate(timeout=30)[0]
    print(f"$ python client.py watch {sid} --history\n{out.rstrip()}\n")
    print(f"$ python simulator.py {sid} …\n{sim_err.strip()}\n")
    check("UNAVAILABLE" in sim_err, "Client-Stream der Ladesäule bricht mit UNAVAILABLE ab")
    check("Wiederverbindung" in out, "Server-Stream-Consumer erkennt UNAVAILABLE und verbindet sich neu")
    check("Stream beendet" in out, "nach Neustart empfängt der Consumer wieder Messwerte")
    client("stop", sid, show=False)


SCENARIOS = {1: s1, 2: s2, 3: s3, 4: s4, 5: s5, 6: s6}


def main() -> int:
    selected = [int(a) for a in sys.argv[1:]] or list(SCENARIOS)
    try:
        for name in SERVICES:
            start(name)
        for nr in selected:
            SCENARIOS[nr]()
    finally:
        stop_all()
    print("\nErgebnis:", "FEHLGESCHLAGEN" if failed else "alle Prüfungen bestanden", f"(Logs: {LOGS})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
