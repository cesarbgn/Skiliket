"""Prueba de estrés en Docker: N estaciones concurrentes contra un solo API.

Mezcla tres tipos de nodo en la misma corrida:
- estaciones legítimas (GDL-001..N) que reenvían HORAS de historia lo más rápido posible,
  con FAULT_RATE de lecturas corruptas (422 esperado);
- intrusos con una clave aleatoria no registrada (401 esperado);
- un suplantador que usa la clave de GDL-001 pero dice ser GDL-002 (403 esperado).

Las claves se generan en memoria y solo llegan a Docker por variables de entorno; el
compose generado las referencia como ${KEY_...}. Al terminar cruza tres fuentes:
el log del API (código por solicitud), el resumen de cada estación y el JSONL persistido.

Uso (desde la raíz, con el daemon de Docker activo):
    python scripts/stress_docker.py --stations 10 --hours 24 --fault-rate 0.3
Resultados en data/stress/<run>/ (ignorado por git).
"""

import argparse
import hashlib
import json
import os
import secrets
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
API_IMAGE = "skilliket/ingest-api:local"
STATION_IMAGE = "skilliket/station:local"


def run(args: list[str], env: dict[str, str] | None = None, check: bool = True) -> str:
    proc = subprocess.run(
        args, cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if check and proc.returncode != 0:
        sys.exit(f"falló {' '.join(args)}:\n{proc.stdout}\n{proc.stderr}")
    return proc.stdout


def json_lines(text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def build_compose(nodes: list[dict], hours: int, interval_s: int) -> dict:
    services: dict[str, dict] = {
        "api": {
            "image": API_IMAGE,
            "environment": {"STATION_KEY_HASHES": "${STATION_KEY_HASHES}"},
            "volumes": ["readings:/data"],
        }
    }
    for node in nodes:
        services[node["service"]] = {
            "image": STATION_IMAGE,
            "depends_on": {"api": {"condition": "service_healthy"}},
            "restart": "no",
            "environment": {
                "STATION_ID": node["station_id"],
                "STATION_KEY": "${" + node["key_var"] + "}",
                "INGEST_URL": "http://api:8000/v1/readings",
                "INTERVAL_S": str(interval_s),
                "REPLAY_HOURS": str(hours),
                "REPLAY_DURATION_MIN": "0",
                "MAX_READINGS": str(node["max_readings"]),
                "FAULT_RATE": str(node["fault_rate"]),
            },
        }
    return {"services": services, "volumes": {"readings": {}}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stations", type=int, default=10)
    parser.add_argument("--intruders", type=int, default=2)
    parser.add_argument("--spoofers", type=int, choices=(0, 1), default=1)
    parser.add_argument("--hours", type=int, default=24, help="historia que reenvía cada estación")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--fault-rate", type=float, default=0.3)
    parser.add_argument("--rogue-readings", type=int, default=240, help="lecturas por intruso/suplantador")
    parser.add_argument("--no-build", action="store_true")
    opts = parser.parse_args()
    if opts.spoofers and opts.stations < 2:
        sys.exit("el suplantador necesita al menos 2 estaciones")

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = ROOT / "data" / "stress" / run_id
    out_dir.mkdir(parents=True)
    project = f"skilliket-stress-{run_id.lower()}"
    per_station = opts.hours * 3600 // opts.interval

    # --- claves (solo en memoria) y nodos
    env = dict(os.environ)
    keys: dict[str, str] = {}
    hashes: dict[str, list[str]] = {}
    nodes: list[dict] = []
    for i in range(1, opts.stations + 1):
        sid = f"GDL-{i:03d}"
        key = secrets.token_urlsafe(32)
        keys[sid] = key
        hashes[sid] = [hashlib.sha256(key.encode()).hexdigest()]
        var = f"KEY_{sid.replace('-', '_')}"
        env[var] = key
        nodes.append({"service": f"st-{sid.lower()}", "station_id": sid, "key_var": var, "kind": "legit",
                      "max_readings": per_station, "fault_rate": opts.fault_rate})
    for j in range(1, opts.intruders + 1):
        var = f"KEY_INTRUDER_{j}"
        env[var] = secrets.token_urlsafe(32)  # nunca registrada
        keys[f"intruder-{j}"] = env[var]
        nodes.append({"service": f"intruder-{j}", "station_id": f"XXX-{900 + j}", "key_var": var,
                      "kind": "intruder", "max_readings": opts.rogue_readings, "fault_rate": 0})
    if opts.spoofers:
        nodes.append({"service": "spoofer-1", "station_id": "GDL-002", "key_var": "KEY_GDL_001",
                      "kind": "spoofer", "max_readings": opts.rogue_readings, "fault_rate": 0})
    env["STATION_KEY_HASHES"] = json.dumps(hashes)

    compose_file = out_dir / "compose.json"  # sin claves: solo referencias ${KEY_...}
    compose_file.write_text(json.dumps(build_compose(nodes, opts.hours, opts.interval), indent=2), encoding="utf-8")
    dc = ["docker", "compose", "-p", project, "-f", str(compose_file)]

    if not opts.no_build:
        print("construyendo imágenes...", flush=True)
        run(["docker", "build", "-q", "-t", API_IMAGE, "ingest_api"])
        run(["docker", "build", "-q", "-t", STATION_IMAGE, "station"])

    expected_requests = sum(n["max_readings"] for n in nodes)
    print(f"run {run_id}: {len(nodes)} contenedores de estación, {expected_requests} solicitudes esperadas", flush=True)
    try:
        t0 = time.monotonic()
        run(dc + ["up", "-d"], env=env)  # depends_on espera a que el API esté healthy
        ids = {n["service"]: run(dc + ["ps", "-a", "-q", n["service"]], env=env).strip() for n in nodes}
        exit_codes = dict(zip(ids, run(["docker", "wait", *ids.values()]).split()))
        wall_s = time.monotonic() - t0

        api_log = run(dc + ["logs", "--no-color", "--no-log-prefix", "api"], env=env)
        (out_dir / "api.log").write_text(api_log, encoding="utf-8")
        station_logs = {}
        for n in nodes:
            text = run(dc + ["logs", "--no-color", "--no-log-prefix", n["service"]], env=env)
            station_logs[n["service"]] = text
            (out_dir / f"{n['service']}.log").write_text(text, encoding="utf-8")
        run(dc + ["cp", "api:/data/readings.jsonl", str(out_dir / "readings.jsonl")], env=env, check=False)
        docker_info = json.loads(run(["docker", "info", "--format", "{{json .}}"]))
    finally:
        run(dc + ["down", "-v", "--remove-orphans"], env=env, check=False)

    # --- 1. log del API: un evento por solicitud a /v1/readings
    events = [e for e in json_lines(api_log) if e.get("event") == "reading"]
    by_status = Counter(e["status"] for e in events)
    by_station_status: dict[str, Counter] = defaultdict(Counter)
    codes_422: Counter = Counter()
    for e in events:
        by_station_status[str(e.get("station_id"))][e["status"]] += 1
        if e["status"] == 422:
            codes_422.update(e.get("codes", []))
    ts = sorted(datetime.fromisoformat(e["ts"].replace("Z", "+00:00")) for e in events)
    api_span_s = (ts[-1] - ts[0]).total_seconds() if len(ts) > 1 else 0.0

    # --- 2. resumen y fallas de cada estación
    summaries, faulted = {}, defaultdict(set)
    fault_types: Counter = Counter()
    for n in nodes:
        logs = json_lines(station_logs[n["service"]])
        summary = next((e["summary"] for e in logs if e.get("event") == "summary"), None)
        summaries[n["service"]] = summary
        for e in logs:
            if e.get("event") == "fault_check":
                faulted[n["station_id"]].add(e["sequence"])
                fault_types[e["fault"]] += 1

    def total(field: str) -> int:
        return sum((s or {}).get(field, 0) for s in summaries.values())

    def dropped(kind: str, status: str) -> int:
        return sum((summaries[n["service"]] or {}).get("dropped_by_status", {}).get(status, 0)
                   for n in nodes if n["kind"] == kind)

    # --- 3. JSONL persistido
    lines = (out_dir / "readings.jsonl").read_text(encoding="utf-8").splitlines()
    parsed, bad_lines = [], 0
    for line in lines:
        try:
            parsed.append(json.loads(line))
        except json.JSONDecodeError:
            bad_lines += 1
    pairs = [(r["reading"]["station_id"], r["reading"]["timestamp"]) for r in parsed]
    seqs: dict[str, set] = defaultdict(set)
    for r in parsed:
        seqs[r["reading"]["station_id"]].add(r["reading"]["sequence"])
    gaps_match = all(
        set(range(per_station)) - seqs[n["station_id"]] == faulted[n["station_id"]]
        for n in nodes if n["kind"] == "legit"
    )

    # --- 4. claves en claro en logs y datos
    corpus = api_log + "".join(station_logs.values()) + "\n".join(lines) + compose_file.read_text(encoding="utf-8")
    key_hits = sum(corpus.count(k) for k in keys.values())

    total_requests = sum(by_status.values())
    results = {
        "run_id": run_id,
        "host": {"docker_server": docker_info.get("ServerVersion"), "cpus": docker_info.get("NCPU"),
                 "mem_gib": round(docker_info.get("MemTotal", 0) / 2**30, 1),
                 "os": docker_info.get("OperatingSystem"), "kernel": docker_info.get("KernelVersion")},
        "config": {"stations": opts.stations, "intruders": opts.intruders, "spoofers": opts.spoofers,
                   "hours": opts.hours, "interval_s": opts.interval, "fault_rate": opts.fault_rate,
                   "readings_per_station": per_station, "rogue_readings": opts.rogue_readings,
                   "containers": len(nodes) + 1},
        "exit_codes": exit_codes,
        "timing": {"wall_s_up_to_last_exit": round(wall_s, 1), "api_first_to_last_request_s": round(api_span_s, 1),
                   "throughput_req_s": round(total_requests / api_span_s, 1) if api_span_s else None},
        "api": {"requests": total_requests, "by_status": dict(sorted(by_status.items())),
                "rates_pct": {s: round(100 * c / total_requests, 2) for s, c in sorted(by_status.items())},
                "codes_422": dict(codes_422.most_common()),
                "by_station": {k: dict(sorted(v.items())) for k, v in sorted(by_station_status.items())}},
        "stations": {"generated": total("generated"), "created": total("created"), "duplicates": total("duplicates"),
                     "retries": total("retries"), "faults_injected": total("faults_injected"),
                     "faults_confirmed": total("faults_confirmed"), "faults_unexpected": total("faults_unexpected"),
                     "dropped_overflow": total("dropped_overflow"), "unsent_at_exit": total("unsent_at_exit"),
                     "intruder_401": dropped("intruder", "401"), "spoofer_403": dropped("spoofer", "403"),
                     "fault_types": dict(fault_types.most_common())},
        "storage": {"lines": len(lines), "unparseable_lines": bad_lines, "unique_pairs": len(set(pairs)),
                    "per_station": {k: len(v) for k, v in sorted(seqs.items())},
                    "sequence_gaps_equal_faults": gaps_match},
        "checks": {},
    }
    s, a, st = results["stations"], results["api"]["by_status"], results["storage"]
    results["checks"] = {
        "all_containers_exit_0": all(c == "0" for c in exit_codes.values()),
        "requests_equal_expected": total_requests == expected_requests,
        "api_201_eq_station_created_eq_jsonl": a.get(201, 0) == s["created"] == st["lines"],
        "api_422_eq_faults_injected_eq_confirmed": a.get(422, 0) == s["faults_injected"] == s["faults_confirmed"],
        "api_401_eq_intruder_drops": a.get(401, 0) == s["intruder_401"],
        "api_403_eq_spoofer_drops": a.get(403, 0) == s["spoofer_403"],
        "no_409_no_5xx": a.get(409, 0) == 0 and not any(code >= 500 for code in a),
        "no_collisions_jsonl": st["unparseable_lines"] == 0 and st["unique_pairs"] == st["lines"],
        "no_writes_for_rogue_nodes": not any(k.startswith("XXX-") for k in st["per_station"]),
        "sequence_gaps_equal_faults": gaps_match,
        "no_plaintext_keys": key_hits == 0,
    }
    (out_dir / "results.json").write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\nresultados: {out_dir / 'results.json'}")
    sys.exit(0 if all(results["checks"].values()) else 1)


if __name__ == "__main__":
    main()
