"""Shared by the lane scripts: paths, keys, the SRW API, the database tool's control calls, SRW's records.

State of 24 November 2026 (pilot). Written against SRW on the local k3d cluster of the owner's machine;
the places marked K3D are specific to it (SRW's audit database read through kubectl, the self-signed
certificate of https://localhost).
"""

from __future__ import annotations

import csv
import datetime
import gzip
import json
import ssl
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

LANE = Path(__file__).resolve().parent
HARNESS_REPO = LANE.parents[2]                       # Graph-ETL-Pipeline-schema-treatments
MAIN_REPO = HARNESS_REPO.parent / "Graph-ETL-Pipeline"
PILOT = HARNESS_REPO / "deploy/pilot"
PILOT_RUN = PILOT / "run"
CELLS_JSON = PILOT / "cells.json"
UNITS_TXT = HARNESS_REPO / "experiments/harness/pilot/units.txt"
UNITS_DIR = MAIN_REPO / "experiments/sample/out/units"
UNITS_CSV = MAIN_REPO / "experiments/sample/frozen/development-pilot.csv"
PROMPTS_DIR = MAIN_REPO / "experiments/prompts"
SCHEMAS = HARNESS_REPO / "schemas/treatments"
DBTOOL = "http://127.0.0.1:28080"                    # the driver runs on the bundle's machine
OUT = PILOT_RUN / "lane"                             # everything the driver writes (not tracked)

# The five development units of the proof of concept, in the order of 17 November 2026.
DEV5 = ["u4bd4e5399703", "u0412edda3142", "u6ccb15bfa361", "u80afca1f8808", "u1e92954b6acd"]


def read_env(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and not key.strip().startswith("#"):
            out[key.strip()] = value.strip()
    return out


SRW_ENV = read_env(PILOT_RUN / "srw.env")
KEYS = read_env(PILOT_RUN / "keys.env")


def env_name(cell_id: str) -> str:
    return cell_id.upper().replace("-", "_")


def token_of_cell(cell_id: str) -> str:
    return SRW_ENV[f"SRW_TOKEN_{env_name(cell_id)}"]


def key_of_cell(cell_id: str) -> str:
    return KEYS[f"DBTOOL_KEY_{env_name(cell_id)}"]


def cells_config() -> dict:
    return json.loads(CELLS_JSON.read_text(encoding="utf-8"))


def cell_of(cell_id: str) -> dict:
    for cell in cells_config()["cells"]:
        if cell["id"] == cell_id:
            return cell
    raise SystemExit(f"no cell {cell_id} in {CELLS_JSON}")


def setup_of(cell_id: str) -> dict:
    return json.loads((PILOT_RUN / "setup" / f"{cell_id}.json").read_text(encoding="utf-8"))


def now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


UNSAFE = ssl.create_default_context()                # K3D: self-signed certificate
UNSAFE.check_hostname, UNSAFE.verify_mode = False, ssl.CERT_NONE


def http(method: str, url: str, body: dict | None = None, headers: dict | None = None, timeout: int = 60):
    """(status, JSON answer). A refused connection, a timeout or a 200 that is not JSON (SRW's address
    answers with the Cockpit's page while its API restarts) count as 503: the caller polls on."""
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=UNSAFE if url.startswith("https") else None) as response:
            raw = response.read().decode("utf-8")
            try:
                return response.status, json.loads(raw) if raw.strip() else {}
            except ValueError:
                return 503, {"raw": raw[:300], "not_json": True}
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", "replace")
        try:
            return error.code, json.loads(raw)
        except ValueError:
            return error.code, {"raw": raw[:600]}
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
        return 503, {"error": f"{type(error).__name__}: {error}"}


class SrwClient:
    def __init__(self, token: str, api: str | None = None):
        self.token, self.api = token, api or SRW_ENV["SRW_API"]

    def call(self, method: str, path: str, body: dict | None = None, timeout: int = 60):
        return http(method, self.api + path, body, {"Authorization": f"Bearer {self.token}"}, timeout)


def tool_control(cell_id: str, method: str, body: dict | None = None) -> dict:
    status, answer = http(method, f"{DBTOOL}/cells/{cell_id}/unit", body, {"X-Harness-Key": KEYS["DBTOOL_CONTROL_KEY"]})
    if status != 200:
        raise RuntimeError(f"database tool: {method} unit of {cell_id} answered {status}: {answer}")
    return answer


def tool_health() -> dict:
    return http("GET", f"{DBTOOL}/health")[1]


def pilot_units() -> list[dict]:
    """The twelve pilot units in the order of a cell: unit_id, source_id, stratum."""
    rows = []
    for line in UNITS_TXT.read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#"):
            unit_id, source_id, stratum = line.split("\t")
            rows.append({"unit_id": unit_id, "source_id": source_id, "stratum": stratum})
    return rows


def development_unit(unit_id: str) -> dict:
    with UNITS_CSV.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["unit_id"] == unit_id:
                if row["bucket"] != "development":
                    raise SystemExit(f"{unit_id} is not a development unit")
                return {"unit_id": unit_id, "source_id": row["source_id"], "stratum": row["stratum"]}
    raise SystemExit(f"{unit_id} is not in {UNITS_CSV.name}")


# ---------------------------------------------------------------------------------------------------
# SRW's records (K3D: read through kubectl from the audit database)

def psql(pod: str, database: str, sql: str) -> list[str]:
    out = subprocess.run(
        ["kubectl", "--context=k3d-srw", "-n", "srw", "exec", "-i", pod, "-c", pod.rsplit("-", 1)[0].removeprefix("srw-"),
         "--", "psql", "-U", "srw", "-d", database, "-At", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip()[-600:])
    return [line for line in out.stdout.splitlines() if line.strip()]


def tokens_so_far(job_id: str) -> int:
    """Input plus output tokens of a job's model calls recorded so far (SRW's audit store)."""
    rows = psql("srw-auditdb-0", "srw_audit",
                "select coalesce(sum(coalesce((metrics->'token_usage'->>'prompt_tokens')::bigint, 0)"
                " + coalesce((metrics->'token_usage'->>'completion_tokens')::bigint, 0)), 0)"
                f" from llm_requests where job_id = '{job_id}';")
    return int(rows[0]) if rows else 0


def tokens_of(usage: dict) -> dict:
    details_in = usage.get("prompt_tokens_details") or {}
    details_out = usage.get("completion_tokens_details") or {}
    cached = usage.get("prompt_cache_hit_tokens", details_in.get("cached_tokens"))
    prompt = usage.get("prompt_tokens")
    return {
        "input": prompt,
        "input_cached": cached,
        "input_not_cached": usage.get("prompt_cache_miss_tokens",
                                      (prompt - cached) if prompt is not None and cached is not None else None),
        "output": usage.get("completion_tokens"),
        "reasoning": details_out.get("reasoning_tokens"),
    }


def export(job_id: str, since: str, target: Path) -> dict:
    """SRW's records of one job: one line per model call (compact), the complete rows (gzip), one line
    per tool step. Returns counts and sums for the summary."""
    target.mkdir(parents=True, exist_ok=True)
    where = f"job_id = '{job_id}' and timestamp >= '{since}'::timestamptz - interval '1 hour'"
    rows = [json.loads(line) for line in psql(
        "srw-auditdb-0", "srw_audit", f"select row_to_json(r) from llm_requests r where {where} order by timestamp, id;")]
    with gzip.open(target / f"{job_id}.llm_requests.full.jsonl.gz", "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    calls = []
    for row in rows:
        request, response = row.get("request") or {}, row.get("response") or {}
        metrics, metadata = row.get("metrics") or {}, row.get("metadata") or {}
        usage = metrics.get("token_usage") or {}
        streamed = metrics.get("usage_metadata") or {}
        if not usage and streamed:
            usage = {"prompt_tokens": streamed.get("input_tokens"), "completion_tokens": streamed.get("output_tokens"),
                     "prompt_tokens_details": {"cached_tokens": (streamed.get("input_token_details") or {}).get("cache_read")},
                     "completion_tokens_details": {"reasoning_tokens": (streamed.get("output_token_details") or {}).get("reasoning")}}
        meta = response.get("response_metadata") or {}
        wire = request.get("wire") or []
        extra = response.get("additional_kwargs") or {}
        reasoning = extra.get("reasoning_content") or extra.get("reasoning") or ""
        calls.append({
            "job_id": job_id, "call_id": row["id"], "step_index": row.get("iteration"), "call_type": row.get("call_type"),
            "status": metadata.get("status", "ok"), "rejected_by": metadata.get("rejected") or metadata.get("guard"),
            "error": metadata.get("error") or row.get("error"),
            "attempt": metrics.get("attempt"),
            "request_started_at": metrics.get("started_at"), "row_written_at": row.get("timestamp"),
            "latency_ms": row.get("latency_ms"),
            "requests_on_the_wire": [
                {key: entry.get(key) for key in ("sent_at", "status", "elapsed_ms", "error", "retry_after", "params", "shape", "bytes", "sha256")}
                for entry in wire],
            "returned_model": meta.get("model_name") or meta.get("model"),
            "system_fingerprint": meta.get("system_fingerprint"),
            "finish_reason": meta.get("finish_reason"),
            "tokens": tokens_of(usage),
            "reasoning_chars": len(reasoning) if isinstance(reasoning, str) else None,
            "tool_calls": [call.get("name") for call in (response.get("tool_calls") or [])],
            "reply_text": (response.get("content") if isinstance(response.get("content"), str) else None) if not response.get("tool_calls") else None,
        })
    with (target / f"{job_id}.model_calls.jsonl").open("w", encoding="utf-8") as handle:
        for call in calls:
            handle.write(json.dumps(call, ensure_ascii=False) + "\n")
    steps = psql("srw-auditdb-0", "srw_audit", f"select row_to_json(a) from agent_audit a where {where} order by timestamp, id;")
    (target / f"{job_id}.agent_audit.jsonl").write_text("\n".join(steps) + ("\n" if steps else ""), encoding="utf-8")
    main = [call for call in calls if call["call_type"] == "main"]
    total = lambda kind: sum((call["tokens"][kind] or 0) for call in calls)
    names = [name for call in calls for name in call["tool_calls"]]
    step_types = {}
    for line in steps:
        kind = json.loads(line).get("step_type")
        step_types[kind] = step_types.get(kind, 0) + 1
    return {
        "model_calls": len(calls), "main_calls": len(main), "calls_not_ok": sum(call["status"] != "ok" for call in calls),
        "tool_calls": len(names),
        "tool_calls_by_name": {name: names.count(name) for name in sorted(set(names))},
        "tokens": {kind: total(kind) for kind in ("input", "input_cached", "input_not_cached", "output", "reasoning")},
        "largest_output": max(((call["tokens"]["output"] or 0) for call in calls), default=0),
        "model_seconds": round(sum((call["latency_ms"] or 0) for call in calls) / 1000, 1),
        "returned_models": sorted({call["returned_model"] for call in calls if call["returned_model"]}),
        "finish_reasons": sorted({str(call["finish_reason"]) for call in calls}),
        "calls_without_reasoning": sum(1 for call in calls if not call["reasoning_chars"]),
        "last_reply": next((call["reply_text"] for call in reversed(main) if call["reply_text"]), None),
        "audit_rows": len(steps),
        "audit_step_types": step_types,
        **compaction_of(rows, step_types),
    }


def compaction_of(rows: list[dict], step_types: dict) -> dict:
    """Was the job's context compacted? SRW's own sign: the number of messages it sends falls from one request
    to the next, or a model call that is not the job's own (a summary) appears, or an audit step names it."""
    counts = [((row.get("request") or {}).get("message_count")) for row in rows if row.get("call_type") == "main"]
    counts = [count for count in counts if isinstance(count, int)]
    drops = sum(1 for a, b in zip(counts, counts[1:]) if b < a)
    other_calls = sorted({str(row.get("call_type")) for row in rows if row.get("call_type") != "main"})
    audit = sorted(kind for kind in step_types if kind and "compact" in kind)
    contexts = [((row.get("metrics") or {}).get("token_usage") or {}).get("prompt_tokens") or 0 for row in rows]
    return {"compacted": bool(drops or other_calls or audit), "message_count_drops": drops,
            "calls_not_main": other_calls, "compaction_audit_steps": audit, "largest_context": max(contexts, default=0)}


# ---------------------------------------------------------------------------------------------------
# The station's stack id (read through the dev cluster's model router, which reaches the station)

def stack_id() -> dict:
    script = ("import os, json, urllib.request\n"
              "r = urllib.request.Request('http://uni-workstation:8000/stack', headers={'Authorization': 'Bearer ' + os.environ.get('DGX_DEEPSEEK_API_KEY', '')})\n"
              "print(urllib.request.urlopen(r, timeout=15).read().decode())\n")
    try:
        out = subprocess.run(["kubectl", "--context=main", "-n", "model-orchestrator", "exec", "deploy/model-orchestrator",
                              "-c", "router", "--", "python3", "-c", script], capture_output=True, text=True, timeout=60)
        if out.returncode != 0:
            return {"error": out.stderr.strip()[-300:], "read_at": now()}
        data = json.loads(out.stdout)
        data["read_at"] = now()
        return data
    except Exception as error:                      # never stop a pass for this; the report says it is missing
        return {"error": f"{type(error).__name__}: {error}", "read_at": now()}
