#!/usr/bin/env python3
"""Step 2 of the pilot: the schema induction, once per database type, as one SRW job each.

    python3 induce.py --arm pg --arm rdf [--max-output-tokens 131072]

The fixed part of the induction prompt is the job's instructions, the task part its description; the
model's single reply is the schema document. The job is offered as little as SRW allows: no connector,
no file, shell or other tool, no folder; it makes exactly one model call (limits.max_llm_calls_per_job
= 1), so SRW ends it after that call. Same model settings as the construction jobs.

Reads deploy/pilot/run/induction/<arm>/prompt/induction.<arm>.{system,user}.txt (written by
`prompts.py induction`). Writes deploy/pilot/run/induction/<arm>/<attempt>/: reply.txt (unchanged),
reasoning.txt, record.json (tokens by kind, settings sent, returned model, seconds, whether the reply
is one JSON document with nothing around it), check.txt (what schemas/treatments/induced.py prints),
and SRW's records of the job.
"""

from __future__ import annotations

import argparse
import datetime
import gzip
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import HARNESS_REPO, PILOT_RUN, SRW_ENV, SrwClient, export, now, stack_id  # noqa: E402

MODEL = "DeepSeek-V4.1-Flash"
TERMINAL = {"completed", "failed", "cancelled"}
PARKED = {"pending_review", "waiting_for_reply", "reviewing", "waiting", "paused"}
OFF = {name: [] for name in ("skills", "research", "browser_direct", "citation", "communication", "knowledge",
                             "memory", "delegation", "git", "graph", "sql", "mongodb", "repo", "email",
                             "evaluation", "orchestrator", "agent_catalog", "workflows", "webdav", "canvas", "image_generation",
                             "workspace", "shell")}


def body(arm: str, texts: dict, args) -> dict:
    return {
        "datasource_ids": [],                         # explicit: no connector at all
        "execution_lane": "stateless",
        "priority": 5,
        "context": {"experiment": {"run_id": args.run_id, "step": "induction", "arm": arm}},
        "description": texts["user"],
        "instructions": texts["system"],
        "expert": {"inline": {"runtime": {"adapter": "srw/v1", "config": {
            "config_name": "worker_base",
            "config": {"display_name": "Schema designer", "tools": {**OFF, "core": args.core_tools}, "instruction_files": []},
            "prompts": {},
        }}}},
        "config_override": {
            "llm": {"model": MODEL, "reasoning_level": "high", "temperature": 0.2, "top_p": 1.0,
                    "max_output_tokens": args.max_output_tokens},
            "auxiliary": {"enabled": False}, "autonomy": "full",
            "memory": {"enabled": False, "required": False, "manager": {"enabled": False}, "ingestion": {"enabled": False}},
            "verification": {"enabled": False}, "curator": {"enabled": False}, "scholar": {"enabled": False},
            "delegation": {"enabled": False},
            "limits": {"max_llm_calls_per_job": 1},
        },
        "phases": False,
    }


def one_json_document(text: str) -> tuple[bool, str]:
    stripped = text.strip()
    try:
        json.loads(stripped)
    except ValueError as error:
        return False, f"not JSON: {error}"
    if stripped != text and text.strip() != text.rstrip("\n").strip():
        return True, "JSON, with white space around it"
    return True, "one JSON document, nothing around it"


def run(arm: str, args, log) -> dict:
    base = PILOT_RUN / "induction" / arm
    texts = {part: (base / "prompt" / f"induction.{arm}.{part}.txt").read_text(encoding="utf-8") for part in ("system", "user")}
    attempt = 1 + sum(1 for path in base.glob("attempt-*") if path.is_dir())
    out = base / f"attempt-{attempt}"
    out.mkdir(parents=True)
    client = SrwClient(SRW_ENV["SRW_TOKEN"])
    request = body(arm, texts, args)
    (out / "job_body.settings.json").write_text(json.dumps({k: v for k, v in request.items() if k not in ("description", "instructions")},
                                                           indent=1), encoding="utf-8")
    started_at, started = now(), time.monotonic()
    code, created = client.call("POST", "/jobs", request)
    if code not in (200, 201):
        record = {"arm": arm, "status": "not_created", "http": code, "answer": created}
        (out / "record.json").write_text(json.dumps(record, indent=1), encoding="utf-8")
        log(f"{arm}: not created {code} {json.dumps(created)[:500]}")
        return record
    job_id = created["id"]
    log(f"{arm}: job {job_id} created")
    last, job = None, {}
    while True:
        time.sleep(10)
        code, answer = client.call("GET", f"/jobs/{job_id}")
        if code == 200:
            job = answer
            if answer.get("status") != last:
                last = answer.get("status")
                log(f"{arm}: {int(time.monotonic() - started)} s {last}")
            if last in TERMINAL:
                break
            if last in PARKED:
                client.call("PUT", f"/jobs/{job_id}/cancel")
                break
        if time.monotonic() - started > args.deadline:
            client.call("PUT", f"/jobs/{job_id}/cancel")
            log(f"{arm}: deadline, cancelled")
            break
    wall = round(time.monotonic() - started, 1)
    time.sleep(8)
    summary = export(job_id, started_at, out / "srw")
    rows = []
    with gzip.open(out / "srw" / f"{job_id}.llm_requests.full.jsonl.gz", "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]
    main = [row for row in rows if row.get("call_type") == "main"]
    first = main[0] if main else {}
    response = first.get("response") or {}
    reply = response.get("content") if isinstance(response.get("content"), str) else json.dumps(response.get("content"))
    extra = response.get("additional_kwargs") or {}
    reasoning = extra.get("reasoning_content") or extra.get("reasoning") or ""
    (out / "reply.txt").write_text(reply or "", encoding="utf-8")
    (out / "reasoning.txt").write_text(reasoning if isinstance(reasoning, str) else json.dumps(reasoning), encoding="utf-8")
    calls = [json.loads(line) for line in (out / "srw" / f"{job_id}.model_calls.jsonl").read_text().splitlines() if line.strip()]
    is_json, form = one_json_document(reply or "")
    check = ""
    if reply:
        result = subprocess.run([sys.executable, str(HARNESS_REPO / "schemas/treatments/induced.py"), str(out / "reply.txt"), "--arm", arm],
                                capture_output=True, text=True, cwd=HARNESS_REPO)
        check = (result.stdout + result.stderr).strip()
        (out / "check.txt").write_text(check + "\n", encoding="utf-8")
    record = {
        "arm": arm, "job_id": job_id, "status": job.get("status"), "error_message": job.get("error_message"),
        "seconds_total": wall, "seconds_in_model": summary["model_seconds"],
        "model_calls": summary["model_calls"], "tool_calls_in_reply": (calls[0]["tool_calls"] if calls else None),
        "tokens": calls[0]["tokens"] if calls else None, "finish_reason": calls[0]["finish_reason"] if calls else None,
        "returned_model": calls[0]["returned_model"] if calls else None,
        "settings_sent": request["config_override"]["llm"],
        "params_on_the_wire": (calls[0]["requests_on_the_wire"] if calls else None),
        "tools_offered": [t.get("function", {}).get("name") or t.get("name") for t in
                          ((first.get("request") or {}).get("tools") or [])],
        "reply_bytes": len((reply or "").encode("utf-8")), "reasoning_chars": len(reasoning) if isinstance(reasoning, str) else None,
        "one_json_document": is_json, "form": form, "check": check,
        "prompt_sha256": {part: __import__("hashlib").sha256(texts[part].encode()).hexdigest() for part in texts},
    }
    (out / "record.json").write_text(json.dumps(record, indent=1, ensure_ascii=False), encoding="utf-8")
    log(f"{arm}: {record['status']}, {wall} s, finish {record['finish_reason']}, tokens {record['tokens']}, {form}; check: {check[:300]}")
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--arm", action="append", choices=("pg", "rdf"), required=True)
    parser.add_argument("--max-output-tokens", type=int, default=131072)
    parser.add_argument("--core-tools", nargs="*", default=[], help="SRW core tools offered (default: none)")
    parser.add_argument("--deadline", type=int, default=3600)
    args = parser.parse_args()
    args.run_id = f"pilot-induction-{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%SZ}"
    lock = threading.Lock()

    def log(text: str) -> None:
        with lock:
            print(f"{datetime.datetime.now():%H:%M:%S} {text}", flush=True)

    log(f"run {args.run_id}; stack at start {stack_id().get('stack_id')}")
    threads = [threading.Thread(target=run, args=(arm, args, log)) for arm in args.arm]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    log(f"stack at end {stack_id().get('stack_id')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
