#!/usr/bin/env python3
"""The pilot bundle: one graph store per cell, the database tool, one MCP wrapper per cell.

    python3 deploy/pilot/pilot.py start [--arm pg|rdf|all] [--no-build]   # build if the code changed, start, wait until ready
    python3 deploy/pilot/pilot.py addresses [--host NAME]   # what an agent framework needs per cell
    python3 deploy/pilot/pilot.py reset [NAME] [--force]    # empty every cell's graph, put the records away under NAME
    python3 deploy/pilot/pilot.py stop [--delete-data]      # stop; with --delete-data the graphs are deleted too
    python3 deploy/pilot/pilot.py smoke [--arm ...] [--down]   # start, test every cell, reset; --down stops and deletes afterwards

A cell is a property-graph cell (arm pg: a Neo4j, the tool `cypher`) or an RDF cell (arm rdf: a
Fuseki with a change log the tool reads, the tool `sparql`). `--arm` starts the cells of one arm
only; the choice is kept in run/arm, so that the later commands act on the same cells.

Everything is generated from cells.json into run/ (not tracked): the compose file, the tool's
configuration, the keys, the staged copy the image is built from. Standard library only; needs
podman-compose or docker compose. PILOT_COMPOSE and PILOT_ENGINE name other commands.
"""

from __future__ import annotations

import argparse
import base64
import datetime
import hashlib
import json
import os
import re
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
RUN = HERE / "run"
IMAGE = "localhost/pilot-dbtool"
TOOL = "pilot-dbtool"
NETWORK = "pilot-net"
TOOL_PORT_INSIDE, MCP_PORT_INSIDE = 18080, 18081
ARMS = {"pg": {"store": "neo4j", "tool_name": "cypher", "wrapper": "mcp"},
        "rdf": {"store": "fuseki", "tool_name": "sparql", "wrapper": "mcp-sparql"}}
FUSEKI_CONFIG = REPO / "deploy" / "harness" / "fuseki" / "cell.ttl"      # the logged dataset `cell`, the same for every cell
FUSEKI_DATASET = "cell"


# ------------------------------------------------------------------ configuration

def chosen_arm(arm: str | None = None) -> str:
    """The arm the bundle runs with: as asked, else as the last start chose, else both."""
    if arm:
        return arm
    kept = RUN / "arm"
    return kept.read_text(encoding="utf-8").strip() if kept.is_file() else "all"


def load_config(arm: str | None = None) -> dict:
    config = json.loads((HERE / "cells.json").read_text(encoding="utf-8"))
    arm = chosen_arm(arm)
    if arm not in ("pg", "rdf", "all"):
        raise SystemExit(f"arm is pg, rdf or all, not {arm!r}")
    cells, seen_ports = [], {config["tool_port"]}
    for cell in config["cells"]:
        if not cell.get("enabled", True):
            continue
        if not re.fullmatch(r"pilot-[a-z0-9]+(-[a-z0-9]+)*", cell["id"]):
            raise SystemExit(f"cell id {cell['id']!r}: lower-case letters, digits and hyphens, starting with pilot-")
        if cell["arm"] not in ARMS:
            raise SystemExit(f"{cell['id']}: arm is pg or rdf, not {cell['arm']!r}")
        if cell["mcp_port"] in seen_ports:
            raise SystemExit(f"{cell['id']}: port {cell['mcp_port']} is used twice")
        seen_ports.add(cell["mcp_port"])
        if arm not in ("all", cell["arm"]):
            continue
        store = f"{cell['id']}-{ARMS[cell['arm']]['store']}"
        cells.append({**cell, "store": store, "mcp": f"{cell['id']}-mcp", "tool_name": ARMS[cell["arm"]]["tool_name"],
                      "key_env": "DBTOOL_KEY_" + cell["id"].upper().replace("-", "_"),
                      # an RDF cell has two volumes: the data, and the change log, which the tool reads as well
                      "volumes": [f"{store}-data"] + ([f"{store}-log"] if cell["arm"] == "rdf" else [])})
    if not cells:
        raise SystemExit("cells.json enables no cell" + ("" if arm == "all" else f" of the arm {arm}"))
    config["cells"], config["arm"] = cells, arm
    config["bind"] = os.environ.get("PILOT_BIND", config.get("bind", "0.0.0.0"))
    return config


def of_arm(config: dict, arm: str) -> list[dict]:
    return [cell for cell in config["cells"] if cell["arm"] == arm]


def local(config: dict) -> str:
    """The address this machine uses to reach what the bundle publishes."""
    return "127.0.0.1" if config["bind"] in ("0.0.0.0", "") else config["bind"]


def read_env(path: Path) -> dict:
    if not path.is_file():
        return {}
    return dict(line.split("=", 1) for line in path.read_text(encoding="utf-8").splitlines() if "=" in line)


def write_private(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)


def ensure_keys(config: dict) -> dict:
    """Keys are made once and kept; a new cell gets its key on the next start."""
    keys = read_env(RUN / "keys.env")
    for name in ["PILOT_NEO4J_PASSWORD", "PILOT_FUSEKI_PASSWORD", "DBTOOL_CONTROL_KEY"] + [cell["key_env"] for cell in config["cells"]]:
        keys.setdefault(name, secrets.token_hex(16))
    write_private(RUN / "keys.env", "".join(f"{name}={value}\n" for name, value in keys.items()))
    write_private(RUN / "store.env", f"NEO4J_AUTH=neo4j/{keys['PILOT_NEO4J_PASSWORD']}\n")
    return keys


def stage_fuseki(keys: dict) -> None:
    """What every Fuseki of the bundle mounts: the dataset's configuration and the login of this bundle."""
    folder = RUN / "fuseki"
    folder.mkdir(exist_ok=True)
    shutil.copy2(FUSEKI_CONFIG, folder / "cell.ttl")
    # Readable by the store's own user inside its container, so not private on this machine. The stores are
    # not published: only the tool, on the bundle's network, can use this login.
    (folder / "shiro.ini").write_text(
        "# Generated by deploy/pilot/pilot.py: the login of this bundle's Fuseki stores. Only the database tool uses it.\n"
        "[main]\nplainMatcher = org.apache.shiro.authc.credential.SimpleCredentialsMatcher\n\n"
        f"[users]\nadmin = {keys['PILOT_FUSEKI_PASSWORD']}\n\n"
        "[urls]\n/$/ping = anon\n/**     = authcBasic,user[admin]\n", encoding="utf-8")
    (folder / "shiro.ini").chmod(0o644)


def ensure_fuseki_image(config: dict) -> None:
    """The Fuseki image is built from deploy/local/fuseki when this machine does not have it."""
    image = config["fuseki_image"]
    if image_exists(image):
        return
    context = (HERE / config.get("fuseki_context", "../local/fuseki")).resolve()
    if not (context / "Dockerfile").is_file():
        raise SystemExit(f"the image {image} is not on this machine and {context} holds no Dockerfile to build it from")
    run([commands()[1], "build", "-t", image, "--build-arg", f"JENA_VERSION={config.get('jena_version', '6.2.0')}", str(context)])


# ------------------------------------------------------------------ commands of the container engine

def commands() -> tuple[list[str], str]:
    if os.environ.get("PILOT_COMPOSE"):
        compose = shlex.split(os.environ["PILOT_COMPOSE"])
    elif shutil.which("podman-compose"):
        compose = ["podman-compose"]
    elif shutil.which("docker"):
        compose = ["docker", "compose"]
    else:
        raise SystemExit("neither podman-compose nor docker found; set PILOT_COMPOSE and PILOT_ENGINE")
    engine = os.environ.get("PILOT_ENGINE") or ("podman" if "podman" in compose[0] else "docker")
    return compose, engine


def run(command: list[str], check: bool = True, quiet: bool = False, **more) -> subprocess.CompletedProcess:
    if not quiet:
        print("  $ " + " ".join(shlex.quote(part) for part in command), flush=True)
    result = subprocess.run(command, text=True, capture_output=quiet, **more)
    if check and result.returncode != 0:
        raise SystemExit((result.stderr or "").strip() or f"failed: {' '.join(command)}")
    return result


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return run(commands()[0] + ["-f", str(RUN / "compose.yml"), *args], check=check)


def engine(*args: str, check: bool = True, quiet: bool = True) -> subprocess.CompletedProcess:
    return run([commands()[1], *args], check=check, quiet=quiet)


# ------------------------------------------------------------------ the image

def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def copy_tree(source: Path, target: Path, skip: tuple[str, ...] = ("__pycache__", ".pytest_cache", "tests")) -> None:
    shutil.copytree(source, target, ignore=shutil.ignore_patterns(*skip, "*.pyc"))


def git(checkout: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(checkout), *args], text=True, capture_output=True)
    return result.stdout.strip() if result.returncode == 0 else ""


def stage_build(config: dict) -> dict:
    """Copy what the image holds into run/build and describe it. The tag is a hash of the copy."""
    validator_checkout = Path(os.environ.get("VALIDATOR_CHECKOUT") or (REPO / config["validator_checkout"])).resolve()
    validator = validator_checkout / "experiments" / "thesis-b" / "validator"
    if not (validator / "checker.py").is_file():
        raise SystemExit(f"no validator in {validator}; set VALIDATOR_CHECKOUT to a checkout of the branch thesis-b/f3-spec")
    for cell in config["cells"]:
        declaration = REPO / "schemas" / "treatments" / cell["realisation"] / "declaration.json"
        if not declaration.is_file():
            raise SystemExit(f"{cell['id']}: no declaration for {cell['realisation']}")
        if json.loads(declaration.read_text(encoding="utf-8")).get("status") == "placeholder":
            raise SystemExit(f"{cell['id']}: the declaration of {cell['realisation']} is a placeholder; "
                             f"switch the cell on in cells.json once the schema exists")
    build = RUN / "build"
    shutil.rmtree(build, ignore_errors=True)
    build.mkdir(parents=True)
    copy_tree(REPO / "experiments" / "harness" / "dbtool", build / "dbtool")
    copy_tree(REPO / "experiments" / "harness" / "mcp", build / "mcp")
    copy_tree(validator, build / "validator-root" / "validator")
    for declaration in sorted((REPO / "schemas" / "treatments").glob("*/*/declaration.json")):
        target = build / "kit-root" / declaration.relative_to(REPO)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(declaration, target)
    for name in ("Dockerfile", "entrypoint.py", "requirements.txt"):
        shutil.copy2(HERE / "image" / name, build / name)

    files = {str(path.relative_to(build)): sha256(path) for path in sorted(build.rglob("*")) if path.is_file()}
    content = hashlib.sha256(json.dumps([files, config["python_image"]], sort_keys=True).encode()).hexdigest()
    info = {
        "image": f"{IMAGE}:{content[:12]}",
        "content_sha256": content,
        "staged": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "python_image": config["python_image"],
        "tool": {"commit": git(REPO, "rev-parse", "HEAD"), "branch": git(REPO, "rev-parse", "--abbrev-ref", "HEAD"),
                 "uncommitted": git(REPO, "status", "--porcelain", "--", "experiments/harness/dbtool",
                                    "experiments/harness/mcp", "schemas/treatments", "deploy/pilot").splitlines()},
        "validator": {"commit": git(validator_checkout, "rev-parse", "HEAD"),
                      "uncommitted": git(validator_checkout, "status", "--porcelain", "--",
                                         "experiments/thesis-b/validator").splitlines()},
        "declarations": {name.split("schemas/treatments/")[1].rsplit("/", 1)[0]: digest
                         for name, digest in files.items() if name.endswith("declaration.json")},
        "files": files,
    }
    if of_arm(config, "rdf"):
        info["fuseki"] = {"image": config["fuseki_image"], "jena_version": config.get("jena_version", "6.2.0"),
                          "cell_ttl_sha256": sha256(FUSEKI_CONFIG)}
    (build / "BUILD_INFO.json").write_text(json.dumps(info, indent=1) + "\n", encoding="utf-8")
    (RUN / "BUILD_INFO.json").write_text(json.dumps(info, indent=1) + "\n", encoding="utf-8")
    return info


def image_exists(image: str) -> bool:
    return engine("image", "inspect", image, check=False).returncode == 0


def build_image(config: dict, info: dict) -> None:
    if image_exists(info["image"]):
        print(f"image {info['image']} exists; nothing to build")
        return
    run([commands()[1], "build", "-t", info["image"], "--build-arg", f"PYTHON_IMAGE={config['python_image']}",
         str(RUN / "build")])


# ------------------------------------------------------------------ generated files

def tool_config(config: dict) -> dict:
    return {
        "listen": f"0.0.0.0:{TOOL_PORT_INSIDE}",
        "hook_address": f"http://{TOOL}:{TOOL_PORT_INSIDE}",     # how the stores reach the tool
        "control_key_env": "DBTOOL_CONTROL_KEY",
        "log_dir": "/records",
        "checker": config.get("checker", "validator.checker:checker"),
        "warn_deleted": config.get("warn_deleted", 25),
        "remove_hooks_on_stop": True,
        "cells": {cell["id"]: cell_config(cell) for cell in config["cells"]},
    }


def cell_config(cell: dict) -> dict:
    common = {"realisation": cell["realisation"], "tool_key_env": cell["key_env"], "mode": cell.get("mode", "reject")}
    if cell["arm"] == "pg":
        return {"arm": "pg", "uri": f"bolt://{cell['store']}:7687", "user": "neo4j",
                "password_env": "PILOT_NEO4J_PASSWORD", "database": "neo4j", **common}
    # The schema's namespace is given here, from the declaration: inside the image the tool does not find the
    # declarations where it looks for them in a checkout. Raw triples name none.
    declared = json.loads((REPO / "schemas" / "treatments" / cell["realisation"] / "declaration.json").read_text(encoding="utf-8"))
    return {"arm": "rdf", "endpoint": f"http://{cell['store']}:3030/{FUSEKI_DATASET}", "user": "admin",
            "password_env": "PILOT_FUSEKI_PASSWORD", "change_log": f"/logs/{cell['id']}/{FUSEKI_DATASET}.rdfp",
            **({"namespace": declared["namespace"]} if declared.get("namespace") else {}), **common}


SETTINGS = "@settings@"


def settings_hash(config: dict, image: str) -> str:
    """Names what a container was started with: the compose file and the tool's configuration."""
    text = compose_text(config, image) + json.dumps(tool_config(config), sort_keys=True)
    if of_arm(config, "rdf"):
        text += sha256(FUSEKI_CONFIG)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def write_compose(config: dict, image: str) -> None:
    text = compose_text(config, image).replace(SETTINGS, settings_hash(config, image))
    (RUN / "compose.yml").write_text(text, encoding="utf-8")


def compose_text(config: dict, image: str) -> str:
    """The compose file, with a placeholder where every container carries the hash of its settings."""
    bind = config["bind"]
    label = ["    labels:", f"      - pilot.settings={SETTINGS}"]
    lines = [
        "# Generated by deploy/pilot/pilot.py from deploy/pilot/cells.json. Do not edit: change cells.json and start again.",
        "name: pilot",
        "services:",
        f"  {TOOL}:",
        f"    image: {image}",
        f"    container_name: {TOOL}",
        *label,
        '    command: ["tool", "/config/tool-config.json"]',
        "    env_file:",
        "      - ./keys.env",
        "    ports:",
        f'      - "{bind}:{config["tool_port"]}:{TOOL_PORT_INSIDE}"',
        "    volumes:",
        "      - ./tool-config.json:/config/tool-config.json:ro,z",
        "      - ./records:/records:z",
        # the change log of every RDF cell, as its store writes it
        *[f"      - {cell['store']}-log:/logs/{cell['id']}:ro" for cell in of_arm(config, "rdf")],
        f"    networks: [{NETWORK}]",
        "    depends_on:",
        *[f"      - {cell['store']}" for cell in config["cells"]],
        "    stop_grace_period: 30s",
    ]
    for cell in of_arm(config, "rdf"):
        lines += [
            f"  {cell['store']}:",
            f"    image: {config['fuseki_image']}",
            f"    container_name: {cell['store']}",
            *label,
            # One dataset, `cell`, which writes every change into a log; the tool reads that log and takes a
            # rejected statement out again. The store is not published: only the tool talks to it.
            '    command: ["--shiro", "/fuseki/shiro.ini", "--ping", "--compact", "--config", "/fuseki/cell.ttl"]',
            "    environment:",
            f"      - JAVA_OPTIONS=-Xmx{config.get('fuseki_heap', '1g')} -Xms256m",
            "    volumes:",
            "      - ./fuseki/cell.ttl:/fuseki/cell.ttl:ro,z",
            "      - ./fuseki/shiro.ini:/fuseki/shiro.ini:ro,z",
            f"      - {cell['store']}-data:/fuseki/databases",
            f"      - {cell['store']}-log:/fuseki/logs",
            f"    networks: [{NETWORK}]",
            "    healthcheck:",
            "      test: [\"CMD-SHELL\", \"wget -q -O /dev/null http://localhost:3030/$$/ping || exit 1\"]",
            "      interval: 10s",
            "      timeout: 5s",
            "      retries: 12",
            "      start_period: 10s",
            "    stop_grace_period: 30s",
        ]
    for cell in of_arm(config, "pg"):
        lines += [
            f"  {cell['store']}:",
            f"    image: {config['neo4j_image']}",
            f"    container_name: {cell['store']}",
            *label,
            "    env_file:",
            "      - ./store.env",
            "    environment:",
            "      - 'NEO4J_PLUGINS=[\"apoc\"]'",
            "      # The commit hook: the store hands every transaction's changes to the database tool before it commits.",
            "      - NEO4J_apoc_trigger_enabled=true",
            "      - NEO4J_apoc_trigger_refresh=1000",
            f"      - NEO4J_server_memory_heap_max__size={config.get('neo4j_heap', '1G')}",
            f"      - NEO4J_server_memory_pagecache_size={config.get('neo4j_pagecache', '512M')}",
            "    volumes:",
            f"      - {cell['store']}-data:/data",
            f"    networks: [{NETWORK}]",
            "    healthcheck:",
            "      test: [\"CMD-SHELL\", \"cypher-shell -u neo4j -p $${NEO4J_AUTH#neo4j/} 'RETURN 1' || exit 1\"]",
            "      interval: 10s",
            "      timeout: 10s",
            "      retries: 12",
            "      start_period: 30s",
            "    stop_grace_period: 120s",
        ]
    for cell in config["cells"]:
        lines += [
            f"  {cell['mcp']}:",
            f"    image: {image}",
            f"    container_name: {cell['mcp']}",
            *label,
            f'    command: ["{ARMS[cell["arm"]]["wrapper"]}", "{MCP_PORT_INSIDE}"]',
            "    environment:",
            f"      - DBTOOL_URL=http://{TOOL}:{TOOL_PORT_INSIDE}/cells/{cell['id']}/tool",
            "    ports:",
            f'      - "{bind}:{cell["mcp_port"]}:{MCP_PORT_INSIDE}"',
            f"    networks: [{NETWORK}]",
            "    depends_on:",
            f"      - {TOOL}",
        ]
    lines += ["networks:", f"  {NETWORK}:", f"    name: {NETWORK}", "volumes:"]
    for cell in config["cells"]:
        for volume in cell["volumes"]:
            lines += [f"  {volume}:", f"    name: {volume}"]
    return "\n".join(lines) + "\n"


def addresses(config: dict, host: str) -> dict:
    base = f"http://{host}:{config['tool_port']}"
    return {
        "tool": base, "health": f"{base}/health", "control_key_env": "DBTOOL_CONTROL_KEY", "keys_file": str(RUN / "keys.env"),
        "cells": {cell["id"]: {
            "realisation": cell["realisation"],
            "arm": cell["arm"],
            "tool_name": cell["tool_name"],
            "mcp_url": f"http://{host}:{cell['mcp_port']}/mcp",
            "run_query_url": f"{base}/cells/{cell['id']}/tool",
            "unit_url": f"{base}/cells/{cell['id']}/unit",
            "tool_key_env": cell["key_env"],
            "schema_description": f"schemas/treatments/{cell['realisation']}/description.construction.txt",
        } for cell in config["cells"]},
    }


def print_addresses(config: dict, host: str) -> None:
    found = addresses(config, host)
    (RUN / "addresses.json").write_text(json.dumps(found, indent=2) + "\n", encoding="utf-8")
    print(f"\ndatabase tool   {found['tool']}   (health: GET /health; open or close a unit: POST or DELETE "
          f".../cells/<cell>/unit with X-Harness-Key = DBTOOL_CONTROL_KEY)")
    print(f"keys            {RUN / 'keys.env'}")
    for cell_id, cell in found["cells"].items():
        print(f"\n{cell_id}   {cell['realisation']}")
        print(f"  MCP connector   {cell['mcp_url']}   header X-Tool-Key = {cell['tool_key_env']}   (tool `{cell['tool_name']}`)")
        print(f"  RUN_QUERY_URL   {cell['run_query_url']}")
        print(f"  RUN_QUERY_KEY   {cell['tool_key_env']}")
        print(f"  schema text     {cell['schema_description']}")
    print(f"\n(also written to {RUN / 'addresses.json'}; the host name is {host!r}, change it with --host)")


# ------------------------------------------------------------------ talking to the tool

def call(url: str, method: str = "GET", body: dict | None = None, headers: dict | None = None, timeout: float = 60):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as error:
        text = error.read().decode("utf-8")
        try:
            return error.code, json.loads(text)
        except ValueError:
            return error.code, {"status": "error", "message": text[:300]}
    except (urllib.error.URLError, OSError, ValueError) as error:
        return 0, {"status": "unreachable", "message": str(error)}


def wait_ready(config: dict, timeout: float = 420) -> None:
    url, wanted = f"http://{local(config)}:{config['tool_port']}/health", sorted(cell["id"] for cell in config["cells"])
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        status, answer = call(url, timeout=5)
        if status == 200 and answer.get("status") == "ok":
            if answer.get("cells") != wanted:
                raise SystemExit(f"the tool serves {answer.get('cells')}, expected {wanted}: stop and start again")
            print(f"database tool ready: {', '.join(wanted)}")
            return
        state = engine("inspect", "-f", "{{.State.Status}}", TOOL, check=False).stdout.strip()
        if state in ("exited", "dead"):
            logs = engine("logs", "--tail", "30", TOOL, check=False)
            raise SystemExit(f"the tool's container stopped:\n{logs.stdout}{logs.stderr}")
        if answer.get("status") != last:
            last = answer.get("status")
            print(f"  waiting for the tool ({last})", flush=True)
        time.sleep(3)
    raise SystemExit(f"the database tool was not ready within {timeout:.0f} s; see: {commands()[1]} logs {TOOL}")


def open_units(config: dict) -> list[str]:
    """Cells whose record ends with a unit that was opened and not closed."""
    out = []
    for cell in config["cells"]:
        path, state = RUN / "records" / f"{cell['id']}.jsonl", None
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                kind = json.loads(line).get("record_type") if line.strip() else None
                state = kind if kind in ("unit_open", "unit_close") else state
        if state == "unit_open":
            out.append(cell["id"])
    return out


def existing_containers() -> list[str]:
    """The bundle's containers on this machine, running or not: the tool, and a store and a wrapper per cell."""
    names = engine("ps", "-a", "--format", "{{.Names}}", check=False).stdout.split()
    return [name for name in names if name == TOOL or re.fullmatch(r"pilot-[a-z0-9-]+-(neo4j|fuseki|mcp)", name)]


def halt(remove: bool = False) -> None:
    """Stop the bundle's containers in order: the tool first, so that it takes its hooks out of the stores;
    the stores last and with time to write their data."""
    names = existing_containers()
    wrappers = [name for name in names if name.endswith("-mcp")]
    stores = [name for name in names if name.endswith(("-neo4j", "-fuseki"))]
    if TOOL in names:
        engine("stop", "-t", "30", TOOL, check=False)
    if wrappers:
        engine("stop", "-t", "10", *wrappers, check=False)
    if stores:
        engine("stop", "-t", "120", *stores, check=False)
    if remove:
        for name in wrappers + ([TOOL] if TOOL in names else []) + stores:      # dependents before what they need
            engine("rm", "-f", name, check=False)


def cypher_shell(cell: dict, statement: str, database: str = "neo4j") -> subprocess.CompletedProcess:
    script = f'cypher-shell -u neo4j -p "${{NEO4J_AUTH#neo4j/}}" -d {database} --format plain {shlex.quote(statement)}'
    return engine("exec", cell["store"], "sh", "-c", script, check=False)


def prepare_log_volumes(config: dict) -> None:
    """Make the change-log volume of every RDF cell, with an empty log that belongs to the store's user.

    Two containers mount this volume: the store, which writes the log, and the tool, which reads it. Fuseki
    creates the log only with its first write, and a container engine hands a volume that is still empty to
    the user of whichever container mounts it next: here the tool's, after which the store could not write
    (seen with Podman on 22 November 2026: "cell.rdfp (Permission denied)"). So the log exists before either
    starts."""
    for cell in of_arm(config, "rdf"):
        volume = f"{cell['store']}-log"
        if engine("volume", "inspect", volume, check=False).returncode != 0:
            engine("volume", "create", volume)
        made = engine("run", "--rm", "--user", "0:0", "--entrypoint", "sh", "-v", f"{volume}:/fuseki/logs",
                      config["fuseki_image"], "-c",
                      f"touch /fuseki/logs/{FUSEKI_DATASET}.rdfp && chown -R fuseki:fuseki /fuseki/logs", check=False)
        if made.returncode != 0:
            raise SystemExit(f"{cell['id']}: the change-log volume {volume} could not be prepared: {made.stderr.strip()}")


def fuseki(cell: dict, service: str, statement: str) -> tuple[bool, str]:
    """One request straight to a cell's Fuseki, from inside its container: not through the tool. `service` is
    `sparql` (the answer is CSV) or `update`. Used to count after a reset and, in the smoke test, to write past
    the tool."""
    keys = read_env(RUN / "keys.env")
    login = base64.b64encode(f"admin:{keys['PILOT_FUSEKI_PASSWORD']}".encode()).decode()
    data = urllib.parse.urlencode({"query" if service == "sparql" else "update": statement})
    result = engine("exec", cell["store"], "wget", "-q", "-O", "-", "--header", f"Authorization: Basic {login}",
                    "--header", "Accept: text/csv", "--post-data", data,
                    f"http://localhost:3030/{FUSEKI_DATASET}/{service}", check=False)
    return result.returncode == 0, result.stdout.strip()


def triples(cell: dict, pattern: str = "?s ?p ?o") -> str:
    ok, answer = fuseki(cell, "sparql", f"SELECT (COUNT(*) AS ?n) WHERE {{ {pattern} }}")
    return answer.split()[-1] if ok and answer else "?"


# ------------------------------------------------------------------ start, stop, reset

def start(no_build: bool = False, host: str | None = None, arm: str | None = None) -> dict:
    RUN.mkdir(exist_ok=True)
    (RUN / "records").mkdir(exist_ok=True)
    config = load_config(arm)
    keys = ensure_keys(config)
    earlier = (RUN / "BUILD_INFO.json").read_text(encoding="utf-8") if (RUN / "BUILD_INFO.json").is_file() else None
    if no_build:
        if earlier is None:
            raise SystemExit("--no-build needs run/BUILD_INFO.json of an earlier build")
        info = json.loads(earlier)
        if not image_exists(info["image"]):
            raise SystemExit(f"--no-build: the image {info['image']} is not on this machine (load it first)")
        missing = [cell["id"] for cell in config["cells"] if cell["realisation"] not in info.get("declarations", {})]
        if missing:
            raise SystemExit(f"--no-build: the image holds no declaration for {', '.join(missing)}")
    else:
        info = stage_build(config)

    # Containers of an earlier start may hold another image or other cells. They are made again; the graphs stay.
    wanted, existing = settings_hash(config, info["image"]), existing_containers()
    stale = [name for name in existing if engine("inspect", "-f", '{{index .Config.Labels "pilot.settings"}}', name,
                                                 check=False).stdout.strip() != wanted]
    if stale:
        busy = open_units(config)
        if busy:
            if earlier is not None:
                (RUN / "BUILD_INFO.json").write_text(earlier, encoding="utf-8")
            raise SystemExit(f"the image or the cells changed, and a unit is open in {', '.join(busy)}: "
                             f"let it end, then start again (the running bundle was not touched)")
    if not no_build:
        build_image(config, info)
    if of_arm(config, "rdf"):
        stage_fuseki(keys)
        ensure_fuseki_image(config)
    if stale:
        print("the image or the cells changed: the containers are made again, the graphs stay in their volumes")
        halt(remove=True)
    (RUN / "arm").write_text(config["arm"] + "\n", encoding="utf-8")
    prepare_log_volumes(config)
    (RUN / "tool-config.json").write_text(json.dumps(tool_config(config), indent=2) + "\n", encoding="utf-8")
    write_compose(config, info["image"])
    shutil.copy2(RUN / "BUILD_INFO.json", RUN / "records" / "BUILD_INFO.json")
    compose("up", "-d")
    wait_ready(config)
    print(f"image {info['image']}: tool at {info['tool']['commit'][:7]}"
          f"{' plus uncommitted changes' if info['tool']['uncommitted'] else ''}, "
          f"validator at {info['validator']['commit'][:7]}"
          f"{' plus uncommitted changes' if info['validator']['uncommitted'] else ''}")
    print_addresses(config, host or os.environ.get("PILOT_HOST") or socket.gethostname())
    return config


def stop(delete_data: bool = False) -> None:
    if not (RUN / "compose.yml").is_file():
        raise SystemExit("nothing was started from here (no run/compose.yml)")
    config = load_config()
    busy = open_units(config)
    if busy:
        print(f"note: a unit is still open in {', '.join(busy)}")
    halt()
    compose("down", *(["-v"] if delete_data else []), check=False)
    if delete_data:
        for name in existing_containers():                 # a volume is only free once nothing holds it
            engine("rm", "-f", name, check=False)
        for cell in config["cells"]:
            for volume in cell["volumes"]:
                engine("volume", "rm", "-f", volume, check=False)
            (RUN / "records" / f"{cell['id']}.settled").unlink(missing_ok=True)     # the tool's mark belongs to the log
        engine("network", "rm", NETWORK, check=False)
    print("stopped" + ("; the graphs are deleted" if delete_data else "; the graphs are kept in their volumes"))


def reset(name: str | None = None, force: bool = False) -> None:
    config = load_config()
    busy = open_units(config)
    if busy and not force:
        raise SystemExit(f"a unit is open in {', '.join(busy)}: close it (or let its job end) first; --force resets anyway")
    name = name or datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    pg, rdf = of_arm(config, "pg"), of_arm(config, "rdf")
    engine("stop", "-t", "30", TOOL, check=False)          # the tool removes its hooks; without a hook the stores take a delete
    for cell in pg:
        cypher_shell(cell, f"CALL apoc.trigger.drop('neo4j', 'dbtool-{cell['id']}')", database="system")   # left by a killed tool
    if pg:
        time.sleep(3)                                      # the stores read their hooks once a second
    for cell in pg:
        wiped = cypher_shell(cell, "MATCH (n) DETACH DELETE n")
        left = cypher_shell(cell, "MATCH (n) RETURN count(n) AS n").stdout.strip().splitlines()
        if wiped.returncode != 0 or not left or left[-1] != "0":
            raise SystemExit(f"{cell['id']}: the graph could not be emptied: {wiped.stderr.strip() or left}")
        print(f"{cell['id']}: 0 nodes left")
    # An RDF cell is emptied by deleting its data and its change log together, and the tool's mark with them:
    # the tool rebuilds its list of the graph from the log, so the three belong to each other.
    if rdf:
        # The tool's container holds the log volumes, and the wrappers were started as depending on the tool,
        # so they go first: dependents before what they need. `up` below makes them all again.
        for cell in config["cells"]:
            engine("rm", "-f", cell["mcp"], check=False)
        engine("rm", "-f", TOOL, check=False)
    for cell in rdf:
        engine("stop", "-t", "30", cell["store"], check=False)
        engine("rm", "-f", cell["store"], check=False)
        for volume in cell["volumes"]:
            if engine("volume", "inspect", volume, check=False).returncode == 0:
                gone = engine("volume", "rm", volume, check=False)
                if gone.returncode != 0:
                    raise SystemExit(f"{cell['id']}: the volume {volume} could not be deleted: {gone.stderr.strip()}")
        (RUN / "records" / f"{cell['id']}.settled").unlink(missing_ok=True)
    target = RUN / "records" / name
    moved = [path for path in (RUN / "records").glob("*.jsonl")]
    if moved:
        target.mkdir(parents=True, exist_ok=True)
        for path in moved:
            shutil.move(str(path), str(target / path.name))
        print(f"records of the pass put away in {target}")
    if rdf:
        prepare_log_volumes(config)
        compose("up", "-d")                                # makes the stores, their data volumes and the tool again
    else:
        engine("start", TOOL)
    wait_ready(config)
    for cell in rdf:
        left = triples(cell)
        if left != "0":
            raise SystemExit(f"{cell['id']}: the graph could not be emptied: {left} triples left")
        print(f"{cell['id']}: 0 triples left")


# ------------------------------------------------------------------ smoke test

# One statement that is valid under the cell's schema, one that breaks it, and a read of what the first stored.
SMOKE = {
    "S4-curated/pg": {
        "valid": "MERGE (:Material {uid: 'smoke-material', tag: 'smoke', name: 'smoke'})",
        "invalid": "CREATE (:NoSuchType {uid: 'smoke'})",
        "read": "MATCH (m:Material {uid: 'smoke-material'}) RETURN m.name AS name",
        "also_valid": "CREATE (:DisposalRule {uid: 'smoke-rule'})",          # a rule node: the flat schema rejects it
    },
    "S3-flat/pg": {
        "valid": "MERGE (:Material {uid: 'smoke-material', tag: 'smoke', name: 'smoke'})",
        "invalid": "CREATE (:DisposalRule {uid: 'smoke-rule'})",             # the flat schema has no rule nodes
        "read": "MATCH (m:Material {uid: 'smoke-material'}) RETURN m.name AS name",
    },
    "S1-raw-triples/pg": {
        "valid": "CREATE (:Entity {name: 'smoke'})",
        "invalid": "CREATE (:Entity)",                                       # nothing declared: a fault of the shared rules
        "read": "MATCH (e:Entity {name: 'smoke'}) RETURN e.name AS name",
        "also_valid": "MATCH (e:Entity {name: 'smoke'}) CREATE (:Assertion {predicate: 'is', value: 'a test'})-[:SUBJECT]->(e)",
    },
}
DATA = "PREFIX d: <https://fessi.example/data/> "
RDFS = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "


def declared_rdf(slug: str, invalid: str, also_valid: str | None = None) -> dict:
    """The same cases as in the property graph, in SPARQL, plus a value on a relation in the annotation form."""
    head = f"PREFIX fessi: <https://fessi.example/schema/{slug}#> " + DATA
    case = {
        "valid": head + 'INSERT DATA { d:smoke-material a fessi:Material ; fessi:uid "smoke-material" ; fessi:tag "smoke" ; fessi:name "smoke" }',
        "invalid": head + invalid,
        "read": head + "SELECT ?name WHERE { d:smoke-material fessi:name ?name }",
        "annotated": head + 'INSERT DATA { d:smoke-product a fessi:Product ; fessi:uid "smoke-product" . '
                            'd:smoke-part a fessi:PackagingComponent ; fessi:uid "smoke-part" . '
                            "d:smoke-product fessi:HAS_COMPONENT d:smoke-part {| fessi:qty 2 |} }",
        "annotated_read": head + "SELECT ?value WHERE { << d:smoke-product fessi:HAS_COMPONENT d:smoke-part >> fessi:qty ?value }",
        "annotated_value": 2,
    }
    if also_valid:
        case["also_valid"] = head + also_valid
    return case


SMOKE.update({
    "S4-curated/rdf": declared_rdf("s4", 'INSERT DATA { d:smoke-other a fessi:NoSuchType ; fessi:uid "smoke" }',
                                   'INSERT DATA { d:smoke-rule a fessi:DisposalRule ; fessi:uid "smoke-rule" }'),
    # the flat schema has no rule nodes
    "S3-flat/rdf": declared_rdf("s3", 'INSERT DATA { d:smoke-rule a fessi:DisposalRule ; fessi:uid "smoke-rule" }'),
    "S1-raw-triples/rdf": {
        "valid": DATA + RDFS + 'INSERT DATA { d:smoke rdfs:label "smoke" ; d:is "a test" . d:smoke-two rdfs:label "smoke two" }',
        "invalid": DATA + 'INSERT DATA { d:smoke-nameless d:weight "5 kg" }',     # nothing declared: a fault of the shared rules
        "read": DATA + RDFS + "SELECT ?name WHERE { d:smoke rdfs:label ?name }",
        "annotated": DATA + 'INSERT DATA { d:smoke d:goes_to d:smoke-two {| d:since "2020" |} }',
        "annotated_read": DATA + "SELECT ?value WHERE { << d:smoke d:goes_to d:smoke-two >> d:since ?value }",
        "annotated_value": "2020",
    },
})
ANY = {"pg": {"nothing": "RETURN 1 AS one", "count": "MATCH (n) RETURN count(n) AS n"},
       "rdf": {"nothing": "ASK { }", "count": "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"}}


def mcp_call(url: str, key: str, method: str, params: dict | None = None) -> dict:
    """One JSON-RPC request to a stateless streamable-HTTP MCP server."""
    body = {"jsonrpc": "2.0", "id": 1, "method": method, **({"params": params} if params is not None else {})}
    request = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"), method="POST", headers={
        "Content-Type": "application/json", "Accept": "application/json, text/event-stream", "X-Tool-Key": key})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            text = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return {"error": {"message": f"http {error.code}: {error.read().decode('utf-8')[:300]}"}}
    except (urllib.error.URLError, OSError) as error:
        return {"error": {"message": str(error)}}
    if text.lstrip().startswith("event:") or text.lstrip().startswith("data:"):       # an event stream with one message
        text = "\n".join(line[5:].strip() for line in text.splitlines() if line.startswith("data:"))
    return json.loads(text)


def smoke(down: bool = False, arm: str | None = None) -> int:
    config = start(arm=arm)
    keys, host = read_env(RUN / "keys.env"), local(config)
    base, failures = f"http://{host}:{config['tool_port']}", []
    control = {"X-Harness-Key": keys["DBTOOL_CONTROL_KEY"]}
    print()

    def show(cell_id: str, what: str, ok: bool, detail: str) -> None:
        print(f"{cell_id:13} {'ok  ' if ok else 'FAIL'} {what:22} {detail}")
        if not ok:
            failures.append(f"{cell_id}: {what}")

    for cell in config["cells"]:
        cid, tool = cell["id"], {"X-Tool-Key": keys[cell["key_env"]]}
        case = SMOKE.get(cell["realisation"], {})
        status, answer = call(f"{base}/health")
        show(cid, "health", answer.get("status") == "ok" and cid in answer.get("cells", []), json.dumps(answer))
        status, answer = call(f"{base}/cells/{cid}/tool", "POST", {"statement": ANY[cell["arm"]]["nothing"]}, tool)
        show(cid, "no unit open", answer.get("status") == "error", answer.get("message", "")[:70])
        status, answer = call(f"{base}/cells/{cid}/unit", "POST", {"unit_id": f"smoke-{cid}", "run_id": "smoke",
                                                                    "source_id": "none", "stratum": "none"}, control)
        show(cid, "unit opens", status == 200 and answer.get("status") == "ok", json.dumps(answer))
        for name in ("valid", "also_valid"):
            if name in case:
                status, answer = call(f"{base}/cells/{cid}/tool", "POST", {"statement": case[name], "passage": "smoke"}, tool)
                show(cid, f"{name.replace('_', ' ')} write", answer.get("status") == "stored",
                     f"{answer.get('status')} {answer.get('id', '')} {json.dumps(answer.get('created', answer.get('message', '')))[:90]}")
        if "invalid" in case:
            status, answer = call(f"{base}/cells/{cid}/tool", "POST", {"statement": case["invalid"], "passage": "smoke"}, tool)
            rules = [rejection.get("rule_id") for rejection in answer.get("rejections", [])]
            show(cid, "invalid write", answer.get("status") == "rejected" and bool(rules),
                 f"{answer.get('status')} {rules} {answer.get('message', '')[:70]}")
        if "annotated" in case:                               # RDF: a value on a relation, in the annotation form
            status, answer = call(f"{base}/cells/{cid}/tool", "POST", {"statement": case["annotated"], "passage": "smoke"}, tool)
            show(cid, "value on a relation", answer.get("status") == "stored",
                 f"{answer.get('status')} {answer.get('id', '')} {json.dumps(answer.get('created', answer.get('message', '')))[:90]}")
            status, answer = call(f"{base}/cells/{cid}/tool", "POST", {"statement": case["annotated_read"]}, tool)
            values = [row.get("value") for row in answer.get("rows", [])] if answer.get("status") == "result" else []
            show(cid, "... and read back", values == [case["annotated_value"]], json.dumps(answer.get("rows", answer))[:80])
        read = case.get("read", ANY[cell["arm"]]["count"])
        status, answer = call(f"{base}/cells/{cid}/tool", "POST", {"statement": read}, tool)
        show(cid, "read", answer.get("status") == "result" and answer.get("row_count", 0) >= 1, json.dumps(answer.get("rows", answer))[:80])
        status, answer = call(f"{base}/cells/{cid}/tool", "POST", {"statement": read}, {"X-Tool-Key": "wrong"})
        show(cid, "wrong key refused", status == 403, f"http {status} {answer.get('message', '')}")
        other = next((c for c in config["cells"] if c["id"] != cid), None)
        if other:
            status, answer = call(f"{base}/cells/{cid}/tool", "POST", {"statement": read}, {"X-Tool-Key": keys[other["key_env"]]})
            show(cid, "other cell's key", status == 403, f"http {status} {answer.get('message', '')}")
        mcp = f"http://{host}:{cell['mcp_port']}/mcp"
        listed = mcp_call(mcp, keys[cell["key_env"]], "tools/list")
        names = [tool_["name"] for tool_ in listed.get("result", {}).get("tools", [])]
        show(cid, "wrapper lists the tool", names == [cell["tool_name"]], json.dumps(names or listed.get("error", listed))[:90])
        called = mcp_call(mcp, keys[cell["key_env"]], "tools/call", {"name": cell["tool_name"], "arguments": {"statement": read}})
        text = "".join(part.get("text", "") for part in called.get("result", {}).get("content", []))
        try:
            through = json.loads(text)
        except ValueError:
            through = {"status": "?", "message": text[:80] or json.dumps(called.get("error", called))[:80]}
        show(cid, "wrapper, end to end", through.get("status") == "result" and through.get("row_count", 0) >= 1,
             json.dumps(through.get("rows", through))[:80])
        if cell["arm"] == "pg":
            around = cypher_shell(cell, "CREATE (:WrittenAroundTheTool)")       # straight into the store, not through the tool
            left = cypher_shell(cell, "MATCH (n:WrittenAroundTheTool) RETURN count(n) AS n").stdout.strip().splitlines()
            show(cid, "write around the tool", around.returncode != 0 and left[-1:] == ["0"],
                 "undone: " + (around.stderr.strip().splitlines() or ["(the store accepted it)"])[-1][:70])
        else:
            # Fuseki has no check before the commit: the store takes the write, and the tool takes it out
            # again when it next looks at the change log, which is before its next statement.
            pattern = '<urn:smoke:around> <urn:smoke:the> "tool"'
            taken, _ = fuseki(cell, "update", f"INSERT DATA {{ {pattern} }}")
            before = triples(cell, pattern)
            call(f"{base}/cells/{cid}/tool", "POST", {"statement": ANY["rdf"]["nothing"]}, tool)
            after = triples(cell, pattern)
            show(cid, "write around the tool", taken and before == "1" and after == "0",
                 f"in the store {before} time(s), after the tool's next statement {after}")
        status, answer = call(f"{base}/cells/{cid}/unit", "DELETE", headers=control)
        show(cid, "unit closes", status == 200, json.dumps(answer))
        path = RUN / "records" / f"{cid}.jsonl"
        kinds = [json.loads(line)["record_type"] for line in path.read_text(encoding="utf-8").splitlines()] if path.is_file() else []
        show(cid, "record", kinds[:1] == ["unit_open"] and kinds[-1:] == ["unit_close"] and "insert_attempt" in kinds or not case,
             f"{len(kinds)} entries: " + ", ".join(f"{kinds.count(kind)} {kind}" for kind in dict.fromkeys(kinds)))
        print()

    warnings = RUN / "records" / "warnings.jsonl"
    kinds = [json.loads(line).get("kind") for line in warnings.read_text(encoding="utf-8").splitlines()] if warnings.is_file() else []
    expected = ["unattributed_write"] * len(config["cells"])          # one per cell, from the write around the tool
    print(f"warnings file: {len(kinds)} entries ({', '.join(sorted(set(kinds))) or 'none'}); expected {len(expected)} unattributed_write")
    if kinds != expected:
        failures.append("warnings file")
    reset("smoke-" + datetime.datetime.now().strftime("%Y%m%dT%H%M%S"))
    if down:
        stop(delete_data=True)
    print("\nSMOKE TEST " + ("FAILED: " + "; ".join(failures) if failures else "PASSED"))
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    starter = sub.add_parser("start")
    starter.add_argument("--no-build", action="store_true", help="use the image of the last build")
    starter.add_argument("--host", help="the name other machines reach this one by, for the address list")
    lister = sub.add_parser("addresses")
    lister.add_argument("--host")
    resetter = sub.add_parser("reset")
    resetter.add_argument("name", nargs="?", help="folder under run/records for the records of the finished pass")
    resetter.add_argument("--force", action="store_true", help="also when a unit is open")
    stopper = sub.add_parser("stop")
    stopper.add_argument("--delete-data", action="store_true", help="delete the graphs (volumes) as well")
    smoker = sub.add_parser("smoke")
    smoker.add_argument("--down", action="store_true", help="stop and delete the graphs afterwards")
    for command in (starter, smoker):
        command.add_argument("--arm", choices=("pg", "rdf", "all"),
                             help="the cells of one arm only, or of both (default: as the last start, else both)")
    args = parser.parse_args()
    if args.command == "start":
        start(args.no_build, args.host, args.arm)
    elif args.command == "addresses":
        print_addresses(load_config(), args.host or os.environ.get("PILOT_HOST") or socket.gethostname())
    elif args.command == "reset":
        reset(args.name, args.force)
    elif args.command == "stop":
        stop(args.delete_data)
    elif args.command == "smoke":
        return smoke(args.down, args.arm)
    return 0


if __name__ == "__main__":
    sys.exit(main())
