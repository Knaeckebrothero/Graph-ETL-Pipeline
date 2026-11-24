# Pilot bundle: stores, database tool and tool address, per cell

Everything the agent framework needs on the database side of a run, for both arms: property-graph cells on Neo4j and RDF cells on Fuseki. One command starts it on any machine with `podman-compose` or `docker compose`.

**Status: 22 November 2026. Runs and passes its smoke test on the development machine with six cells, three per arm, and with either arm alone. Run with Podman only; no agent has written through an RDF cell yet.**

```bash
deploy/pilot/start.sh                 # build the image if the code changed, start, wait until ready, print the addresses
deploy/pilot/start.sh --arm pg        # the property-graph cells only: no Fuseki is built or started (--arm rdf: the RDF cells only)
deploy/pilot/smoke.sh                 # start, test every cell, empty the graphs again (--down: stop and delete afterwards; --arm as above)
deploy/pilot/reset.sh <name>          # empty every cell's graph; the records of the pass go to run/records/<name>/
deploy/pilot/stop.sh                  # stop, graphs kept (--delete-data: graphs deleted)
python3 deploy/pilot/pilot.py addresses --host <name>     # the address list again, with the name other machines use
```

`--arm` is remembered (`run/arm`): `reset.sh`, `stop.sh` and a later `start.sh` without it act on the same cells. Without any choice both arms start.

## What it starts

| Container | What | Reachable from outside |
|---|---|---|
| `pilot-<cell>-neo4j`, one per property-graph cell | Neo4j 5.26.21 Community with APOC core and the commit hook enabled; empty, no constraint or index of a schema file is created | no |
| `pilot-<cell>-fuseki`, one per RDF cell | Fuseki 6.2.0 with one dataset, `cell`, that writes every change into a log ([`../harness/fuseki/cell.ttl`](../harness/fuseki/cell.ttl)); two volumes, the data and the log | no |
| `pilot-dbtool` | the database tool (`experiments/harness/dbtool`) with Thesis B's validator, serving every cell; it reads the change log of every RDF cell from that cell's log volume | port 28080 |
| `pilot-<cell>-mcp`, one per cell | the tool of that cell as an MCP server with one tool: `cypher` for a property-graph cell, `sparql` for an RDF cell (`experiments/harness/mcp`) | ports 28101 and up |

Cells today, from [`cells.json`](cells.json):

| Cell | Schema | Store | Tool | Wrapper port |
|---|---|---|---|---|
| `pilot-s4-pg` | curated | Neo4j | `cypher` | 28101 |
| `pilot-s3-pg` | flat | Neo4j | `cypher` | 28102 |
| `pilot-s1-pg` | raw triples | Neo4j | `cypher` | 28103 |
| `pilot-s4-rdf` | curated | Fuseki | `sparql` | 28111 |
| `pilot-s3-rdf` | flat | Fuseki | `sparql` | 28112 |
| `pilot-s1-rdf` | raw triples | Fuseki | `sparql` | 28113 |

`pilot-s2-pg` and `pilot-s2-rdf` (induced, ports 28104 and 28114) are listed and switched off.

The stores sit on an inner network and reach the tool by its name; nothing but the tool reaches a store. The image `localhost/pilot-dbtool:<hash>` holds a copy of the tool, the wrapper, the validator and the declarations of the moment it was built, so a running pilot does not change when a file changes. `run/BUILD_INFO.json` (also beside the records, and `<engine> run --rm <image> info`) names the commits and the hash of every file.

## What the agent framework needs, per cell

`start.sh` prints this list and writes it to `run/addresses.json`. The keys are in `run/keys.env` (made once, kept).

| For | Value |
|---|---|
| The model's tool: an MCP connector (streamable HTTP) | `http://<host>:<mcp port of the cell>/mcp`, with the header `X-Tool-Key: <DBTOOL_KEY_<CELL>>`. It offers `cypher(statement, parameters, passage, locator, id)` or, for an RDF cell, `sparql(statement, passage, locator, id)` |
| Scripts in the workspace | `RUN_QUERY_URL=http://<host>:28080/cells/<cell>/tool` and `RUN_QUERY_KEY=<DBTOOL_KEY_<CELL>>` |
| The driver, around every job | `POST http://<host>:28080/cells/<cell>/unit` with `{"unit_id", "run_id", "source_id", "stratum"}` to open, `DELETE` to close; header `X-Harness-Key: <DBTOOL_CONTROL_KEY>` |
| Ready or not | `GET http://<host>:28080/health` answers `"status": "ok"` and the cells |
| The schema text of the cell's prompt | `schemas/treatments/<realisation>/description.construction.txt` |
| The units and their order | `experiments/harness/pilot/units.txt` |

**An RDF cell needs the same three things as a property-graph cell,** with its own values: the wrapper address of the cell (ports 28111 and up), `RUN_QUERY_URL` ending in `/cells/<its id>/tool`, and its key (`DBTOOL_KEY_PILOT_S4_RDF` and so on). `run_query` in the workspace is the same file for both arms; in an RDF cell it is called without parameters, since SPARQL has none. The prompt's binding has to name the tool `sparql`.

One wrapper per cell: the address is the cell, so a connector needs nothing but its address and the cell's key. A key of one cell is refused by every other cell. The agent pods need a path to the wrapper port, the workspace pods to port 28080; on this machine both listen on every interface (`"bind"` in `cells.json`, or `PILOT_BIND`), and a firewall may still close them.

## Records

`run/records/<cell>.jsonl` holds every call of a cell in order, `run/records/warnings.jsonl` what a person should look at (one file for all cells). `reset.sh <name>` moves them to `run/records/<name>/`, so every pass has its own record. `run/` is not tracked.

## Changing it

- **Add a cell:** one line in `cells.json` (id starting with `pilot-`, realisation, a free port), then `start.sh`. A new cell gets its key and its store; the others keep their graphs.
- **Switch the induced schema on:** set `"enabled": true` for `pilot-s2-pg` or `pilot-s2-rdf` once the declaration of its realisation holds a schema. Until then the start refuses it.
- **Changed code or cells:** `start.sh` notices, makes the containers again and keeps the graphs. It refuses while a unit is open.
- **Another machine:** clone both checkouts there (this repository and the branch `thesis-b/f3-spec` beside it, or `VALIDATOR_CHECKOUT=<path>`) and run `start.sh`; the build needs the Python and Neo4j images and PyPI. For RDF cells the Fuseki image (`fuseki_image` in `cells.json`) is built from `deploy/local/fuseki` when the machine does not have it, which downloads the Fuseki 6.2.0 jar; it has to be saved and loaded like the tool's image for a start without a build. Without a build: `podman save localhost/pilot-dbtool:<hash>` here, `podman load` there, copy `deploy/pilot/` with `run/BUILD_INFO.json`, then `start.sh --no-build`.
- **Memory:** each Neo4j takes a 1 GB heap and 512 MB page cache (`neo4j_heap`, `neo4j_pagecache` in `cells.json`), each Fuseki a heap of up to 1 GB (`fuseki_heap`). The tool keeps a list of every RDF cell's graph in memory, about 300 bytes per triple. What six cells take together was not measured.

## To know

- **One tool process serves all cells.** `reset.sh` and a restart touch every cell, so both refuse while a unit is open anywhere. Emptying one cell while others run would need an address for it in the tool.
- **Nothing comes back by itself.** No container has a restart policy: a tool that came back on its own in the middle of a unit would answer "no unit is open" to an agent. After a crash or a reboot, start again; the graphs are in their volumes.
- **A write around the tool is undone** (the smoke test tries it) and leaves an `unattributed_write` warning. Neo4j refuses it at once. Fuseki takes it, and the tool takes it out again before its next statement or the next unit, so for that moment it is in the graph. To look into a store: `podman exec -it pilot-s4-pg-neo4j sh -c 'cypher-shell -u neo4j -p "${NEO4J_AUTH#neo4j/}"'`, reads only.
- **Emptying an RDF cell** (`reset.sh`) deletes its data volume, its log volume and the tool's mark of how far the log is dealt with (`run/records/<cell>.settled`): the three belong together, because the tool rebuilds its list of the graph from the log. The stores, the tool and the wrappers are made again for it; the Neo4j stores keep running.
- **The log volume of an RDF cell is prepared before anything starts** (an empty log that belongs to Fuseki's user). Store and tool both mount it, Fuseki creates the log only with its first write, and Podman hands a volume that is still empty to the user of the container that mounts it next; without this step that was the tool, and the store could then not write ("cell.rdfp (Permission denied)").
- **The login of the Fuseki stores** is generated into `run/fuseki/shiro.ini`, which other users of this machine can read: the store's own user inside its container has to. The stores are not published, so only the tool can use it.
- **Not here:** a hash or snapshot of the graph after a unit, the job driver, the workspace image, the read-only tools of the querying role.
- **Stand-ins:** the tool descriptions the model reads (`experiments/harness/mcp/cypher_mcp.py`, and `sparql_mcp.py` with the same sentences and the language swapped) are not frozen text yet. The wrapper refuses a call whose arguments have the wrong type before it reaches the tool, so that call is in the framework's record and not in the tool's.
