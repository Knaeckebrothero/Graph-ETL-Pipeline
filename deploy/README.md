# Deployment

| Folder | Content |
|---|---|
| [`local/`](local/podman-compose.yml) | Three graph backends for development, via `podman-compose`; see below |
| [`server/`](server/README.md) | Neo4j as a rootless Podman Quadlet (systemd unit), with install and dump-import scripts |
| [`history/`](history/) | Earlier deployment plans, kept for their recon |

## Local backends

One backend per graph family, so that a construction method can be compared across data models and not only across schemas:

| Service | Image | Model and query language | Ports (host) | Credentials |
|---|---|---|---|---|
| `neo4j` | `neo4j:5-community` with APOC | Property graph, Cypher | 7474 (Browser), 7687 (Bolt) | `neo4j` / `neo4j_dev` |
| `postgres-age` | `apache/age:release_PG18_1.8.0` (Postgres 18, AGE 1.8.0) | Relational plus property graph, SQL and openCypher | 127.0.0.1:5432 | `fessi` / `postgres_dev`, database `fessi` |
| `fuseki` | `localhost/fuseki:6.2.0`, built from [`local/fuseki/`](local/fuseki/) | RDF, SPARQL 1.1 | 127.0.0.1:3030 | none |

```bash
podman-compose -f deploy/local/podman-compose.yml up -d              # all three; the first run builds Fuseki
podman-compose -f deploy/local/podman-compose.yml up -d postgres-age # or one of them
podman ps --format '{{.Names}} {{.Status}}'                          # each reports (healthy) when ready

podman exec neo4j-etl cypher-shell -u neo4j -p neo4j_dev 'RETURN 1'
podman exec postgres-age-etl psql -U fessi -d fessi -c "SELECT extversion FROM pg_extension WHERE extname = 'age'"
curl -s http://localhost:3030/fessi/sparql --data-urlencode 'query=ASK {}'

podman-compose -f deploy/local/podman-compose.yml down               # stop; add -v to delete the data volumes
```

- **Postgres + AGE.** The image preloads AGE and creates the extension. [`local/postgres/01-age-defaults.sql`](local/postgres/01-age-defaults.sql) puts `ag_catalog` last on the database's search path, so `cypher()` works in every session without `LOAD 'age'`, and plain tables still land in `public`. It runs only when the data volume is first created. One instance holds many graphs (`SELECT create_graph('name')`), each in its own schema. AGE implements a subset of openCypher and has no APOC.
- **Fuseki.** One updatable TDB2 dataset `/fessi`: query at `/fessi/sparql`, update at `/fessi/update`, Graph Store Protocol at `/fessi/data`. There is no UI and no authentication, so the port is bound to localhost. The Java heap is capped at 2 GB through `JAVA_OPTIONS`.
- **Fuseki build files.** [`local/fuseki/`](local/fuseki/) is Jena's Docker kit `jena-fuseki-docker-6.2.0.zip` from Maven Central (SHA-1 `7d7af7cc7d706343a23f76caf7d1dc3227d19df2`, Apache License 2.0). The only change: the two `FROM` lines name their images fully, because Podman rejects short names without an interactive prompt. The build downloads the Fuseki jar from Maven Central and checks its SHA-1. To upgrade, replace the kit with the matching release and change `JENA_VERSION` and the image tag in the compose file.
- **Container names** are fixed (`neo4j-etl`, `postgres-age-etl`, `fuseki-etl`); the server scripts expect `neo4j-etl`. An older stopped container with one of these names blocks `up`; remove the container (its volumes stay) or rename it.
- **Runs.** These are development instances. Construction runs use a throwaway instance each (see the target below and [`experiments/`](../experiments/README.md)).

## Target

Two kinds of Neo4j instance on the university server, plus the chatbot:

| Instance | Purpose | Lifetime |
|---|---|---|
| **Demo** | The fessi chatbot connects to it, so the bot can be shown and tested; also used for manual experiments | Long-running, fixed schema |
| **Run instances** | One per construction run (treatment × source × run), because Neo4j Community holds one user database per instance | Started and removed by the experiment runner |
| **Chatbot** | [Advanced-LLM-Chat](https://github.com/Knaeckebrothero/Advanced-LLM-Chat) backend and frontend, images from GitHub Actions on `ghcr.io`, LLM via the server's model-orchestrator | Long-running |

The chatbot's five Neo4j tools run fixed Cypher against its own vocabulary (`WasteItem-DISPOSED_VIA->DisposalMethod-AVAILABLE_AT->Location`, plus `WasteCategory` and `FAQ`). Against a graph built under a different schema they return nothing. So the demo instance keeps the chatbot's schema until a treatment's graph is deliberately adapted for it; that gap is itself a queryability result.

The same compose or Quadlet files must also run locally, so that experiments do not depend on the VPN or the server.

## University server

Host `10.18.2.105` (Fedora Server, hostname `workstation`), reachable only through the university VPN. Multi-tenant: one rootless-Podman service user per application.

State at the last recon, **20 May 2026** (not checked since):

| User | Running |
|---|---|
| `fessiprod` | The obsolete `llm-chat-app` monolith (broken: its LLM backend no longer exists), `fessi-neo4j` 5.15 with an old graph, `fessi-postgres` (unused). Stale Quadlet files of an abandoned split deployment and a `neo4j.container` Quadlet that was defined but not running |
| `llmprod` | Model serving: Gemma-4 MoE, Pixtral-12B, Whisper STT (:8087), Kokoro TTS, model-orchestrator (:8088–8090) |
| `embeddingprod` | TEI Qwen3 embeddings (:8081, :8083), vLLM Qwen3 reranker (:8082) |

Issues then: the root disk was 94 % full, Fedora 42 had reached end of life on 2026-05-13, and an OpenAI service-account key sat in plaintext in env files under `fessiprod` and needed rotating. The k3s migration planned that day ([`history/2026-05-20-k3s-redeployment-plan.md`](history/2026-05-20-k3s-redeployment-plan.md)) was never started.

## Next steps

1. Check the server's current state over SSH (read-only first).
2. Decide the runtime. Rootless Podman Quadlets are already used on the host and in [`server/`](server/README.md); k3s was planned mainly as practice for a separate client project.
3. Deploy the demo Neo4j and the chatbot, then a script that starts and removes run instances.
4. Replace the default password `neo4j_dev` on anything reachable from the network; keep real credentials out of this repository.
