# Deployment

| Folder | Content |
|---|---|
| [`local/`](local/podman-compose.yml) | Neo4j 5 Community for development, via `podman-compose` |
| [`server/`](server/README.md) | Neo4j as a rootless Podman Quadlet (systemd unit), with install and dump-import scripts |
| [`history/`](history/) | Earlier deployment plans, kept for their recon |

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
