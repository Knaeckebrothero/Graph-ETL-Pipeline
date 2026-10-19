# Fessi Redeployment Plan — University Server (10.18.2.105)

> **Historical record.** Drafted on 2026-05-20 and never executed: work stopped before Phase 0. Kept for its recon of the university server. The current deployment state is in [`deploy/README.md`](../README.md). The key mentioned under "Leaked OpenAI key" has been redacted from this copy.

**Status:** Plan locked, execution paused before Phase 0.
**Last update:** 2026-05-20
**Drafted by:** Claude Code session, working from Graph-ETL-Pipeline + HomeLab repos and live recon of the server.

---

## Goals

1. Get the current Advanced-LLM-Chat (barcode scanner + new Neo4j graph + multimodal) running on the university server at **10.18.2.105**, replacing the obsolete `llm-chat-app` monolith that's currently deployed there.
2. Migrate from rootless Podman quadlets → **k3s** on the same host, leaving the LLM/embedding serving infrastructure on quadlets untouched.
3. Capture the install + manifest adaptations as a reusable runbook for the **Proxmox client deployment** (separate project: installing a self-built k8s/helm system at a client whose host runs Proxmox).

---

## Current state on 10.18.2.105 (as of 2026-05-20 recon)

**Host:** Fedora 42 Server (`workstation`), kernel 6.18.12. Reached via university VPN (ppp0 → 194.95.83.200). Multi-tenant: one rootless-Podman service-user per app.

| User | What's running |
|---|---|
| `fessiprod` | `llm-chat-app` (monolithic, ChromaDB, broken — gpt-oss no longer hosted), `fessi-neo4j` (5.15 — OLD graph, running but unused), `fessi-postgres` (running but unused) |
| `llmprod` | Gemma-4 MoE on 2 GPUs, Pixtral-12B, Whisper STT (:8087), Kokoro TTS, **model-orchestrator (:8088-8090)**, gpt-oss-20b (exited) |
| `embeddingprod` | TEI Qwen3 embeddings (:8081, :8083), vLLM Qwen3 reranker (:8082) |

**Disk:** `/` is **94 % full** (27 GB free of 445 GB). Must reclaim space before installing k3s + pulling images.

**Stale artifacts on `fessiprod`:**
- 9 quadlet files from an abandoned split-architecture deployment (`fessi-backend.container`, `fessi-frontend.container`, `fessi-cert-init.container`, `fessi-init.container`, `fessi-certs.volume`, `fessi-filesystem.volume`, `fessi.network`, `fessi-nginx-ssl.conf`, etc.) — defined but not running.
- A separate `neo4j.container` quadlet (Mar 3) for the new graph DB — also defined, not running. Its volume `systemd-neo4j-data` already holds **6.1 GB**, almost certainly from an earlier `neo4j-admin database load`.
- Two copies of the same 1 GB `neo4j.dump` (in `~fessiprod/` and `~fessiprod/dumps/`).

**The currently running `llm-chat-app` is obsolete and unused.** It points to ChromaDB collection `Entsorgung_FRA_UAS` and an OpenAI base URL on port 9002 that doesn't exist anymore. No one uses it. Safe to stop without coordination.

---

## Locked design decisions

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | Access | Internal-only via uni VPN | No public DNS; uni VPN is the existing access path |
| 2 | TLS | Self-signed Traefik cert at edge, single cert for everything | Avoids the historical browser-trust pain that occurred when frontend and backend had separate self-signed certs on separate ports |
| 3 | Ingress | k3s built-in Traefik on hostPorts 80/443 | Default, transfers cleanly to Proxmox via helm |
| 4 | Image architecture | **Split** (frontend + backend), matches existing HomeLab manifests | Closer to typical client deployment pattern; bigger educational delta from current monolith |
| 5 | Image source | `ghcr.io/knaeckebrothero/advanced-llm-chat-backend:develop-latest` + `-frontend:develop-latest` (built by existing GitHub Actions) | No manual build; CI already produces these |
| 6 | Storage class | `local-path-provisioner` (k3s default) | No Longhorn here; works the same on Proxmox |
| 7 | Secrets | Plain `Secret` resources (no External Secrets Operator) | Skip ESO for now; introduce in a Phase 2 if needed |
| 8 | Migration scope | All four fessi components: backend, frontend, postgres, neo4j. Leave `llmprod` and `embeddingprod` on quadlets. | Clean split: fessi-owned → k3s; shared LLM infra → unchanged |
| 9 | Neo4j password | Canonical value from `credentials.txt` (the "fessi (neo4j db user)" entry) | Single source of truth; current state has three inconsistent values |
| 10 | Retrieval backend | Neo4j only — current code does **not** use ChromaDB | Confirmed: vector DB was used in the old prototype, not in current code |
| 11 | LLM endpoint | `http://10.18.2.105:8090/v1` (model-orchestrator) | Already running on `llmprod`; replaces the dead port 9002 reference |
| 12 | Cutover | Single step: stop old quadlets → apply manifests | Old chatbot is broken anyway (no gpt-oss); no parallel running needed |

### Single-origin ingress design

The browser-trust pain is solved by routing everything through one Traefik origin:

```
Browser ─► https://10.18.2.105 ─► Traefik (one self-signed cert)
                                    ├─ /api/* ─► fessi-backend  (plain HTTP inside cluster, USE_DEV_CERTS=False)
                                    └─ /*     ─► fessi-frontend (nginx, env.js: API_URL=/api)
```

- Frontend's `env.js` gets `API_URL=/api` — a **relative** URL, same origin.
- Backend runs HTTP-only inside the cluster (no self-signed cert generation, no DEV_CERT volume).
- User accepts the Traefik cert once, the whole app works.

---

## Open / deferred concerns (not blocking, but noted)

- **OS support expired:** Fedora 42 reached EOL 2026-05-13. Plan an upgrade to 43 after this migration stabilises.
- **Leaked OpenAI key:** An OpenAI service-account key appears in `credentials.txt`, `~fessiprod/.config/containers/systemd/fessi.env`, and `~fessiprod/.config/containers/systemd/llm-chat.env`. **Rotate during this migration.**
- **GPU sharing:** Not needed for fessi pods (backend just HTTPs to model-orchestrator). If a future workload needs GPU in k3s, we'd add the nvidia device plugin then — out of scope here.

---

## Execution phases

### Phase 0 — Pre-flight cleanup (no risk to running services)

On `fessiprod` via SSH:

```bash
# Free disk (~1 GB)
rm ~/neo4j.dump                            # keep ~/dumps/neo4j.dump as the canonical copy

# Drop unused images from the abandoned split-architecture deployment
podman image prune -a                      # removes images not referenced by any container
```

On `admin` (root via sudo) — only if disk still tight:
- `sudo journalctl --vacuum-size=500M`
- `sudo dnf clean all`

**Exit criteria:** `df -h /` shows ≥ 50 GB free.

---

### Phase 1 — Install k3s

As `admin` via sudo:

```bash
sudo dnf install -y container-selinux                    # required for SELinux integration
sudo systemctl stop firewalld && sudo systemctl disable firewalld   # locked decision

curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC='--selinux \
  --tls-san=10.18.2.105 \
  --node-ip=10.18.2.105 \
  --write-kubeconfig-mode=644 \
  --disable=servicelb' sh -

# Make kubectl usable for admin
sudo cp /etc/rancher/k3s/k3s.yaml ~admin/.kube/config
sudo chown admin:admin ~admin/.kube/config
```

**Verification:**
- `kubectl get nodes` → Ready
- `kubectl get pods -A` → traefik, coredns, local-path-provisioner running
- `curl -k https://10.18.2.105` → Traefik default 404 (ingress live)

**Reusable for Proxmox runbook:** install command + flags + firewalld decision + SELinux step.

---

### Phase 2 — Adapt the HomeLab manifests

Source: `/home/ghost/Repositories/HomeLab/deployments_managed/fessi/` (existing K8s manifests built for the homelab cluster).
Destination: `/home/ghost/Repositories/HomeLab/deployments_managed/fessi-university/` (new directory, preserve originals).

Per-file changes vs the originals:

| HomeLab file | Action |
|---|---|
| `namespace.yaml` | Use as-is |
| `01-eso.yaml` | **Skip** — replace with plain `Secret` manifest (`01-secrets.yaml` generated via `kubectl create secret --dry-run=client -o yaml`) |
| `configmap.yaml` | `CORS_ORIGINS=https://10.18.2.105`, `API_URL=/api` (relative), `OPENAI_BASE_URL=http://10.18.2.105:8090/v1`, vision/whisper similarly point at orchestrator |
| `postgres.yaml` | `storageClassName: longhorn` → `local-path` |
| `neo4j.yaml` | `storageClassName: longhorn` → `local-path`. Bump image to `5.26-community`. Add init job to load `neo4j.dump` (see Phase 3). |
| `backend.yaml` | Image → `ghcr.io/knaeckebrothero/advanced-llm-chat-backend:develop-latest`. Set `USE_DEV_CERTS=False`. Strip Cloudflare-Tunnel bits. |
| `frontend.yaml` | Image → `ghcr.io/knaeckebrothero/advanced-llm-chat-frontend:develop-latest`. Strip Cloudflare-Tunnel bits. Inject `API_URL=/api` at startup. |
| `fleet.yaml` | **Skip** — no Fleet here |

New file:
- `50-ingressroute.yaml` — Traefik IngressRoute on `https://10.18.2.105`, routes `/api/*` → backend Service, `/*` → frontend Service, with the default self-signed cert.

Commit the new directory to the HomeLab repo as part of execution.

---

### Phase 3 — Data migration

**Neo4j:**
- Re-import the dump cleanly into the new PV via a Job (not by copying the existing 6.1 GB volume — too coupled to whatever state it's in).
- `kubectl cp ~fessiprod/dumps/neo4j.dump fessi-neo4j-init-0:/dumps/` then run `neo4j-admin database load --from-path=/dumps neo4j --overwrite-destination=true` in an init container.
- Backup the dump file to an off-host location first.

**Postgres:**
- Starts empty. The init job (`50-init-job.yaml` in HomeLab originals, adapted) creates schema.

**ChromaDB:**
- Not used. No migration.

---

### Phase 4 — Deploy and smoke-test

```bash
kubectl apply -f deployments_managed/fessi-university/namespace.yaml
kubectl apply -f deployments_managed/fessi-university/01-secrets.yaml      # generated locally
kubectl apply -f deployments_managed/fessi-university/configmap.yaml
kubectl apply -f deployments_managed/fessi-university/postgres.yaml
kubectl apply -f deployments_managed/fessi-university/neo4j.yaml
# wait for the neo4j init job to complete; then:
kubectl apply -f deployments_managed/fessi-university/backend.yaml
kubectl apply -f deployments_managed/fessi-university/frontend.yaml
kubectl apply -f deployments_managed/fessi-university/50-ingressroute.yaml
```

**Smoke tests:**
- `kubectl -n fessi get pods` — all Running
- `curl -k https://10.18.2.105/api/health` — OK
- Browser: open `https://10.18.2.105`, accept cert once, try a waste-disposal question and the barcode scanner
- Validate Neo4j tool: ask about an EAN that should be in the graph

---

### Phase 5 — Cutover

On `fessiprod`:

```bash
systemctl --user stop llm-chat fessi-postgres fessi-neo4j
systemctl --user disable llm-chat fessi-postgres fessi-neo4j
```

The old chatbot is broken anyway (no gpt-oss backend), so no users to coordinate with.

---

### Phase 6 — Cleanup & runbook capture

After ~1 week of stability:

- Remove the 9 stale `fessi-*` quadlet files in `~fessiprod/.config/containers/systemd/`
- Remove old volumes (`systemd-fessi-postgres`, `systemd-fessi-neo4j`, `systemd-neo4j-data`)
- Remove `~fessiprod/dumps/neo4j.dump` (or archive off-host)
- Commit `deployments_managed/fessi-university/` final state
- Write `deployments_managed/fessi-university/RUNBOOK.md` — the proving-ground artifact for Proxmox

---

## What transfers to the Proxmox client install (and what doesn't)

**Transfers cleanly:**
- Single-node k3s install with Traefik + local-path-provisioner + plain Secrets
- Single-origin ingress design (one cert, path-based routing)
- Workload split: deploy app in k3s, leave host infrastructure outside
- Manifest adaptation pattern (remove Longhorn/cert-manager/Cloudflare-DNS/Tunnel/ESO dependencies)

**Doesn't transfer — needs different handling on Proxmox:**
- **SELinux → AppArmor** (Debian default). Different security policy model.
- **firewalld → pve-firewall** + `fwbr*` per-VM bridges. More aggressive network mgmt.
- **rootless-Podman neighbours → KVM/LXC VMs on Linux bridges.** Different coexistence failure modes.
- **Storage:** Proxmox usually has ZFS or LVM-thin for VM disks; PVs need to dance around those.

---

## Pause point (2026-05-20)

Plan finalised. Awaiting user before Phase 0. **Next action:** SSH as `fessiprod`, run the cleanup commands shown in Phase 0.
