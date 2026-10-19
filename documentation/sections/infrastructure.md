# 8. Infrastructure

The ETL pipelines described in the preceding sections require two things to run: a Neo4j database to write into and a Python environment to execute from. Both of these, along with a JupyterLab instance for interactive development and the chatbot application itself, are deployed on a shared workstation (10.18.2.105) that serves as the project's combined data processing and application server. All services — Neo4j, JupyterLab, and the chatbot — run as rootless Podman containers managed by systemd, sharing the same machine and the same graph database. This section describes how these systems are deployed and how to reproduce or extend the setup.

## 8.1 Why Python

The entire data processing stack is implemented in Python. This was a deliberate choice driven by ecosystem fit rather than language preference. Python provides the tightest integration with the tools the project depends on: LangChain and LangGraph for the LLM extraction pipeline, the `neo4j` driver for database access, PyArrow for reading the multi-gigabyte Open Food Facts Parquet dump, Pydantic for schema-driven extraction, and the `wn` package for WordNet access in the dictionary baseline. Equally important, Python's dominance in data science means that JupyterLab — our primary environment for developing and testing extraction pipelines — treats Python as a first-class citizen. Every pipeline in this project was prototyped in a Jupyter notebook before being formalized into a module, and that workflow would have been significantly more friction-laden in any other language.

## 8.2 Neo4j Deployment

Neo4j 5 Community Edition runs as a rootless Podman container managed by systemd through Podman's Quadlet mechanism. Quadlet files are declarative container definitions — similar in spirit to Docker Compose services but translated directly into native systemd units. This means Neo4j starts on boot, restarts on failure, and integrates with standard Linux service management (`systemctl`, `journalctl`) without requiring a container orchestrator.

The deployment consists of four Quadlet files installed to `~/.config/containers/systemd/`: a container unit (`neo4j.container`) defining the image, ports (7474 for the HTTP browser, 7687 for the Bolt protocol), environment variables, and the APOC plugin configuration; and three companion volume units for persistent storage of graph data, logs, and the APOC import staging directory. An install script (`deployment/scripts/install.sh`) handles the complete setup: it verifies prerequisites (Podman installed, user lingering enabled), copies the Quadlet files, reloads the systemd daemon, enables the service, and polls the HTTP endpoint until Neo4j responds.

The choice of rootless Podman over Docker was motivated by the server environment. The workstation runs Fedora, where Podman is the native container runtime and rootless execution is the default — no daemon, no root privileges, no Docker socket to secure. Quadlet was chosen over a plain `podman-compose` setup (which also exists in the repository under `docker/podman-compose.yml` for local development) because it provides proper service supervision: automatic restarts, boot-time startup, and log aggregation through journald.

## 8.3 JupyterLab

A JupyterLab instance runs on the same workstation, providing an interactive environment for pipeline development, data exploration, and one-off extraction tasks. This co-location was necessary for practical reasons: the Open Food Facts Parquet dump is 4.4 GB of highly compressed columnar data representing 4.29 million products, the JSONL variant adds another 2.6 GB, and several intermediate processing files (the Kaikki dictionary JSONL at 280 MB, classification result caches, OFF subset extracts) add to the data footprint. Running notebooks on a laptop and connecting to a remote Neo4j instance would have meant either transferring these files repeatedly or maintaining duplicate copies. With JupyterLab on the same machine as Neo4j, both the data files and the database are local, and iteration cycles — load a Parquet partition, transform it, write to Neo4j, inspect the result — take seconds rather than minutes.

Every pipeline in this project has a corresponding notebook that served as its development environment: `notebooks/pipeline_example.ipynb` for the LLM extraction chain, `notebooks/avv_classification.ipynb` for the AVV code hierarchy, and additional notebooks for the dictionary baseline and Open Food Facts imports. These notebooks remain the most practical way to understand what each pipeline does, since they show the intermediate outputs that the command-line interfaces hide.

## 8.4 Database Management

Three operations cover the database lifecycle: initialization, dump import, and reset.

**Initialization** is handled by `src/scripts/init_db.py`, which verifies the Neo4j connection, applies schema constraints and indexes from `src/db/schema.cql`, and reports statistics (node counts by label, total relationships). The schema file defines uniqueness constraints on all node types — ensuring that `MERGE` operations in the ETL pipelines behave correctly — and indexes on frequently queried properties. Schema application is idempotent: the script parses `schema.cql` line by line and silently skips any constraint or index that already exists, so it can be re-run safely after a dump import, after manual schema changes, or as a post-deployment smoke test.

**Dump import** allows transferring a complete graph state between environments. The `deployment/scripts/import-dump.sh` script automates the process: it stops the Neo4j service (required by Community Edition for offline loading), mounts the dump file into a temporary container, runs `neo4j-admin database load`, restarts the service, and verifies the result by querying the node count.

**Reset** clears the entire database and is available via `python src/scripts/init_db.py --reset`. It requires interactive confirmation before executing, and is primarily used during development when a clean graph is needed for testing an import pipeline from scratch.

## 8.5 The Chatbot Deployment

The chatbot application (Advanced-LLM-Chat) is a separate repository containing the Angular frontend, FastAPI backend, and the LangGraph-based agent described in Section 2. It ships with three deployment options: a Docker Compose stack for local development (`docker/docker-compose.yml`), a production Compose variant that pulls pre-built images from the GitHub Container Registry (`docker/docker-compose.prod.yml`), and a Helm chart (`helm/fessi/`) for Kubernetes deployment with StatefulSets for both PostgreSQL and Neo4j, configurable ingress, and secrets management.

In practice, the chatbot is deployed on the same workstation as a Podman Quadlet — loosely based on the Docker Compose definition but adapted for rootless systemd management, following the same pattern as the Neo4j deployment. Because all services share one machine, the backend connects to Neo4j at `bolt://localhost:7687` and both the ETL pipelines and the chatbot agent operate on the same graph without any network transfer or synchronization step. The PostgreSQL database that the chatbot uses for session management and user data is a separate concern; it does not interact with the knowledge graph.

## 8.6 Reproducing the Setup

Because the current deployment co-locates all services on a single workstation, the graph built by the ETL pipelines is immediately available to the chatbot agent. For future deployments where the data processing environment and the chatbot run on separate machines, the dump import mechanism described in Section 8.4 provides the transfer path: export a dump from the source Neo4j, transfer the file, and run the import script on the target. The dump captures the complete graph state, so the target environment does not need access to any of the raw data files or pipeline code.

The Neo4j browser is accessible at `http://10.18.2.105:7474` and the Bolt endpoint at `bolt://10.18.2.105:7687`. The ETL pipeline's `.env` file requires three variables for database access (`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`) and, for LLM-dependent pipelines, `LLM_API_KEY` and `LLM_BASE_URL` as described in Section 7.6. An `.env.example` template documents all available variables. Dependencies are installed via `pip install -r requirements.txt`; no system-level packages are required beyond Python 3.12 and Podman.
