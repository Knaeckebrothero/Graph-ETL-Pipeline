# fessi knowledge-graph theses

Shared workspace for two master's theses at Frankfurt University of Applied Sciences (M.Sc. Wirtschaftsinformatik). Both use the **fessi** waste-disposal chatbot as their case study and analyse the same construction runs: LLM agents import heterogeneous waste-disposal sources into a Neo4j graph under different schemas.

| | Thesis A | Thesis B |
|---|---|---|
| Title | A Technique for Evaluating Knowledge-Graph Schema Fitness across Coverage, Populatability, and Queryability | A Multi-Layer Evaluation Methodology for Agent-Driven Knowledge Graph Construction: Design, Instrumentation, and Case Study in the Waste-Disposal Domain |
| Author | [@Knaeckebrothero](https://github.com/Knaeckebrothero) | Rachid Yagubi |
| Compares | schema treatments (curated fessi schema, raw-triple/EAV, LLM-induced) | agent configurations |
| Supplies to the other | competency questions, reference benchmark | run instrumentation and process data |
| Supervision | Prof. Dr. Josef Fink, second examiner Prof. Dr. Sebastian Bremm | to be added |
| Submission | 16 February 2027 (hard deadline) | to be added |

Both theses are graded as individual work. Keep authorship traceable: commit under your own name, and put each thesis's own analysis under [`analysis/`](analysis/).

## Status (19 October 2026)

The project starts from the sources, import code and documentation of earlier fessi semesters, not from their graphs. No earlier graph is reused: rebuilding graphs under controlled conditions is what the theses study. [`docs/prior-work/`](docs/prior-work/README.md) summarises what was built before, what actually ran, and which claims in the earlier reports do not hold.

| Component | State |
|---|---|
| Sources | 27 files and endpoints in [`sources/manifest.csv`](sources/manifest.csv); candidate catalogue of about 256 unverified sources; no stratified sample yet |
| Import code | Hand-written importers for the structured sources in [`src/etl/`](src/etl/), tied to one vocabulary; LLM extraction scaffolding in [`src/llm_etl/`](src/llm_etl/) |
| Schemas | Curated v4.0 and v5.0 in [`schemas/`](schemas/); EAV and LLM-induced treatments not built |
| Competency questions | 160 legacy questions as seeds in [`questions/`](questions/); about 500 needed |
| Construction agent and runs | Not built; see [`experiments/`](experiments/) |
| Deployment | Nothing current on the university server; see [`deploy/`](deploy/) |

## Repository layout

```
sources/       source files, the manifest and the candidate catalogue
  raw/         small source files, tracked in Git, one folder per source
  large/       large files (Open Food Facts, Wiktionary), kept outside Git
  catalogue/   LLM-generated candidate source list and the research behind it
schemas/       curated schema versions (v4.0 spec and Cypher, v5.0 Cypher)
src/           Python code: Neo4j access, importers (etl/), LLM extraction (llm_etl/), barcode lookup
questions/     competency questions and their seeds
experiments/   run configurations and results (shared by both theses)
analysis/      each thesis's own analysis
deploy/        Neo4j and chatbot deployment, locally and on the university server
docs/          documentation of the earlier semesters and of this project
notebooks/     example notebooks from the earlier semester
private/       local only, never committed: credentials and files with personal data
```

## Where things run

| What | Where | State |
|---|---|---|
| This repository | GitHub, `Knaeckebrothero/Graph-ETL-Pipeline` (private) | Restructured for the theses on 19 October 2026 |
| fessi chatbot | GitHub, [`Knaeckebrothero/Advanced-LLM-Chat`](https://github.com/Knaeckebrothero/Advanced-LLM-Chat) (public); CI builds images to `ghcr.io/knaeckebrothero/advanced-llm-chat-{backend,frontend}` | Not deployed. Barcode work on branch `feature/barcode-scanner` |
| Neo4j for experiments and demo | University server `10.18.2.105`, reachable through the university VPN | Planned. Recon of May 2026 found only an outdated Neo4j 5.15 and a broken chatbot there |
| LLM endpoint | University server, model-orchestrator at `http://10.18.2.105:8090/v1` | Running as of May 2026 |
| Local development | Neo4j 5 Community via [`deploy/local/podman-compose.yml`](deploy/local/podman-compose.yml) | Works |

Details and the deployment plan: [`deploy/README.md`](deploy/README.md).

## Quick start (local)

Requires Python 3.12+ and Podman.

```bash
podman-compose -f deploy/local/podman-compose.yml up -d   # Neo4j at http://localhost:7474
python -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env                                       # set NEO4J_PASSWORD=neo4j_dev and LLM settings

.venv/bin/python src/scripts/init_db.py --schema           # apply schemas/v5.0/schema.cql
.venv/bin/python src/scripts/init_db.py --schema --schema-file schemas/v4.0/schema.cql
.venv/bin/python -m src.etl.facilities --dry-run           # importers read from sources/raw/
```

Large source files are not in Git. [`sources/large/README.md`](sources/large/README.md) lists them and where to get them.

## Working rules

- **No credentials in the repository.** Keep keys and passwords in `.env` or `private/`; both are ignored by Git.
- **No personal data.** Earlier material contains staff contact details, student numbers and persona data; see [`sources/README.md`](sources/README.md#restricted-material). Redact before committing.
- **Every source file goes into the manifest** with its origin, licence, checksum and date, so that gold answers can be traced to a dated source.
- **No graph dumps in Git.** Runs write to throwaway Neo4j instances; results are recorded as measurements in `experiments/`.
- **Report claims only with run artefacts.** Earlier documentation marked steps "complete" that never ran.

## History

- The repository began in December 2025 as the ETL pipeline of the WiSe 2025/26 fessi project. The graded state is tagged [`wise2526-submission`](../../tree/wise2526-submission).
- Earlier fessi semesters (SoSe 2024 to SoSe 2025) and their material are described in [`docs/prior-work/`](docs/prior-work/README.md).

## License

See [LICENSE.txt](LICENSE.txt) for the project's own code and text. Third-party source files keep their own licences, recorded per file in [`sources/manifest.csv`](sources/manifest.csv); several are not cleared for publication.
