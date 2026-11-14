# Experiments

The shared empirical substrate of both theses. One run is one cell: a construction agent populates a fresh graph from the whole frozen sample of source units, one unit per task, under one schema treatment in one data model (a Neo4j property graph or RDF in Apache Jena Fuseki). Thesis A builds every treatment in both data models three times with one fixed agent configuration; Thesis B runs further agent configurations on the same sample. Thesis A compares the treatments; Thesis B compares agent configurations and instruments the runs. The run matrix, the identifiers and the log fields are in [`docs/demarcation-agreement.md`](../docs/demarcation-agreement.md), section 4.

Nothing is built yet. This folder will hold:

- **run configurations:** treatment, source IDs from `sources/manifest.csv`, agent configuration, model, prompt template, resource limits;
- **run manifests:** run ID, start and end time, model ID, prompt and schema checksums, source checksums, token use, cost, tool-call log reference;
- **measurements:** the coverage, populatability and queryability results per run.

Raw run output such as logs and graph snapshots goes to `runs/`, which Git ignores.

## Constraints from the earlier code

- **One Neo4j Community instance holds one user database.** Each run needs its own instance; start a throwaway container per run and remove it afterwards. The long-running demo instance on the server is not used for runs.
- **Imports `MERGE` on deterministic IDs,** so repeated runs into one database collapse into one graph.
- **The earlier pipelines logged nothing:** no run IDs, model IDs, tokens or cost. Instrumentation has to be built (Thesis B).
- **The LLM ETL in `src/llm_etl/` is scaffolding.** It takes no schema as input, writes no relationships, and its default JSON mode never sends the schema to the model. Reusable parts: the LLM factory, the structured-output wrapper, the chunking and `schema_to_prompt_hint`.
- **The import-agent design** in `docs/prior-work/design/graph_import_agent.md` is worth building on: source adapter → common record → one shared agent and toolset, bulk classification first, an explicit `skip(reason)` tool with a dead-letter file, checkpoints and resume.
