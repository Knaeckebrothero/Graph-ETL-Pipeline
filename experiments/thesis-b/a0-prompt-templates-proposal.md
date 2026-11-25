# A0 prompt templates: Thesis B's proposal for the joint decision on 13 Nov 2026 (F3)

Scope note (9 Nov): Thesis A's register (EX-06, EX-07) fixes three frozen templates, one per agent role: construction,
querying and S2 induction. This document drafts the construction template only. Querying and induction templates are
Thesis A's roles; Thesis B reads them before F3 and logs their ids and hashes in `run_start.prompt_hashes`.

Handoff, table "How Thesis A measures": *Prompt templates, A0 — one frozen template per agent role, schema slot
filled by the renderer, per-arm query-language binding — built by both — frozen F3, 13 Nov.*
This file is Thesis B's draft of the construction-role templates. The querying-role template is Thesis A's
(query harness); Thesis B only asks that its id and hash are logged the same way.

Status: proposal. Template id proposed: `A-frozen-v1` (the id used in `agent-configs/A0-v1.json`).

## 1. Rules the templates follow

1. **One slot varies between treatments:** `{{schema_description}}`, the renderer output (declared-only view).
   Nothing else in the text depends on the treatment.
2. **One slot varies between arms:** `{{query_language_binding}}`, a fixed sentence per arm (PG: Cypher on Neo4j;
   RDF: SPARQL Update on Jena Fuseki). Constant across treatments.
3. **Parameter slots** (`{{max_self_corrections}}`, `{{max_tool_calls}}`, `{{max_turns}}`) carry configuration
   values. For every A0 cell they are identical. Thesis B's configurations A4, A5 and A10 change them without
   changing the template text, so the template id stays `A-frozen-v1`; the filled values are logged in
   `run_start.settings`.
4. **No domain examples, no domain constants** (no stream names, no city names, no AVV codes). Examples are
   Thesis B's factor A8 and get their own template id (section 5).
5. **Tool-agnostic wording.** The text never names a tool. Tools are declared through the API tool definitions
   that Thesis A's harness supplies per arm. This lets A7 (read tools off) run on the same template.
6. **Source language preserved.** Values are written as the source states them, no translation, no
   normalisation beyond the datatype the schema requires. Thesis A's reference uses source terminology.
7. **Every write is anchored.** Each write names the source unit id and a locator (row, line or character span)
   and quotes the supporting text. Required by BU-14/BU-16 ("per written assertion: the source unit the agent was
   processing") and by Thesis B's supportedness and provenance metrics.
8. **Hash rule.** `sha256` over the UTF-8 template text with placeholders unfilled, LF line endings, trailing
   whitespace stripped. Ids and hashes go into `run_start.prompt_hashes`; any wording change gets a new id.

## 2. System template (construction role) — `A-frozen-v1/system`

```text
You are a knowledge-graph construction agent. You read one source unit at a time and record the facts it
states in a graph database, using only the tools available to you.

{{query_language_binding}}

Schema (this is the complete and only description of the target graph; do not assume types, properties or
relations that are not listed here):

{{schema_description}}

Working rules:
1. Record only what the source unit states. Do not add facts from general knowledge. Do not complete, guess or
   round values.
2. Write every value as the source states it, in the source language, converted only to the datatype the schema
   requires.
3. Before you create an entity, check whether an entity denoting the same real-world thing already exists in the
   graph and reuse it. Create a new entity only when no existing one matches.
4. Open node types may be instantiated whenever the source unit names a new instance. Do not load lists or
   defaults that the source unit does not contain.
5. Every write names the source unit id, a locator inside the source unit and the verbatim text that supports
   the write.
6. Writes are checked by a validator. If a write is rejected, read the rejection, correct the write and retry.
   You may correct the same item at most {{max_self_corrections}} time(s); after that, leave the item out and
   continue with the next one.
7. Stop when the source unit is fully processed, or when you reach the limits below. Then reply with a short
   plain-text summary of what you recorded and what you left out, and make no further tool call.

Limits for this source unit: at most {{max_turns}} model turns and {{max_tool_calls}} tool calls.
```

## 3. Per-source user template — `A-frozen-v1/user`

```text
Source unit {{source_id}} ({{source_modality}}; authority class {{authority_class}}; jurisdiction
{{jurisdiction}}).

{{source_unit}}

Record every fact this source unit states that the schema can represent. Start now.
```

`{{source_unit}}` is the whole unit as text (A0: `prompting.source_delivery = whole`). A9 delivers the unit in
chunks: the same user template is sent once per chunk with `{{source_unit}}` replaced by the chunk and a line
`Chunk {{chunk_index}} of {{chunk_count}}` prepended; template id stays `A-frozen-v1`, delivery mode and chunk size
are logged in `run_start.settings`. If Thesis A's harness delivers the unit as a file path on the VM instead of
inline text, the `{{source_unit}}` line becomes the path and the mode is logged as `path`; the text above does not
change otherwise. Decide on 13 Nov.

Metadata slots (`source_modality`, `authority_class`, `jurisdiction`) come from the frozen sample manifest. They are
labels, not domain knowledge, and the same for every configuration. If Thesis A prefers a blind agent, drop
the three slots in both theses; Thesis B has no requirement either way.

## 4. Per-arm query-language binding (slot `{{query_language_binding}}`)

| Arm | Text |
|---|---|
| PG | `The graph is a Neo4j property graph. Writes are expressed in Cypher through the write tool. Labels, relationship types and property keys must match the schema exactly, including case.` |
| RDF | `The graph is an RDF dataset on Apache Jena Fuseki. Writes are expressed as SPARQL Update through the write tool. Classes, properties and datatypes must use the IRIs given in the schema exactly.` |

Constant across treatments; owned by Thesis A (Formalism Binding table, BU-05). Thesis B only asks that the two
sentences are frozen with the template and hashed.

## 5. Thesis B's template variant for A8 — `A-fewshot-v1`

Examples are taken from the development bucket only, never from pilot, held-out or canary units (EX-16 leakage rule); the unit ids of the examples are recorded with the template hash.

`A-frozen-v1/system` plus one block inserted after the schema:

```text
Worked examples (illustrative form only; the facts below are not part of your source unit):

{{examples}}
```

`{{examples}}` = two or three source excerpts from the development set with the resulting writes, rendered per arm
by Thesis A's renderer in the same canonical form, chosen before the pilot, never from the frozen sample and never
from held-out material (Munich, Berlin, held-out CQ families). Hash of the filled block logged like the template.
Everything else identical to `A-frozen-v1`.

## 6. What Thesis B needs from Thesis A for the freeze

- Confirmation of the slot list (section 1) and the inline vs. path delivery mode (section 3).
- Renderer output for S1 to S4 in both arms, so the filled prompts can be hashed and read once before the pilot.
- The querying-role template id and hash logged in `run_start.prompt_hashes` the same way.
- Agreement that the per-arm binding sentences (section 4) are Thesis A's text and may be edited by Thesis A
  before F3 without consulting Thesis B, as long as they stay treatment-independent.

## 7. Record at F3

`template_id = A-frozen-v1`, `sha256(system)`, `sha256(user)`, both binding sentences with hashes, delivery mode,
parameter values for A0 (`max_self_corrections = 2`; `max_turns`, `max_tool_calls` from the pilot distribution,
set at F4 at the latest), stored next to `A0-v1.json` in the shared repository and referenced by every `run_start`.
