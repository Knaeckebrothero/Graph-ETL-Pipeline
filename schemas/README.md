# Schemas

The curated fessi schema in its two versions. They are the starting material for the curated treatment of Thesis A. The four treatments of Thesis A, each for the property graph and for RDF, are in [`treatments/`](treatments/README.md) (drafts of 14 November 2026, derived from v5.0 by a script; the LLM-induced schema is a placeholder).

| Folder | Date | Content |
|---|---|---|
| [`v4.0/`](v4.0/) | 5–9 Dec 2025 | `specification.docx` and its PDF export: three layers, 17 node types, 21 tabulated relationship types plus 4 in prose, cardinalities marked "not enforced", and three worked reasoning flows (banana peel, paint bucket with AVV 20 01 27\*, electric toothbrush with a campus override). `schema.cql`: the first Cypher, from commit `7c5bb17` (13 labels, 15 relationship types) |
| [`v5.0/`](v5.0/) | 26 Feb 2026 | `schema.cql` only: five layers (vocabulary, core entities, products, rules, infrastructure), 17 labels, 21 relationship types in 33 commented patterns, 21 uniqueness constraints, 11 indexes. No specification document exists |

Apply a schema with `python src/scripts/init_db.py --schema --schema-file <file>`; the default is v5.0. The script runs only the `CREATE` statements, which define constraints and indexes. The relationship patterns are comments.

## What v5.0 adds to v4.0

- **Layer 0, vocabulary:** `DictionaryEntry` with `SYNONYM_OF`, `TRANSLATION_OF` and `IS_A`.
- **Layer 2, products:** `Product`, `PackagingComponent` and `Material`, with `HAS_COMPONENT`, `MADE_OF`, `DEFAULT_STREAM` and `IS_A`, plus `Material-CLASSIFIED_AS->AVVCode`, `Product-IS_A->WasteItem`, `PackagingComponent-HAS_RULE->DisposalRule` and `Product-DERIVED_FROM->Source`.
- The rules and infrastructure layers are unchanged.

## Known problems

**Six vocabularies for the same functions.** The split exists from the first Cypher on, not just in v5.0:

| Function | v4.0 spec | Cypher v4.0 = v5.0 | Importers / LLM loader | Chatbot seed |
|---|---|---|---|---|
| Rule → condition | `REQUIRES_CHECK` | `HAS_CONDITION` | condition as a string property | — |
| Answer branch | `HAS_OPTION`, `NEXT_CHECK` | `IF_TRUE` / `IF_FALSE` | `ROUTES_TO {condition}` | — |
| → stream | `LEADS_TO`, `IMPLIES_STREAM` | `ROUTES_TO` | `DISPOSED_IN` (item → stream, no rule) | `DISPOSED_VIA` |
| → facility | `AVAILABLE_AT` | only `Container-LOCATED_AT->Facility` | `DISPOSED_AT` | `AVAILABLE_AT` |
| Override | `OVERRIDES` | — | — | — |
| Legal check | `ALLOWS_AVV` | `REQUIRES_AVV` | — | — |

The Data Engineering report mixes the spec and Cypher columns, which makes six.

**Never exercised.** Only 9 of the 17 v5.0 labels and 9 of the 21 relationship types ever held data. The rules layer never held a node; in the infrastructure layer only `Facility` did, and only in December 2025. `Product-IS_A->WasteItem`, `Material-CLASSIFIED_AS->AVVCode` and `PackagingComponent-HAS_RULE` never occur.

**Not enforced.** Neo4j Community has uniqueness constraints only, no existence or type constraints. The importers wrote off-schema edges without error, so v5.0 was an open schema in practice. Conformance has to be checked outside the database.

**Overloaded types** (test cases for OntoClean/OOPS!): `IS_A` carries word hypernymy, material taxonomy and Product→WasteItem; `WasteItem` is a role label added onto `DictionaryEntry` nodes; `CLASSIFIED_AS` is reused across domains; `IF_TRUE`/`IF_FALSE` point to either a rule or a stream.

**Design method.** Both versions were designed around the three worked flows, not around competency questions. The v4.0 specification was drafted with ChatGPT and cites none of the 19 schema papers collected in the Teams folder.

## Open decisions for the curated treatment

- Freeze one reconciled vocabulary: v5.0 is the only executable artefact, but the v4.0 specification has the richer rule semantics (n-ary answers, overrides, legal-provision checks).
- Decide how conformance is enforced: by the agent prompt, by an external validator, or both.
- Characterise every treatment with the same eight-attribute matrix from the exposé (§ 4.1).
