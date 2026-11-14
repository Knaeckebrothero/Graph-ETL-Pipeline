# Shared harness validator (Thesis B), version 0.1.0, 14 Nov 2026

Agreement version 1.0, section 3.2: the harness validator is built and owned by Thesis B since 14 Nov 2026. Local
per-insert rules, global end-of-run rules, reject and log-only mode, uniform messages carrying rule id, element,
expected and found; delivered as pull requests to the harness, reviewed and merged by Thesis A. What it checks is
fixed by Thesis A: the common-scope rule list is part of Thesis A's technique (Eval 2 package v0.1, card P.3), the
treatment-scope rules follow from the constraints each schema realisation declares (`schemas/treatments/*/declaration.json`).
Frozen at F4 (23 Nov) together with Thesis A's rule list and mappings.

**Status: core, not yet wired into the harness.** Everything below runs offline on canonical assertions and schema
writes. What is still missing is listed in section 5; it needs Thesis A's inputs or decisions.

## 1. Files

| File | What |
|---|---|
| `validator.py` | `Validator` (modes, local common scope, local treatment scope, global export check), `GraphView`, `Result` with `to_log()` and `message()` |
| `rules_common.py` | CS-01 to CS-07, the common-scope rules of card P.3, on canonical form |
| `rules_treatment.py` | TS-01 to TS-05 from a realisation's `declaration.json`, on the write as rendered into the schema |
| `vocabulary.py` | loaders for the equivalence table and the unit list (Thesis A's Domain Binding; placeholder format) |
| `canonical.schema.json` | **draft** JSON Schema of one canonical assertion, Thesis B's reading of Eval 2 §1.2; Thesis A confirms or corrects |
| `validate_export.py` | CLI: global check of a canonical export (JSON Lines) |
| `examples/` | placeholder equivalence table and unit list; a four-line export with one duplicate and one unanchored assertion |
| `tests/` | expected properties on Thesis A's seven boundary graphs; treatment scope against a declaration; Thesis A's real declarations when present |

```bash
python3 -m pytest experiments/thesis-b/validator/tests -q
python3 experiments/thesis-b/validator/validate_export.py experiments/thesis-b/validator/examples/export.example.jsonl \
    --equivalence experiments/thesis-b/validator/examples/equivalence.example.json \
    --units experiments/thesis-b/validator/examples/units.example.json --schema --detail
```

## 2. Rules

**Common scope** (Thesis A's list, card P.3; identical in both arms; on canonical form, never on engine errors):

| Rule | Checks | Needs from Thesis A |
|---|---|---|
| CS-01 | subject, predicate and object well formed | — |
| CS-02 | predicate and type mapped or mappable under the equivalence table | equivalence table; until then `not_checkable` |
| CS-03 | no dangling reference: an entity referenced by id exists in the graph or is introduced in the same write | the harness fills `GraphView.entity_ids` |
| CS-04 | datatypes parse (string, integer, decimal, boolean, date, datetime, duration); units are in the unit list | unit list; until then units `not_checkable` |
| CS-05 | qualifiers well formed: known keys only (scope, validity_time, condition, exception, modality, priority), dates parse, from <= to, modality in its list | confirm the qualifier set |
| CS-06 | provenance pointer present: `unit_id` plus a locator or a quote inside the unit | — |
| CS-07 | no duplicate assertion: same subject, canonical predicate, object and qualifiers (provenance is not part of the identity, card P.8) | confirm the identity rule |

A rule that cannot be decided for want of a binding is reported as `not_checkable`, listed in the log record and in the
global report, and never counted as pass or reject. Nothing is approved silently.

**Treatment scope** (from `declaration.json`, card P.4):

| Rule | Checks | Declared by v5.0? |
|---|---|---|
| TS-01 | node label and relationship type declared | yes |
| TS-02 | node property declared for its label | yes (constraints, indexes, `// Properties:` comments) |
| TS-03 | relationship pattern (from label, type, to label) declared | yes (reference patterns) |
| TS-04 | uniqueness: value not already used for that label and property | yes (21 constraints) |
| TS-05 | relationship property declared on the pattern | yes (`HAS_COMPONENT {qty, weight_g}`) |
| — | required property, datatype, cardinality, closed value list | **no**: n.a. for every realisation of draft 0.1, as card P.4 foresees |

S1 declares nothing, so every write passes the treatment scope. S2 is a placeholder until the induction; the loader
refuses it. PG and RDF declarations of one treatment carry the same content, so the rules are the same in both arms.

## 3. Modes and the log record

- `reject`: a rejected write is not sent to the engine (`engine_result: not_sent`, `accepted: false`); the agent
  receives `Result.message()`, one line per rejection, item id first, and resends the corrected candidates with the
  same item ids (shared-minimum-log/1 README, "The item id").
- `log_only` (configuration A6): the same checks, the same record, but the write goes through. Thesis A's card P.9
  counts these separately and never as a rejection.
- `Result.to_log()` gives exactly the fields of `insert_attempt` in shared-minimum-log/1: `validator_mode`,
  `validator_result`, `rejections[rule_id, element, expected, found]` (plus Thesis B's `message` and
  `validator_not_checkable` as extensions). `check_export()` gives `run_end.global_validator` (`checked_rules`,
  `violations`, `by_rule`) plus the per-assertion detail.

## 4. Expected properties (tests)

On Thesis A's seven boundary graphs: full gold passes every rule; half gold passes; the empty export has no
violations and no denominator; duplicated gold violates CS-07 and nothing else, once per extra occurrence, and a second
run gives the same figures; the fully hallucinated graph (no basis in any unit) fails CS-06 on every assertion; wrong
qualifiers fail CS-05 and nothing else; the text blob fails CS-02. Without an equivalence table and unit list, CS-02
and CS-04 are `not_checkable`, not passed. Treatment scope: each of TS-01 to TS-05 is triggered by exactly one
construction and nothing else; a write that matches the declaration passes; Thesis A's real S4/S3/S1 declarations load
and give the inventory of the README (17 / 33 / 21 for S4).

## 5. Not yet done, and what it needs

| Missing | Needs | Owner |
|---|---|---|
| The hook in the SRW write tool: today `cypher_execute(statement: str)` runs raw Cypher with no validator and no item id; there is no SPARQL write tool yet | a structured write tool that takes canonical assertions (`write_assertions([...])`), renders them per realisation, and calls `check_insert` before and `check_write` after rendering; or Thesis A's own hook design | Thesis A builds the tool and the handover point (agreement 3.1); Thesis B wires the validator in |
| Canonical form confirmed | Thesis A reads `canonical.schema.json` and confirms or corrects the field set, the qualifier keys and the literal datatypes | Thesis A |
| Equivalence table and unit list in their final format | Thesis A's Domain Binding | Thesis A |
| Mapping canonical form to each realisation (renderer), so that `check_write` sees labels and properties | Thesis A's mappings, frozen with technique v1.0 (23 Nov) | Thesis A |
| Duplicate identity and the qualifier set confirmed | one line each from Thesis A | Thesis A |
| Rule ids `rule_id` in Thesis A's vocabulary (if a different naming is wanted) | — | Thesis A |
| Treatment scope for S2 | the induced schema per arm, written to `S2-induced/*/declaration.json` by Thesis A's derive.py | Thesis A, after induction |
| Global rules beyond the common scope (e.g. graph-wide uniqueness after commit) | the per-realisation constraints are covered by TS-04; anything else: Thesis A names it | both |
| Pilot wiring (16 Nov) | hook + canonical form; until then the validator can run log-only on the canonical export after each unit | both |

Dates: Thesis A's rule list, mappings and binding tables are frozen at F4 (23 Nov), the same day as this validator.
Thesis B asked on 14 Nov for the hook design and the canonical form by Mon 17 Nov and the bindings by 20 Nov; if they
come later, the validator's freeze moves with them.
