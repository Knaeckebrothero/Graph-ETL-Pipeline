# Shared harness validator (Thesis B), version 0.2.0, 15 Nov 2026

Agreement version 1.0, section 3.2: the harness validator is built and owned by Thesis B since 14 Nov 2026. What it
checks is fixed by Thesis A: the common-scope rule list (`docs/technique/common-scope-rules.md`, **version 0.2 of
15 Nov**, pull request 3) and the constraints each realisation declares (`schemas/treatments/*/declaration.json`,
**version 0.2 of 15 Nov**, Rule 5 "what is binding"). Frozen at F4 (23 Nov) with Thesis A's rule list.

Version 0.2.0 follows Thesis A's three decisions of 15 Nov: the rule list v0.2 (two test moments, ids `CS0` to `CS7`,
`TS-*`), the database tool with its handover point (`experiments/harness/dbtool/checker.py`: the agent writes Cypher
or SPARQL, the store runs it, the tool hands the checker **what the statement changed**), and the enforcement rules of
the schema treatments (a label outside the list, a relationship matching no pattern and a uniqueness violation are
rejected; a property the schema does not name is accepted and counted).

**Status: the library is complete for the property-graph side and runs against Thesis A's `changes`; it is not yet
started inside Thesis A's tool** (that is one line of configuration, section 3). RDF: the adapter exists
(`diff_from_ntriples`), Thesis A's RDF side of the tool does not.

## 1. Two entry points, one rule code (rule list section 9)

```python
from validator import Checker, Validator, FormatTable, EquivalenceTable

# 1. before the database: Thesis A's tool calls this on every statement that changed something
c = Checker()                                   # declarations from schemas/treatments/<realisation>/declaration.json
faults = c.check(changes, lookup)               # [{"rule_id", "element", "expected", "found", "message"}, ...]; [] = pass
c.observations                                  # counted, never rejected: properties the schema does not name (Rule 5)
c.not_checkable()                               # rules no binding allowed to decide (never a pass)

# 2. after the run: the canonical export, one assertion per line
v = Validator(equivalence=EquivalenceTable.load(...), formats=FormatTable.load(...))
report = v.check_export(assertions, write_log={element_id: unit_id})   # run_end.global_validator + detail
```

```bash
python3 -m pytest experiments/thesis-b/validator/tests -q                 # 29 tests here, 36 with Thesis A's schemas checked out
python3 experiments/thesis-b/validator/validate_export.py EXPORT.jsonl --equivalence ... --formats ... --log RUN.jsonl --detail
```

## 2. Files

| File | What |
|---|---|
| `checker.py` | `Checker.check(changes, lookup)` and the factory `checker()` for Thesis A's `load_checker("validator.checker:checker")` |
| `items.py` | the item model of the write (entity item, relation item) and the adapters: `diff_from_changes` (Thesis A's tool), `diff_from_ntriples` (RDF), `diff_from_neo4j` (driver objects) |
| `rules_write.py` | CS1, CS3, CS4 at the write; treatment scope `TS-type`, `TS-unique`, `TS-endpoint` (and `TS-required`, `TS-datatype`, `TS-valuelist` for a declaration that carries them) under the realisation's enforcement |
| `rules_common.py` | CS1 to CS7 on the export |
| `rules_treatment.py` | `Declaration`: loader for `declaration.json` (`enforcement`, `identifying_value`, uniqueness, patterns) |
| `validator.py` | `Validator` with `check_write` and `check_export`, `Result` (`to_log()`, `message()`, `engine_result()`, `element_ids`) |
| `vocabulary.py` | `EquivalenceTable`, `FormatTable` (Thesis A's format and unit table; provisional formats until 23 Nov) |
| `changes.schema.json` | Thesis B's reading of Thesis A's `changes` (checker.py docstring) as JSON Schema, for tests |
| `canonical.schema.json` | **draft** of one canonical assertion on the export; revised to BE-08 modalities; Thesis A confirms |
| `examples/` | `changes.example.json` (Thesis A's rejected-write test, by hand), `inserted.example.nt` (RDF 1.2 annotation form), a four-line export, placeholder equivalence table and unit list |
| `tests/` | the boundary graphs on the export, Thesis A's tool situations on the write, Rule 5, the RDF adapter, Thesis A's real declarations when present |

## 3. Starting it inside Thesis A's tool

`experiments/harness/dbtool/cells.example.json`: `"checker": "validator.checker:checker"`, and the tool started with
`PYTHONPATH=experiments/thesis-b` (the folder name has a hyphen, so it is not importable as a package path). The
checker finds `schemas/treatments/<realisation>/declaration.json` from the repository root; `KG_VALIDATOR_ROOT`
overrides the root, `KG_VALIDATOR_FORMATS` names Thesis A's format and unit table once it exists. Nothing else is
configured: the mode (reject, log_only), the message, "already present", CS0 and the record are the tool's.

What the checker returns is exactly Thesis A's four fields (plus `message`, which `normalise` drops). What the tool does
with the answer: reject mode rolls the transaction back and sends the agent `message(faults)`; log-only keeps the write
and records the faults.

## 4. Rules

**At the write** (on the items of `changes`; rule list v0.2 sections 3 to 5):

| Rule | Checks | Note |
|---|---|---|
| `CS0` | the statement cannot be run | reported by the tool, not by this code |
| `CS1` | empty type, attribute name or relation type; an entity without an identifying value; a relation with a missing end; an empty or white-space value; control characters | identifying value = a declared unique property, else `uid`/`id`/`key`/`name`/...; in S1 only `Entity` needs one (`identifying_value`), an `Assertion` node is a reification |
| `CS3` | an end of a relation resolves to exactly one entity | in the property graph the store resolved the ends before `changes` exists (`ends`); an end that matched nothing or several leaves an empty or multiplied diff, which the N-Triples path reads from the statement (`diff_level`); Thesis A's tool answers "already present" for an empty diff itself |
| `CS4` | a value given with a datatype is a valid form of it | driver-typed values are already valid; typed RDF literals and the explicit form `{"value", "datatype", "unit"}` are parsed; units `not_checkable` until Thesis A's table |
| `TS-type` | label, class or relationship type outside the list | rejected where `enforcement` says so (S3, S4); S1 declares nothing |
| `TS-endpoint` | relationship whose type and end types match no declared pattern | rejected (S3, S4); `not_checkable` when an end has no label |
| `TS-unique` | a declared unique value already in the graph (`lookup.find`) or twice in one statement | rejected (S3, S4); a changed node keeping its own value is no violation |
| `TS-property` | a property the schema does not name | **accepted and counted** (Rule 5): an observation, never a fault |
| `TS-required`, `TS-datatype`, `TS-valuelist`, `TS-cardinality` | from a declaration that carries them | n.a. for every realisation of v0.2, as card P.4 foresees |

Order: common-scope rules first; an item that fails one is not tested against the schema's rules (rule list, CS4).
Every fault of a readable statement comes back in one list. Nothing from the benchmark in `expected`: only what the
schema description or the unit already shows the agent. CS2, CS5, CS6 and CS7 are never tested at the write.

**On the export** (canonical assertions; rule list v0.2 section 4 "counts"):

| Rule | Counts | Needs from Thesis A |
|---|---|---|
| `CS1` | subject, predicate or object missing or empty | — |
| `CS2` | predicate or type resolves neither through the mapping nor the equivalence table | mappings (23 Nov), equivalence table (28 Dec); until then `not_checkable`, figure provisional |
| `CS3` | subject or entity-valued object without an identifying value anywhere in the export | — |
| `CS4` | object value that does not normalise to its datatype; unit not in the table | format and unit table (23 Nov) |
| `CS5` | a present qualifier not in BE-08 form: scope DE / state code / 8-digit key / institution id; validity closed-open ISO interval, start before end; modality must / may / must not; condition or exception non-empty. A missing qualifier is no violation | — |
| `CS6` | the chain breaks: no element pointer, or an element the write log does not show as the product of an accepted write with a unit id (`write_log` from `element_ids`); without a log, `provenance.unit_id` stands in | `element_ids` in the log (in since 15 Nov) |
| `CS7` | equal subject, predicate, object and every qualifier after normalisation; first occurrence passes; provenance excluded | equivalence table for the final figure |

A rule that cannot be decided for want of a binding is `not_checkable`, listed in the report and never counted as
pass or reject.

## 5. Expected properties (tests)

Export: full gold passes every rule; half gold passes; the empty export has no violations and no denominator;
duplicated gold violates CS7 and nothing else, once per extra occurrence, and a second run gives the same figures; a
different qualifier is a different fact; the fully hallucinated graph fails CS6 on every assertion; CS6 follows the
write log; wrong qualifiers fail CS5 and nothing else, missing qualifiers nothing; the text blob fails CS2; without
bindings CS2 and CS4 are `not_checkable`.
Write: Thesis A's rejected-write test gives `CS1` (Forbidden node, no identifying value) and `TS-endpoint` in one
answer; its rule write passes; `TS-unique` asks `lookup.find`; a label added to an existing node outside the list is
`TS-type`; an undeclared property is counted, not rejected; S1 entities need a name, assertion nodes do not; S2's
placeholder is no schema; common rules before schema rules per item; CS4 only on typed values; the rejection fits
Thesis A's `validator_rejection`; log-only does not block; the checker loads through Thesis A's factory string.

## 6. Where Thesis B's reading differs from Thesis A's stand-in checker (for the record)

Thesis A's `test_neo4j_cell.py` runs a stand-in with two fixed rules. Under the real rules three of its situations
come out differently, by Thesis A's own list:

1. `CREATE (:Forbidden {x: 1})` is `CS1` (no identifying value) before it is `TS-type`; the test expects `TS-type`.
2. `SET w:Checked` adds a label outside the list: `TS-type` under Rule 5; the test expects the write to be stored.
3. `DETACH DELETE` is reported under `changes.deleted` and no rule of v0.2 speaks about deletions; the validator lets
   it pass and the record keeps it. Whether an agent may delete is Thesis A's call.

And one mismatch inside Thesis A's own files: the tool records `evidence` as the plain passage string, the shared log
schema of 15 Nov requires an object `{quote, locator}` or null.

## 7. Not yet done, and what it needs

| Missing | Needs | Owner |
|---|---|---|
| Start inside the tool (section 3) and one run of Thesis A's 16 tool tests with this checker in place of the stand-in | the development Neo4j with the hook | Thesis A (one config line), Thesis B watches the first run |
| RDF side | Thesis A's RDF tool; the adapter `diff_from_ntriples` reads the inserted triples, CS3 for ends outside the write needs a lookup | Thesis A |
| Format and unit table (CS4 units; formats beyond ISO) | the table, 23 Nov | Thesis A |
| Mappings and equivalence table (CS2, final CS7) | 23 Nov, 28 Dec | Thesis A |
| Thesis A's test cases of rule list section 7 with expected verdicts | 23 Nov; this suite is Thesis B's reading until then | Thesis A |
| Canonical form of the export confirmed (`canonical.schema.json`) | one reading by Thesis A | Thesis A |
| S2 declaration | after the induction | Thesis A |
