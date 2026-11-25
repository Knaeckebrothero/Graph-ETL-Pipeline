# Shared harness validator (Thesis B), version 0.3.0, 25 Nov 2026

Agreement version 1.0, section 3.2: the harness validator is built and owned by Thesis B since 14 Nov 2026. What it
checks is fixed by Thesis A: the common-scope rule list (`docs/technique/common-scope-rules.md`, **version 0.2 of
15 Nov**, pull request 3) and the constraints each realisation declares (`schemas/treatments/*/declaration.json`,
**version 0.2 of 15 Nov**, Rule 5 "what is binding", and **Rule 6 of 23 Nov** for the induced schema). Freeze: F4
was 23 Nov; version 0.3.0 is the state after Thesis A's pilot feedback of 25 Nov (`../deviations.md`, D8).

Version 0.3.0 answers the five points Thesis A reported from the pilot of 23/24 Nov (about 280 jobs, both databases,
validator commit 5d2c858 held up: called on every write, never crashed in a run), most important first:

1. **RDF: the graph lookup is passed on.** `Checker.check` now hands the cell's `entity_exists` (and `relation_count`,
   point 3) to the rules, and reads `ends[..].exists` when the tool states it. In the pilot CS3 was the most frequent
   rejection on Neo4j (16 of 21 in one pass) and never fired on RDF, where 56 to 83 writes per run were
   "not checkable" and stored. Expected effect: those turn into pass, a reference to nothing into CS3.
2. **TS-datatype: a whole number is a decimal**, and both are a quantity (`VALID_FORMS`). On the induced Neo4j cell
   `volume_litres: 120` was rejected and `120.0` accepted, 17 of 21 rejection reasons. And **text is now parsed against
   the declared datatype** with the format table: in the property graph every date arrives as text, so `date`,
   `datetime`, `boolean`, `integer`, `decimal` and `quantity` are tested on text too (ISO 8601, point or comma
   decimals, yes/no words, number with unit; Thesis B's provisional formats until Thesis A's format and unit table).
   `string` accepts text only.
3. **TS-cardinality ("at most one") is implemented.** Two such relationships in one write are rejected from the write
   alone; what the start node already has needs the tool's answer `lookup.relation_count(key, type)` (section 3),
   asked of Thesis A on 25 Nov. Until it exists the rule is `not_checkable` beyond the write.
4. **Constraints on the properties of a relationship** (`required`, `datatypes`, `values` on the pattern) are enforced
   with the same code as on a node type; `properties` may be names or `{"name": ...}` objects (the loader no longer
   fails on the latter).
5. **TS-valuelist:** a property with several values is checked value by value (every value must be in the list; 0.2.0
   raised `TypeError`), and a typed RDF number in its written form (`"2"`) matches the number in the list (`2`).

And Thesis A's FYI: the **declared identifier** of an induced type is now required itself (TS-required) and is preferred
as the identifying value; in 0.2.0 any unique or naming property served.

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
python3 -m pytest experiments/thesis-b/validator/tests -q                 # 39 tests here, 46 with Thesis A's schemas checked out
PYTHONPATH=experiments/thesis-b python3 -m pytest schemas/treatments/test_induced_with_validator.py -q   # Thesis A's reading (harness-db-tool)
python3 experiments/thesis-b/validator/validate_export.py EXPORT.jsonl --equivalence ... --formats ... --log RUN.jsonl --detail
```

Thesis A's `test_induced_with_validator.py` (23 Nov) holds ten tests of what was enforced and seven `test_gap_*` of what
was not. Under 0.3.0 the ten pass and six of the seven gap tests fail, which is their purpose ("a test there fails on
the day Thesis B closes the gap, and is then turned round"); the seventh (identifier) stays green because the missing
identifier there is also `required`.

## 2. Files

| File | What |
|---|---|
| `checker.py` | `Checker.check(changes, lookup)` and the factory `checker()` for Thesis A's `load_checker("validator.checker:checker")`; builds the `WriteContext` from everything the lookup offers |
| `items.py` | the item model of the write (entity item, relation item) and the adapters: `diff_from_changes` (Thesis A's tool, with `ends[..].exists`), `diff_from_ntriples` (RDF), `diff_from_neo4j` (driver objects); a property with several values is one attribute with a list value |
| `rules_write.py` | CS1, CS3, CS4 at the write; treatment scope `TS-type`, `TS-unique`, `TS-endpoint`, `TS-required`, `TS-datatype`, `TS-valuelist`, `TS-cardinality` under the realisation's enforcement; `property_rules` shared by node types and patterns; `VALID_FORMS`, `datatype_fault`, `same_value`, `outside_list` |
| `rules_common.py` | CS1 to CS7 on the export |
| `rules_treatment.py` | `Declaration`: loader for `declaration.json` (`enforcement` with the eight kinds, `identifying_value`, uniqueness, patterns; Rule 6: `identifier`, `required`, `datatypes`, `values` per type and per pattern, `at_most_one`) |
| `validator.py` | `Validator` with `check_write` and `check_export`, `Result` (`to_log()`, `message()`, `engine_result()`, `element_ids`) |
| `vocabulary.py` | `EquivalenceTable`, `FormatTable` (Thesis A's format and unit table; provisional formats until it exists) |
| `changes.schema.json` | Thesis B's reading of Thesis A's `changes` (checker.py docstring) as JSON Schema, for tests |
| `canonical.schema.json` | **draft** of one canonical assertion on the export; revised to BE-08 modalities; Thesis A confirms |
| `examples/` | `changes.example.json` (Thesis A's rejected-write test, by hand), `inserted.example.nt` (RDF 1.2 annotation form), a four-line export, placeholder equivalence table and unit list |
| `tests/` | the boundary graphs on the export, Thesis A's tool situations on the write, Rule 5, the five points of 0.3.0 on an induced declaration, the RDF adapter, Thesis A's real declarations when present |

## 3. Starting it inside Thesis A's tool, and what the validator asks the tool

`experiments/harness/dbtool/cells.example.json`: `"checker": "validator.checker:checker"`, and the tool started with
`PYTHONPATH=experiments/thesis-b` (the folder name has a hyphen, so it is not importable as a package path). The
checker finds `schemas/treatments/<realisation>/declaration.json` from the repository root; `KG_VALIDATOR_ROOT`
overrides the root, `KG_VALIDATOR_FORMATS` names Thesis A's format and unit table once it exists. Nothing else is
configured: the mode (reject, log_only), the message, "already present", CS0 and the record are the tool's.

The validator never queries a store. What it may ask is on the `lookup` object the tool passes, and `Checker.check`
passes on whatever is there (`WriteContext.from_lookup`, `getattr`; a missing method leaves its rule `not_checkable`):

| Call | Answers | Used by | State |
|---|---|---|---|
| `find(label, property, value) -> [keys]` | nodes in the graph before the statement with that label and value | TS-unique | both cells (15 Nov) |
| `entity_exists(key) -> 0 \| 1` | whether the key names a node of the graph (a class or any literal value), counting what the statement adds | CS3, an end outside the write | RDF cell (24 Nov); `ends[..].exists` in `changes` is read first |
| `relation_count(key, type) -> int` | how many relationships of `type` leave the node `key` in the graph **before** the statement (RDF: triples `<key> <ns#type> ?o` with a resource object) | TS-cardinality | **asked of Thesis A, 25 Nov**; same definition for both databases |

For `relation_count` the validator adds what the statement itself creates (a changed relationship is already in the
count) and rejects when the sum is above one.

What the checker returns is exactly Thesis A's four fields (plus `message`, which `normalise` drops). What the tool does
with the answer: reject mode rolls the transaction back and sends the agent `message(faults)`; log-only keeps the write
and records the faults.

## 4. Rules

**At the write** (on the items of `changes`; rule list v0.2 sections 3 to 5):

| Rule | Checks | Note |
|---|---|---|
| `CS0` | the statement cannot be run | reported by the tool, not by this code |
| `CS1` | empty type, attribute name or relation type; an entity without an identifying value; a relation with a missing end; an empty or white-space value (or an empty list); control characters | identifying value = the declared identifier (S2), else a declared unique property, else `uid`/`id`/`key`/`name`/...; in S1 only `Entity` needs one (`identifying_value`), an `Assertion` node is a reification |
| `CS3` | an end of a relation resolves to exactly one entity | property graph: the store resolved the ends before `changes` exists (`ends`); RDF: an end outside the write is decided by `ends[..].exists`, else `entity_exists`; without either `not_checkable` |
| `CS4` | a value given with a datatype is a valid form of it | driver-typed values are already valid; typed RDF literals and the explicit form `{"value", "datatype", "unit"}` are parsed, each value of a list; units `not_checkable` until Thesis A's table |
| `TS-type` | label, class or relationship type outside the list | rejected where `enforcement` says so (S2, S3, S4); S1 declares nothing |
| `TS-endpoint` | relationship whose type and end types match no declared pattern | rejected; `not_checkable` when an end has no label or class |
| `TS-unique` | a declared unique value already in the graph (`lookup.find`) or twice in one statement | rejected; a changed node keeping its own value is no violation |
| `TS-property` | a property the schema does not name, on a node or on a relationship | **accepted and counted** (Rule 5): an observation, never a fault |
| `TS-required` | a declared required property missing or blank; the declared identifier missing | S2 (`required_property_missing`); on node types and on patterns |
| `TS-datatype` | a typed value whose datatype is not a valid form of the declared one (`VALID_FORMS`: integer ⊂ decimal ⊂ quantity); text that does not parse as the declared datatype (format table) | S2 (`datatype_violation`); on node types and on patterns; `string` accepts text only |
| `TS-valuelist` | a value, or any of several values, outside the declared list; numbers compared as numbers whatever their written form | S2 (`value_outside_the_list`); on node types and on patterns |
| `TS-cardinality` | a second relationship of a type from one start node where the pattern says `at_most_one` | S2 (`cardinality_violation`); counts the write plus `relation_count`; `not_checkable` beyond the write without it |

Order: common-scope rules first; an item that fails one is not tested against the schema's rules (rule list, CS4).
Every fault of a readable statement comes back in one list. Nothing from the benchmark in `expected`: only what the
schema description or the unit already shows the agent. CS2, CS5, CS6 and CS7 are never tested at the write. An
enforcement of `accept_and_count` for any of the eight kinds turns that rule's faults into observations.

**On the export** (canonical assertions; rule list v0.2 section 4 "counts"):

| Rule | Counts | Needs from Thesis A |
|---|---|---|
| `CS1` | subject, predicate or object missing or empty | — |
| `CS2` | predicate or type resolves neither through the mapping nor the equivalence table | mappings, equivalence table (28 Dec); until then `not_checkable`, figure provisional |
| `CS3` | subject or entity-valued object without an identifying value anywhere in the export | — |
| `CS4` | object value that does not normalise to its datatype; unit not in the table | format and unit table |
| `CS5` | a present qualifier not in BE-08 form: scope DE / state code / 8-digit key / institution id; validity closed-open ISO interval, start before end; modality must / may / must not; condition or exception non-empty. A missing qualifier is no violation | — |
| `CS6` | the chain breaks: no element pointer, or an element the write log does not show as the product of an accepted write with a unit id (`write_log` from `element_ids`); without a log, `provenance.unit_id` stands in | `element_ids` in the log (in since 15 Nov) |
| `CS7` | equal subject, predicate, object and every qualifier after normalisation; first occurrence passes; provenance excluded | equivalence table for the final figure |

A rule that cannot be decided for want of a binding is `not_checkable`, listed in the report and never counted as
pass or reject.

## 5. Decisions of 0.3.0 that the declaration does not state (Thesis B, 25 Nov 2026)

1. **Numbers nest:** a value typed `integer` is a valid `decimal` and a valid `quantity`; a `decimal` is a valid
   `quantity`; nothing else nests (a `date` is no `datetime`). A `quantity` as text is a number with an optional unit.
2. **Text is parsed** against a declared non-string datatype with the format table; `string` admits text only, so a
   number stored where the schema says text is TS-datatype (as in 0.2.0).
3. **Several values:** every value of a multi-valued property must be in the list; the fault names the values outside
   it. A multi-valued property has a datatype only when every value shares one.
4. **Value equality:** exact, or equal as numbers (so `"2"` matches `2`), or the same boolean word; text is
   case-sensitive (`Blue` is not `blue`).
5. **At most one** counts the relationships of the type that leave the start node (Thesis A's induction binding: "a
   start node may have only one such relationship"): what the tool counts before the statement plus what the statement
   creates; every relation item of the group is reported.
6. **The identifier** is required even when the reply did not list it under `required`, and is the first choice for
   the identifying value. Whether it is also unique stays Thesis A's rule (`IDENTIFIER_IS_UNIQUE` in derive.py).

## 6. Where Thesis B's reading differs from Thesis A's stand-in checker (for the record)

Thesis A's `test_neo4j_cell.py` runs a stand-in with two fixed rules. Under the real rules three of its situations
come out differently, by Thesis A's own list:

1. `CREATE (:Forbidden {x: 1})` is `CS1` (no identifying value) before it is `TS-type`; the test expects `TS-type`.
2. `SET w:Checked` adds a label outside the list: `TS-type` under Rule 5; the test expects the write to be stored.
3. `DETACH DELETE` is reported under `changes.deleted` and no rule of v0.2 speaks about deletions; the validator lets
   it pass and the record keeps it (Thesis A: allowed, with `warnings.jsonl`, 16 Nov).

## 7. Not yet done, and what it needs

| Missing | Needs | Owner |
|---|---|---|
| `relation_count(key, type)` on both cells (section 3), then TS-cardinality beyond the write | one method per cell | Thesis A |
| Format and unit table (CS4 units; formats beyond ISO; TS-datatype on text uses the same table) | the table (F4, open since 23 Nov) | Thesis A |
| Mappings and equivalence table (CS2, final CS7) | 23 Nov, 28 Dec | Thesis A |
| Thesis A's test cases of rule list section 7 with expected verdicts | this suite and `test_induced_with_validator.py` are the reading until then | Thesis A |
| Canonical form of the export confirmed (`canonical.schema.json`) | one reading by Thesis A | Thesis A |
| Rule list v1.0 as a tag | the tag | Thesis A |
| The S2 declaration that counts | the induction under the hashed text | Thesis A |
