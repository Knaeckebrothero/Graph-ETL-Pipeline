# Schema treatments

The four schema variants of Thesis A, each for the property graph (`pg`, Neo4j) and for RDF (`rdf`, Fuseki): eight realisations.

**Status: version 0.2 of 15 November 2026. Not frozen.** The author decided the five rules below on 15 November. They can change until the freeze on 23 November; a change is an edit of the script and a new run. What is still open is listed at the end.

Every file in the four folders is written by [`derive.py`](derive.py) from the curated schema [`../v5.0/schema.cql`](../v5.0/schema.cql). Do not edit them by hand; change the script and run it again.

```bash
python schemas/treatments/derive.py           # write all files
python schemas/treatments/derive.py --check   # fail if a file on disk differs from the script
```

## The eight realisations

| Treatment | Property graph | RDF | State |
|---|---|---|---|
| **S1** raw triples | [`S1-raw-triples/pg/schema.cql`](S1-raw-triples/pg/schema.cql): a generic pattern, no statement | no schema file, by definition | rules decided |
| **S2** induced | [`S2-induced/pg/schema.cql`](S2-induced/pg/schema.cql): placeholder | [`S2-induced/rdf/schema.ttl`](S2-induced/rdf/schema.ttl): placeholder | waits for the induction by the main agent model |
| **S3** flat | [`S3-flat/pg/schema.cql`](S3-flat/pg/schema.cql) | [`S3-flat/rdf/schema.ttl`](S3-flat/rdf/schema.ttl) | rules decided; the RDF file is not yet reviewed |
| **S4** curated, fessi v5.0 | [`S4-curated/pg/schema.cql`](S4-curated/pg/schema.cql): byte-identical copy of `../v5.0/schema.cql` | [`S4-curated/rdf/schema.ttl`](S4-curated/rdf/schema.ttl) | the copy is final; the RDF file is not yet reviewed |

| | Node types | Relationship types | Relationship patterns | Uniqueness constraints | Indexes |
|---|---|---|---|---|---|
| S1 (pg) | 2 | 2 | 2 | 0 | 0 |
| S3 | 15 | 17 | 29 | 19 | 11 |
| S4 | 17 | 21 | 33 | 21 | 11 |

Each realisation folder also holds a `declaration.json`. It states the same content for programs: node types with their properties, relationship patterns with source and target, uniqueness constraints. It is generated from the same reading as the schema file, and the two arms of a treatment carry the same content. Three keys carry the rules below:

- `enforcement`: what the validator does with a write that leaves the schema (Rule 5);
- `identifying_value`: where an entity of S1 carries its name (Rule 4);
- `relationship_property_form`: how RDF data carries a property on a relationship (Rule 3).

The three `.cql` files were applied to Neo4j 5.26 Community on 14 November and created the numbers of constraints and indexes in the table. The two `.ttl` files load into Fuseki 6.2.0.

## Rule 1: what is read from the curated file

The curated file declares only constraints and indexes as statements. Its relationship patterns and most of its properties are comments. The script reads as structure:

- every `CREATE CONSTRAINT` and `CREATE INDEX` statement;
- the `// Properties: ...` lines of a node type;
- the pattern lines of the block "RELATIONSHIP PATTERNS (Reference)", such as `(Product)-[:HAS_COMPONENT {qty, weight_g}]->(PackagingComponent)`.

Descriptive text is not read. The declaration marks where each property comes from (`constraint`, `index` or `comment`). The layer of a node type comes from the list at the top of the file, or from the section it stands in when the list does not name it.

A value list in a `// Properties:` line, for example `level (1|2|3)`, is kept as a note. The curated file declares no constraint for it, so none is derived.

## Rule 2: the flat variant (S3)

S3 is S4 without the node types that reify a rule.

1. **Removed:** `DisposalRule` and `Condition`, with their constraints.
2. **Removed with them:** the relationship types that exist only to reach them or to leave `Condition`: `HAS_RULE`, `HAS_CONDITION`, `IF_TRUE`, `IF_FALSE`.
3. **Re-attached:** what hung on a rule now hangs on the node types that could own a rule in S4, `WasteItem` and `PackagingComponent`:
   - `(DisposalRule)-[:ROUTES_TO]->(WasteStream)` becomes `(WasteItem)-[:ROUTES_TO]->(WasteStream)` and `(PackagingComponent)-[:ROUTES_TO]->(WasteStream)`;
   - `HAS_INSTRUCTION` and `DERIVED_FROM` likewise.
4. **No layers:** the file has no layer sections, and node types and patterns are listed alphabetically.
5. **Unchanged:** every other node type, property, constraint, index, pattern and comment. `Instruction` and `Tip` stay: they hold content, not the logic of a rule.

Nothing is added. A condition has no place in S3: a rule such as "empty goes to recycling, otherwise to hazardous waste" cannot be stored as such. This is intended and known before the runs.

**What the pair S3 and S4 isolates:** rules stored as nodes of their own (S4) against rules stored as direct relationships (S3). It does not isolate layers. In the curated file the five layers are a comment and section headings; no label, property or relationship carries one, and the schema description an agent is shown contains no comments. Layers are a fact about how the curated schema was designed.

## Rule 3: from the property graph to RDF (S3 and S4)

| Property graph | RDF |
|---|---|
| Node label | `owl:Class`, same name |
| Property | `owl:DatatypeProperty`, same name, one per name for all classes; `rdfs:domain` lists the classes that carry it |
| Relationship type | `owl:ObjectProperty`, same name; `rdfs:domain` and `rdfs:range` are the union over its patterns |
| Relationship pattern | a `sh:property` entry in the shape of its source class, so the exact pairs are kept |
| Uniqueness constraint | `owl:hasKey` |
| Index | none |
| Property on a relationship | declared with `map:edgeProperty` on the object property |

- **Names** stay as in the Cypher file (`fessi:HAS_COMPONENT`, `fessi:is_hazardous`), so both arms use the same words.
- **A property on a relationship** is written in the data as an RDF 1.2 annotation on the statement, `s p o {| q v |} .`, and the statement itself is always asserted. Only `HAS_COMPONENT {qty, weight_g}` has such properties. Fuseki 6.2.0 stores this form correctly; the other form loses the relationship, see [`../../deploy/harness/README.md`](../../deploy/harness/README.md).
- **Namespaces:** `https://fessi.example/schema/s4#` and `.../s3#` for the terms, `https://fessi.example/mapping#` for `map:edgeProperty`. `fessi.example` is the final choice: a reserved name that never resolves.
- No datatype is declared, because the curated file declares none.

## Rule 4: raw triples (S1)

- **Property graph:** two node types. An `Entity` carries its `name`. An `Assertion` states one thing about the `Entity` it points to with `SUBJECT`. Its `predicate` is free text. The object is a literal in `value`, or another `Entity` reached with `OBJECT`.
- **RDF:** plain triples, no schema file. Every entity the agent creates gets an `rdfs:label`.

The name is not a constraint of this treatment. The shared rules of the validator ask every entity for an identifying value, whatever the schema, and without a place for it raw triples could not meet them.

## Rule 5: what is binding

The validator enforces a schema, in both arms alike. The stores enforce nothing of it themselves; the uniqueness constraints that the `.cql` files create in Neo4j are redundant.

| A write that ... | S3 and S4 | S1 |
|---|---|---|
| uses a label or class outside the list | rejected | nothing declared |
| states a relationship whose type and end types match no pattern | rejected | nothing declared |
| breaks a uniqueness constraint | rejected | nothing declared |
| carries a property the schema does not name | accepted and counted | nothing declared |

- The relationship patterns and property lists are comments in the curated file. They count as declared (Rule 1).
- Without the first two rows, S3 and S4 would be enforced alike and differ only in what the agent is shown: in S4 an agent could skip the rule nodes.
- Properties stay open because the property lists of the curated file are incomplete: `Instruction`, `DisposalRule`, `Container`, `Building` and `Room` name only `uid`.
- The same strengths hold for S2, for what the induced schema declares.
- The seven common-scope rules (`docs/technique/common-scope-rules.md`, pull request 3) apply to all eight realisations on top of this.

## Still open

1. **Write form.** Whether the agent hands structured items to the write tool or writes Cypher and SPARQL itself. In S1 this also decides whether an agent can leave the `Entity` and `Assertion` pattern.
2. **Review.** The author has not yet read the two `.ttl` files.
3. **Mappings.** The mapping from the benchmark's assertions into each realisation is due on 23 November. S3 and S4 name no place for scope, validity time, modality or priority; under Rule 5 these would be properties the schema does not name.
4. **Induced schemas.** The two S2 folders hold placeholders until the induction has run.
5. **Designer.** The characterisation matrix needs an entry on who wrote rules 2 to 5.
