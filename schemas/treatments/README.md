# Schema treatments

The four schema variants of Thesis A, each for the property graph (`pg`, Neo4j) and for RDF (`rdf`, Fuseki): eight realisations.

**Status: draft 0.1 of 14 November 2026. Not reviewed, not frozen.** The freeze is on 23 November. The three rules below are proposals and need the author's confirmation; the open points are listed at the end.

Every file in the four folders is written by [`derive.py`](derive.py) from the curated schema [`../v5.0/schema.cql`](../v5.0/schema.cql). Do not edit them by hand; change the script and run it again.

```bash
python schemas/treatments/derive.py           # write all files
python schemas/treatments/derive.py --check   # fail if a file on disk differs from the script
```

## The eight realisations

| Treatment | Property graph | RDF | State |
|---|---|---|---|
| **S1** raw triples | [`S1-raw-triples/pg/schema.cql`](S1-raw-triples/pg/schema.cql): a generic pattern, no statement | no schema file, by definition | draft |
| **S2** induced | [`S2-induced/pg/schema.cql`](S2-induced/pg/schema.cql): placeholder | [`S2-induced/rdf/schema.ttl`](S2-induced/rdf/schema.ttl): placeholder | waits for the induction by the main agent model |
| **S3** flat | [`S3-flat/pg/schema.cql`](S3-flat/pg/schema.cql) | [`S3-flat/rdf/schema.ttl`](S3-flat/rdf/schema.ttl) | draft |
| **S4** curated, fessi v5.0 | [`S4-curated/pg/schema.cql`](S4-curated/pg/schema.cql): byte-identical copy of `../v5.0/schema.cql` | [`S4-curated/rdf/schema.ttl`](S4-curated/rdf/schema.ttl) | the copy is final; the RDF file is a draft |

| | Node types | Relationship types | Relationship patterns | Uniqueness constraints | Indexes |
|---|---|---|---|---|---|
| S1 (pg) | 2 | 2 | 2 | 0 | 0 |
| S3 | 15 | 17 | 29 | 19 | 11 |
| S4 | 17 | 21 | 33 | 21 | 11 |

Each realisation folder also holds a `declaration.json`. It states the same content for programs: node types with their properties, relationship patterns with source and target, uniqueness constraints. It is generated from the same reading as the schema file, and the two arms of a treatment carry the same content.

The three `.cql` files were applied to Neo4j 5.26 Community on 14 November and created the numbers of constraints and indexes in the table.

## Rule 1: what is read from the curated file

The curated file declares only constraints and indexes as statements. Its relationship patterns and most of its properties are comments. The script reads as structure:

- every `CREATE CONSTRAINT` and `CREATE INDEX` statement;
- the `// Properties: ...` lines of a node type;
- the pattern lines of the block "RELATIONSHIP PATTERNS (Reference)", such as `(Product)-[:HAS_COMPONENT {qty, weight_g}]->(PackagingComponent)`.

Descriptive text is not read. The declaration marks where each property comes from (`constraint`, `index` or `comment`). The layer of a node type comes from the list at the top of the file, or from the section it stands in when the list does not name it.

## Rule 2: the flat variant (S3)

S3 is S4 without the node types that reify a rule.

1. **Removed:** `DisposalRule` and `Condition`, with their constraints.
2. **Removed with them:** the relationship types that exist only to reach them or to leave `Condition`: `HAS_RULE`, `HAS_CONDITION`, `IF_TRUE`, `IF_FALSE`.
3. **Re-attached:** what hung on a rule now hangs on the node types that could own a rule in S4, `WasteItem` and `PackagingComponent`:
   - `(DisposalRule)-[:ROUTES_TO]->(WasteStream)` becomes `(WasteItem)-[:ROUTES_TO]->(WasteStream)` and `(PackagingComponent)-[:ROUTES_TO]->(WasteStream)`;
   - `HAS_INSTRUCTION` and `DERIVED_FROM` likewise.
4. **No layers:** the file has no layer sections, and node types and patterns are listed alphabetically.
5. **Unchanged:** every other node type, property, constraint, index, pattern and comment.

Nothing is added. A condition has no place in S3.

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

Namespaces: `https://fessi.example/schema/s4#` and `.../s3#` for the terms, `https://fessi.example/mapping#` for `map:edgeProperty`. No datatype is declared, because the curated file declares none.

## Rule 4: raw triples (S1)

- **RDF:** plain triples, no schema file.
- **Property graph:** two node types. An `Assertion` states one thing about the `Entity` it points to with `SUBJECT`. Its `predicate` is free text. The object is a literal in `value`, or another `Entity` reached with `OBJECT`. Nothing is constrained or required.

## Open points for the author

1. **Flat rule.** Are `DisposalRule` and `Condition` the right node types to remove, with `Instruction` and `Tip` kept? Should a condition get a place in S3, for example as a property on `ROUTES_TO`, or none, as drafted?
2. **Layers.** In the curated file the five layers exist only as comments. The visible difference between S3 and S4 is therefore the rule structure alone.
3. **Comments as structure.** Properties named only in a `// Properties:` line are included. Value lists in such lines, for example `level (1|2|3)`, are kept as a note and not turned into a constraint.
4. **Names in RDF.** Kept as in the Cypher file (`fessi:HAS_COMPONENT`), not turned into the usual RDF style (`hasComponent`).
5. **Relationship properties in RDF.** Only `HAS_COMPONENT {qty, weight_g}` has them. The schema declares them; how the data carries them is described in [`../../deploy/harness/README.md`](../../deploy/harness/README.md).
6. **Namespaces.** `fessi.example` is a stand-in.
7. **Raw-triple pattern.** `Entity` and `Assertion` nodes, as drafted, or relationships between entities with the predicate as a property.
8. **Designer.** The characterisation matrix needs an entry on who wrote rules 2 to 4.
