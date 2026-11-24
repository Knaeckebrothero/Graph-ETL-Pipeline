# Schema treatments

The four schema variants of Thesis A, each for the property graph (`pg`, Neo4j) and for RDF (`rdf`, Fuseki): eight realisations.

**Status: version 0.2 of 15 November 2026. Not frozen.** The author decided rules 1 to 5 below on 15 November. They can change until the freeze; a change is an edit of the script and a new run. Rule 6 (how an induced schema is read) was written on 23 November and the author has not confirmed it yet. The sentence for a schema that declares nothing was added on 24 November after the pilot, and the author confirmed it. What is still open is listed at the end.

Every file in the four folders is written by [`derive.py`](derive.py) from the curated schema [`../v5.0/schema.cql`](../v5.0/schema.cql) and, for S2, from the reply of the induction (Rule 6). Do not edit them by hand; change the script and run it again.

```bash
python schemas/treatments/derive.py           # write all files
python schemas/treatments/derive.py --check   # fail if a file on disk differs from the script
```

## The eight realisations

| Treatment | Property graph | RDF | State |
|---|---|---|---|
| **S1** raw triples | [`S1-raw-triples/pg/schema.cql`](S1-raw-triples/pg/schema.cql): a generic pattern, no statement | no schema file, by definition | rules decided |
| **S2** induced | [`S2-induced/pg/schema.cql`](S2-induced/pg/schema.cql): 12 node types, 13 relationship patterns | [`S2-induced/rdf/schema.ttl`](S2-induced/rdf/schema.ttl): 7 classes, 5 object properties | **rehearsal reply of 24 November 2026 from the pilot, not the schema that counts.** It is replaced by the induction under the hashed text; made from the reply by Rule 6 |
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
- **Namespace for data:** `https://fessi.example/data/`, the same in every RDF realisation (S1 to S4), so that the IRI of an entity does not carry the schema variant. It is the key `data_namespace` of the RDF declarations, and the description tells the agent to create its entities there. **A default set on 22 November 2026; the author has still to confirm it.**
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
- The same strengths hold for S2, for what the induced schema declares. What only an induced schema can declare is binding too: Rule 6.
- The seven common-scope rules (`docs/technique/common-scope-rules.md`, pull request 3) apply to all eight realisations on top of this.

## Rule 6: the induced schema (S2)

The main agent model writes S2 once per arm, as one JSON document in the form the induction prompt states (first checkout, `experiments/prompts/bindings/induction.pg.txt` and `induction.rdf.txt`). Its reply is stored unchanged as `S2-induced/<arm>/induction.reply.json`. From that file `derive.py` writes the schema file and the `declaration.json` of the realisation, and `render.py` the description. Without a reply the realisation stays a placeholder and nothing else changes.

```bash
python schemas/treatments/induced.py REPLY --arm pg      # can this reply be used? every fault is listed
cp REPLY schemas/treatments/S2-induced/pg/induction.reply.json
python schemas/treatments/derive.py && python schemas/treatments/render.py
```

- **The reader is strict and repairs nothing** ([`induced.py`](induced.py)). A reply is used as it stands, or refused with the list of everything that is wrong with it. A refused reply generates nothing for its arm, and `derive.py` ends with an error after writing the other files. Two things around the document are tolerated: white space, and one code fence around the whole document (the declaration notes it).
- **What a fault is:** not one JSON document, or text before or after it; a key missing, unknown or stated twice; a value of the wrong type; a datatype outside the prompt's list; a name that is not a plain name (letters of any script, digits, underscore, no leading digit: it has to be written unquoted in Cypher and as the local part of an IRI); an identifier or a unique property the type does not declare; a relationship that names an undeclared type or no end; a duplicate type, property, pattern or allowed value; an empty list of allowed values; no type at all; in RDF, one name for two kinds of term.
- **Nothing is renamed, dropped or added, with one exception: the identifier of a type counts as unique.** The shared rules ask every entity for an identifying value, and the validator takes it from the unique properties of the entity's type. An identifier that the reply did not also list as unique would not be found, and every node of that type would be rejected. So the identifier is put into `unique`, and the declaration lists where this added something (`induction.identifier_added_to_unique`). One constant switches it off (`IDENTIFIER_IS_UNIQUE` in `induced.py`).
- **What the declaration carries beyond S3 and S4:** per node type `identifier`, `required` (names), `datatypes` and `values` (name to datatype, name to list); per pattern `at_most_one` and the same three keys for its properties (`properties` itself stays a list of names); the block `induction` (code fence, the identifier rule); and four more keys in `enforcement`.
- **RDF:** the namespace is `https://fessi.example/schema/s2#`. `schema.ttl` states datatypes, required properties, value lists and "at most one" as SHACL property shapes; a quantity is `map:quantity`, which no XSD type covers.

**What is binding.** The induction prompt tells the model that a constraint it declares is enforced. The `enforcement` block of an S2 declaration therefore says `reject` for the four kinds of Rule 5 as in S3 and S4 (an undeclared property is accepted and counted), and `reject` for a missing required property, a value of another datatype, a value outside its list, and a second relationship where the pattern says "at most one". The description tells the agent exactly that.

What Thesis B's validator does with such a declaration today (commit 5d2c858; [`test_induced_with_validator.py`](test_induced_with_validator.py) holds each line):

| Declared | Enforced today |
|---|---|
| Type or relation outside the schema | yes |
| Property outside the schema | accepted and counted, as declared |
| Required property of a type | yes |
| Unique property of a type, the identifier among them | yes |
| Identifier | an entity without any identifying value is rejected. The value may come from any unique property of the type or from a property called `uid`, `id`, `key`, `name`, `title`, `label` or `code`; that the declared identifier itself is there is only enforced if the reply also marks it required |
| Datatype of a property of a type | in part. A typed value of another datatype is rejected. A value that arrives as text passes any declared datatype, and in the property graph every date arrives as text. A whole number is rejected where `decimal` is declared, and any number where `quantity` is declared |
| List of allowed values of a property of a type | yes for text and for numbers in the property graph. In RDF a typed value never matches a list of numbers (the tool hands over `"2"`, the list holds `2`). A property with several values stops the check with an error |
| "At most one" | no |
| Required, datatype and allowed values of a property of a relationship | no; only an undeclared name is counted |

Until Thesis B closes these, the description promises the agent more than is enforced. To tell the agent the weaker truth instead, change the strength in `ENFORCEMENT_INDUCED` (`derive.py`) to `accept_and_count`; the sentence in the description follows.

## The description an agent is shown

[`render.py`](render.py) writes the text that fills the slot `schema_description` of the construction prompt, one file `description.construction.txt` per realisation, from that realisation's `declaration.json` and from nothing else. Do not edit the files by hand.

```bash
python schemas/treatments/render.py                 # write the descriptions
python schemas/treatments/render.py --check         # fail if a file on disk differs from the renderer
python schemas/treatments/render.py S3-flat/pg      # print one
python schemas/treatments/render.py --sizes         # characters per section
python -m pytest schemas/treatments -q                   # 55 tests; 71 with Thesis B's validator on the Python path
```

- **Sections, in fixed order:** identification (treatment, arm, version, "as declared"); types; properties; relationships with their end types; constraints by kind (labels and types, required properties, datatypes, uniqueness, cardinality, endpoints, closed value lists, other properties); counts. A section without content says "none declared".
- **What is closed and what is open** comes from the `enforcement` block: `reject` is a closed list, `accept_and_count` an open one, `nothing_declared` "none declared". The sentences are the same for every realisation of an arm and name nothing of the sources' domain; only the lists differ between schemas.
- **Not shown:** comments of a schema file, notes on a property (`1|2|3`), layers, indexes. A relationship pattern with two possible targets is one line, `(:A)-[:T]->(:B|C)`, so the number of lines is the number of patterns.
- **Raw triples (S1)** show the generic pattern of Rule 4 as its lists, the line that names the identifying value, and no constraint.
- **A schema that declares no type and no pattern at all** (today only raw triples on RDF) says so in its last constraint line: "nothing is declared, so any property is accepted, with a value or with another entity as its object. Give a property an IRI in the namespace for data." Added on 24 November 2026 after the pilot. With "none declared" alone the agent read the text both ways: in two passes it made up properties and stored about 1,500 triples with 490 relations; in a third it took the label for the only property allowed and stored 317 labels and nothing else, and every later unit followed what the first had left in the graph. The property graph's raw triples declare the generic pattern, so their text is unchanged.
- **A key the renderer does not know stops it,** so that nothing a declaration states is left out of the text without notice.
- **An induced schema (S2)** shows what only it can declare: after a property, in square brackets, `identifier`, the datatype, `required` and the allowed values (`one of "a" | "b"`); after a pattern, `at most one`. The lines for required properties, datatypes, cardinality and closed value lists then say what happens to a write that breaks them, from the same `enforcement` block. Two more clauses in the two list headings explain the brackets. A declaration that carries a constraint without a stated strength, or a constraint on a property it does not declare, stops the renderer. For a realisation without such content the text is unchanged, byte for byte.

State on 24 November 2026: written for all eight realisations. The two S2 descriptions come from the rehearsal replies of the pilot (see the table at the top); without a reply in its folder an S2 realisation is a placeholder and is refused, with the reason printed (Rule 6). An RDF description names two namespaces: the schema's, whose classes and properties it writes with the prefix `fessi:`, and the one for the entities an agent creates (`data_namespace`, see Rule 3). The names agree with what the database tool hands the validator: the tool strips the schema's namespace from a class or property, so `fessi:Material` in the text is `Material` in the declaration and in a rejection; `rdfs:label`, the identifying value of raw triples, is handed over as `label`; every other IRI stays whole, so a class from another vocabulary is outside the list. A test holds this. The query-language part of the decided description (dialect, engine, version) is not in this text: the prompt carries it as its own slot, the arm binding. The mode for querying (declared plus what the graph holds) is not built.

## Still open

1. **Write form.** Whether the agent hands structured items to the write tool or writes Cypher and SPARQL itself. In S1 this also decides whether an agent can leave the `Entity` and `Assertion` pattern.
2. **Review.** The author has not yet read the two `.ttl` files.
3. **Mappings.** The mapping from the benchmark's assertions into each realisation is due on 23 November. S3 and S4 name no place for scope, validity time, modality or priority; under Rule 5 these would be properties the schema does not name.
4. **Induced schemas.** Since 24 November the two S2 folders hold the rehearsal replies of the pilot (`deploy/pilot/run/induction/<arm>/attempt-1/reply.txt`, copied unchanged; both read as usable). They are there so that the two S2 cells can be tried, and they are replaced by the induction under the hashed text. Rule 6 was written and tested with invented replies; the author has to confirm the identifier rule, the plain-name rule and the strengths, and Thesis B's validator does not yet enforce everything an induced schema can declare (table in Rule 6).
5. **Designer.** The characterisation matrix needs an entry on who wrote rules 2 to 5.
