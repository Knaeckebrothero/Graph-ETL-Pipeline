# Common-scope rule list (version 0.2 of 15 November 2026)

> **Version 0.2 of 15 November 2026. Draft, not frozen.** The seven rules come from the decision of 25 October 2026 (BU-15) and from card P.3 of `eval2-package-v0.1.md`. Draft 0.1 of 14 November left six points open; Thesis A settled them on 15 November (section 8). The list is part of Thesis A's technique and is frozen with version 1.0 on 23 November 2026 (F4); the validator that applies it is built by Thesis B (agreement, section 3.2). Still to come: the test cases of section 7 and the format and unit table.

## 1. What this list is

Two kinds of rules reach the validator. They must not be mixed, because they are reported differently.

| | Common scope (this note) | Treatment scope (not this note) |
|---|---|---|
| What | Seven rules that hold whatever the schema | The constraints one schema variant declares for itself |
| Same for all eight realisations | Yes, by construction | No, each has its own; raw triples (S1) has none |
| Comes from | This list | The schema file of each realisation |
| Reported as | One comparable rate per realisation (card P.3) | A table per realisation, never compared as a score (card P.4) |

A rule that some realisation cannot meet for structural reasons is removed from this list, not softened (decision of 25 October 2026). Section 6 checks that on paper; the pilot checks it on real runs.

## 2. Where a rule is tested

The decision of 25 October defines the rules on the **export**: the stored graph, turned back into schema-neutral assertions after the run. The validator also sits in front of the database and **rejects writes**. The two are different moments with different knowledge, so each rule needs a statement for each.

| | At the write | On the export |
|---|---|---|
| When | Before the database, for every item the agent hands over | After the run, on the exported assertions |
| Sees | One item, the graph as it stands, the unit being processed | All exported assertions of the cell, the write log, the equivalence table |
| Effect | Reject mode: the item is not written, the agent gets a message and may correct it. Log-only mode: written anyway, violation logged | Nothing is changed; violations are counted |
| Feeds | Pre-commit friction (card P.9) | Common-scope conformance (card P.3) |
| May use the benchmark's vocabulary | **Never** | Yes |

The last row is the reason for the split. A rejection message is read by the agent. If a write were rejected because its predicate is unknown to the equivalence table, the message would hand the benchmark's vocabulary to the agent, which the decision on identifiers forbids (BE-10: agents receive no identifier list beyond what their own schema declares).

The export check cannot run inside the harness at the end of a run: it needs the exporter and the equivalence table, and the table is frozen only on 28 December. It runs afterwards, on the exported file, with the same rule code. Until then every conformance figure is provisional and says which state of the table it used.

## 3. What the validator looks at

**At the write, an item.** Stated so that it holds in both arms and whichever way the write tool is built (an open design choice: the model hands over items, or the model writes a query and a parser turns it into items).

- An **entity item**: its types (labels in the property graph, classes in RDF; may be none), an identifying value (key or name), and attributes (name, value, and a datatype where one is given).
- A **relation item**: a type (relationship type or predicate), two ends that each refer to an entity, and attributes of its own where the realisation allows them.
- With every item the harness, not the model, adds the unit being processed.

**On the export, an assertion:** subject, predicate, object, the qualifiers that are present (scope, validity time, condition or exception, modality, priority), and pointers to the graph elements it was built from (BE-04, BU-07). The export keeps **every occurrence**; it does not merge equal assertions, otherwise rule 7 could never fire.

## 4. The seven rules

Ids `CS1` to `CS7` (CS for common scope; plain `C` would collide with the coverage cards C.1 to C.6).

### CS1 Well formed

*In words:* every stored fact has all three parts, and none of them is empty.

- **At the write, rejects:** (a) an empty type, attribute name or relation type; (b) an entity without an identifying value; (c) a relation with a missing end; (d) an attribute whose value is empty or only white space; (e) text that is not valid Unicode or holds control characters other than tab and line break.
- **On the export, counts:** an assertion whose subject, predicate or object is missing or empty after spelling normalisation.
- **Needs:** nothing.

### CS2 Vocabulary resolves

*In words:* the predicate of a stored fact, and the type of its subject and object where they have one, can be traced to a canonical predicate or concept.

- **At the write:** never tested (section 2).
- **On the export, counts:** an assertion whose predicate or type resolves neither through the written mapping of its realisation, nor through the equivalence table (exact key, lemma, spelling and unit normalisation), nor through an accepted human adjudication. "Mappable" in the decision means the last of these.
- **Needs:** the eight mappings (23 November; for the induced schema after induction) and the equivalence table (28 December).
- **Not the same as** the vocabulary-coherence figure "share of unmapped predicates and types" (card P.5): that one counts distinct names, this one counts stored facts. A realisation with one unmapped predicate used a thousand times is bad here and almost clean there.

### CS3 No dangling reference

*In words:* a fact never points at something that is not there.

- **At the write, rejects:** an end of a relation that does not resolve to exactly one entity, either already in the graph or earlier in the same call. Found none and found several are both rejections. The same order holds in both arms: the entity first, then the relation. The property graph forces that order anyway; RDF does not, so the validator has to.
- **On the export, counts:** an assertion whose subject or entity-valued object has no identifying value anywhere in the export (an empty node; a bare resource without name, key or type).
- **Needs:** read access to the graph at the write.

### CS4 Datatypes and units parse

*In words:* a value that claims to be a date, a number or a quantity really is one.

- **At the write, rejects:** a value given with a datatype (date, date and time, whole number, decimal number, yes or no, quantity with unit) that is not a valid form of that datatype under the format and unit table. A value without a datatype is text and always passes. It does not matter who asked for the datatype, the schema or the agent. Reason for testing it here and not leaving it to the database: Neo4j refuses a malformed date, Fuseki stores it; the validator makes both arms equally strict (EX-02).
- **On the export, counts:** an object value that does not normalise to the value type its canonical predicate expects (date, amount, quantity with unit), under the same table.
- **Needs:** the format and unit table, one file read by the validator and by the matcher (tier 3 in BE-10). It has to exist by 23 November; today it does not.
- **Boundary (clarified 15 November):** key-like values (waste code, municipality key, CAS and EAN numbers) are not tested here. They belong to the coherence figure "share of object values not normalisable to an anchor", so that nothing is counted twice.
- **Against the schema's own datatype constraints:** if the schema prescribes a date and the agent hands over text, that is the schema's rule (treatment scope). If the agent hands over something labelled a date that is no date, it is this rule. Common-scope rules are tested first; an element that fails one is not tested against the schema's rules as well.

### CS5 Qualifiers well formed

*In words:* where a fact carries a scope, a validity period, a condition or a modality, it is in the agreed form.

- **At the write:** nothing of its own. The validator cannot know which attribute is a qualifier without the mapping; qualifier values are attribute values and fall under CS1 and CS4.
- **On the export, counts:** a qualifier that is present and not in the form the decision on the record content fixes (BE-08): scope is DE, a federal-state code, an 8-digit municipality key as text, or an institution id; validity time is a closed-open ISO 8601 interval with the start before the end, open end allowed; modality is one of must, may, must not; a condition or exception is non-empty text.
- **A missing qualifier is no violation.** Whether qualifiers are there is measured by qualifier completeness (card C.5), not here.
- **Needs:** the encodings of BE-08; the equivalence table for modality words.

### CS6 Provenance pointer present

*In words:* for every stored fact it is known which source unit the agent was reading when it wrote it.

- **On the export, counts:** an assertion for which the chain breaks: no pointer to a graph element, or an element that the write log does not show as the product of an accepted write with a unit id. Such an assertion is "unanchored" and counts as unsupported in the supportedness coding (BU-16).
- **At the write (decided 15 November, option b):** the agent names a supporting passage of the unit with every write. The passage is kept in the log and never written into the graph. Nothing is rejected because of it: a missing or empty passage is logged as such. Whether the passage occurs in the unit is checked after the run and reported beside supportedness.
- **Independent of the schema:** the pointer comes from the harness log, never from provenance nodes a schema may have. Otherwise raw triples could not meet the rule and it would have to be removed.
- **Needs:** per accepted write, the log must say **what was written**: the element ids in the property graph; in RDF, where a triple has no id, the triples themselves or their hashes. The shared log schema did not have this field (Thesis B's trace does, `engine.assertion_ids`); it is in since 15 November as `element_ids` (pull request 3, commit a085204), together with `evidence` for the passage. Without it this rule could not be tested, the supportedness sample cannot be traced, and the conformance rate cannot be broken down by stratum of the source unit as card P.3 says.
- **Several units, one fact:** the stratum is that of the first accepted write; an unanchored assertion gets a row of its own.

### CS7 No duplicate

*In words:* the same fact is stored once.

- **On the export, counts:** assertions equal in subject, predicate, object and every qualifier after the matcher's normalisation (keys, lemmas, spelling and units, accepted adjudications). The first occurrence passes; every further one is a violation. Provenance is not part of the comparison: the same fact from two units is one fact. Order is by element id, so a second export gives the same count.
- **At the write (decided 15 November):** when an item repeats exactly what the graph holds (same type, same identifying value, same attributes; or the same relation between the same two entities), the write tool does not create a second copy and does not treat it as a fault. Nothing is hidden: the tool answers "already present" with the id of what is there, so the agent can go on and attach relations to it, and the log keeps the attempt with its unit and the result `already_present`. It costs none of the two corrections and is no rejection. Something with the same name and other attributes is not a repeat; where the schema declares that name as unique, that is a rejection under the schema's own rule (`TS-unique`). Repeats are printed as a count of their own and left out of the friction figures (card P.9).
- **Needs:** the equivalence table for the final figure.

## 5. Messages and ids

Every rejection carries the four fields the agreement fixes: rule id, element, expected, found.

| Rule | Example of element | Example of expected | Example of found |
|---|---|---|---|
| `CS1` | `attribute:name` | non-empty value | `""` |
| `CS3` | `to` | exactly one existing entity | `0 matches for name "Gelbe Tonne"` |
| `CS4` | `attribute:valid_from` | date | `"ab Januar"` |

- **Nothing from the benchmark in a message.** `expected` may only say what the schema description or the unit already shows the agent. This holds for the schema's own rules as well.
- **What one message holds** (decided 15 November): the checks run in steps, and the validator never repairs or guesses. Step 1: can the write be read at all. If not, the message says only that and nothing further is tested, because what the agent meant cannot be known. Step 2: for a write that can be read, every fault of the item as the agent wrote it is named in one message (two required properties missing: both are named). Reason: with two corrections allowed, an item with three faults would otherwise be given up without the agent ever having seen the third, and a schema with many rules would lose items to the message policy and not to the schema.
- **A write that cannot be read** gets the id `CS0`: it is a fault whatever the schema, it counts as friction, and it never appears on the export.
- **Ids for the schema's own rules,** so that the log can tell the two scopes apart: `TS-type`, `TS-required`, `TS-datatype`, `TS-unique`, `TS-cardinality`, `TS-endpoint`, `TS-valuelist`, the seven kinds of the decision of 25 October, with the schema element in `element`. Their exact form follows the schema file format, which is still to agree with Thesis B.
- **Friction by scope:** because every rejection carries its rule id, the friction rows of card P.9 can be printed split into common scope and treatment scope at no cost. That split shows how much of the friction a schema causes and how much any agent would have had.

## 6. Can every realisation meet every rule

On paper; the pilot has to confirm it (Eval 3).

| Rule | Raw triples (S1) | Induced (S2) | Flat (S3) | Curated (S4) | Same in both arms |
|---|---|---|---|---|---|
| CS1 | yes | yes | yes | yes | yes |
| CS2 | yes, but depends on the words the agent invents; expected to be worst here | yes for declared elements, through the mapping written after induction | yes | yes | yes |
| CS3 | yes | yes | yes | yes | yes, with the same order rule at the write |
| CS4 | yes; nothing is typed unless the agent types it | yes | yes | yes | yes, because the validator tests it and not the database |
| CS5 | yes; only qualifiers that are present are tested | yes | yes | yes | yes |
| CS6 | yes, because the pointer is the log | yes | yes | yes | yes, once the log names what was written in both arms |
| CS7 | yes | yes | yes | yes | yes, since a repeat is answered alike in both arms (decision 3) |

Two things can only be checked once the realisations exist: how the RDF version of raw triples names its entities (CS3 on the export needs an identifying value) and how each RDF realisation writes attributes of a relation.

## 7. Test cases that come with the list

So that "what the validator checks is Thesis A's" can be verified, the list ships with cases and the expected verdict for each; Thesis B's implementation has to reproduce them. From the expected properties of card P.3:

- empty export: not estimable;
- full gold, loaded and exported: rate 1;
- gold loaded twice: below 1, with CS7 as the only violated rule;
- a second export of the same graph: the same figure;
- one case per rule that violates exactly that rule, for the write and for the export.

Not built yet; they follow as files in this repository.

## 8. Settled on 15 November 2026

Draft 0.1 left six points open. Thesis A settled them as follows; the decision of 25 October (BU-15) is amended accordingly.

1. **Two test moments.** Each rule is stated for the write and for the export (section 2). Tested at the write: CS1, CS3, CS4. Tested on the export only: CS2, CS5, CS6, CS7. CS2 is never tested at the write, even where it could be, because the rejection message would show the agent the benchmark's vocabulary.
2. **Provenance.** The pointer of rule CS6 is the chain from the exported assertion over its graph elements and the accepted write in the log to the source unit. In addition the agent names a supporting passage of the unit with every write. The passage is kept in the log and never in the graph; nothing is rejected because of it; whether it occurs in the unit is checked after the run and reported beside supportedness.
3. **A repeated write.** An item that repeats exactly what the graph holds is not stored a second time and is no rejection. The write tool answers "already present" with the id of what is there, in both arms. It costs no correction. Repeats are counted on their own and left out of the friction figures. The same key with other content under a declared uniqueness constraint stays a rejection (`TS-unique`).
4. **What one message holds.** The validator never repairs or guesses. A write that cannot be read returns only that (`CS0`). A write that can be read returns every fault of the item as written, in one message. A message names only what the schema description or the unit already shows the agent.
5. **Two boundaries to vocabulary coherence** (card P.5), a clarification. CS4 tests values that state an amount or a time; values that name a thing (codes, keys, names) stay with coherence row (d). CS2 counts stored facts with an unmapped predicate or type; coherence row (c) counts distinct unmapped names. One fault can move both figures; they are not independent evidence.
6. **Log fields.** The write record of the shared minimum log gained `element_ids`, the engine result `already_present` and `evidence` (`experiments/log-schema/`).

## 9. What follows from it

- **For the validator (Thesis B):** two entry points with the same rule code, one for an item before the write and one for an exported file after the run; the rule id `CS0`; all faults of a readable item in one message; no rule that reads the equivalence table at the write.
- **For the write tools (Thesis A):** compare before writing and answer "already present"; record what was written; in RDF no blank nodes without fixed names, otherwise a written triple cannot be found again in the export.
- **For the construction template (freeze 16 November):** every write names a supporting passage; what a rejection looks like; what "already present" means.
- **Open between the theses:** Thesis B's confirmation of the three log fields before the log fields freeze.
- **Owed by Thesis A by 23 November:** the test cases of section 7 with their expected verdicts, and the format and unit table that rule CS4 and the matcher both read.
