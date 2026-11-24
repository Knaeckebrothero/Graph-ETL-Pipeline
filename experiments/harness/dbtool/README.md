# Database tool of the experiment harness

The agent's one way to its graph store. The agent writes Cypher (property-graph cells) or SPARQL (RDF cells) itself. The store runs the statement, the tool gets from the store **only what the statement changed**, hands those changes to the validator, and keeps or undoes them. There is no parser for the query languages and no copy of the graph.

**Status: 22 November 2026. Both sides work and are tested, also with Thesis B's validator as the checker: the property-graph side against Neo4j 5.26, the RDF side against Fuseki 6.2.0. No agent has written through the RDF side yet. Both sides are in the pilot bundle ([`deploy/pilot`](../../../deploy/pilot/README.md)), which offers the tool to an agent framework as `cypher` or `sparql` per cell.**

```bash
podman-compose -f deploy/harness/podman-compose.yml up -d            # the stores: Neo4j with the commit hook enabled, Fuseki with the logged dataset
python -m pytest experiments/harness/dbtool/tests -q                 # 55 tests: 25 against Neo4j, 30 against Fuseki
PYTHONPATH=experiments/thesis-b python -m pytest experiments/harness/dbtool/tests -q   # 17 more, with Thesis B's validator (7 and 10)
HARNESS_NEO4J_PASSWORD=harness_dev HARNESS_FUSEKI_PASSWORD=harness_dev \
    python experiments/harness/dbtool/serve.py experiments/harness/dbtool/cells.example.json
```

A store that is not running skips its tests. `HARNESS_FUSEKI_ENDPOINT` and `HARNESS_FUSEKI_LOG` point the RDF tests at another Fuseki and its change log.

## What the agent sees

One tool per cell, in the cell's own language: `cypher(statement, parameters, passage, locator, id)` or `sparql(statement, passage, locator, id)`. Reading and writing go through the same tool.

- `statement`: one statement. It runs as one transaction: all of it is stored, or nothing. In SPARQL that is one request, which may hold several operations separated by `;`.
- `parameters` (Cypher only): values for `$name` in the statement, so that a bulk insert is one statement with a list (`UNWIND $rows AS row ...`). SPARQL has no parameters; a call that sends some gets an `error` that says so.
- `passage`: the passage of the unit the write rests on, with an optional `locator` (section, row). Kept in the record, never a reason for a rejection.
- `id`: the tool names every write at its first attempt and returns the name with the answer. The agent sends it back only with a corrected statement, which marks it as a retry.

| Answer `status` | Meaning |
|---|---|
| `result` | a statement that only reads: `rows`, `row_count`, `truncated` (at most 200 rows by default) |
| `stored` | the changes are in the graph: counts of what was `created`, `changed`, `deleted` |
| `rejected` | the validator found faults; `message` names all of them, one per line; nothing was stored |
| `already_present` | a writing statement that changed nothing, for example a `MERGE` that found everything |
| `error` | the store could not run the statement; `message` is the store's own text |
| `refused` | a command that changes the schema or the set-up of the store, or in SPARQL leaves the one graph of the cell; not passed on |

On the RDF side `stored` also counts the change in triples (`triples: {added, removed}`), next to the same counts of nodes and relationships. A row of a reading SPARQL statement shows an IRI as `<iri>`, text as text, a whole number, a number and a truth value as such, any other typed value as `{"value", "datatype"}` and text with a language as `{"value", "lang"}`; `ASK` answers one row `{"ask": true}`, `CONSTRUCT` and `DESCRIBE` one row per triple.

## From inside the workspace: `run_query`

A script the agent writes reaches the tool through [`client/run_query.py`](client/run_query.py), a function and a shell command with the same parameters and the same answers as the tool call. Both end at the same address, so every statement goes through the same check and into the same record.

```python
from run_query import run_query
answer = run_query("UNWIND $rows AS row MERGE (:WasteItem {name: row.name})",
                   parameters={"rows": rows}, passage="table 2, rows 1 to 50")
```

```bash
run_query --passage '...' --param rows='[{"name": "Jar"}]' 'UNWIND $rows AS row MERGE (:WasteItem {name: row.name})'
```

The file needs only the standard library. In the workspace image it goes on the Python path and, as `run_query`, on the shell path. `RUN_QUERY_URL` is the address of the cell's tool; `RUN_QUERY_KEY` its key. A cell with a key (`tool_key_env` in the configuration) answers only calls that carry it, so a workspace reaches its own cell and no other. The agent does not get the store's own address or a database driver.

## How a write is checked (Neo4j)

A commit hook in the store (an APOC trigger in the phase `before`) posts the elements a transaction created, changed and deleted to the tool, while the transaction is still open. The tool hands them to the checker. If the answer is "reject", the trigger raises and Neo4j undoes the transaction.

- The cost is proportional to the change, not to the graph.
- A statement that only reads never reaches the checker.
- The store checks every write, whichever way it arrives. A write that did not come through the tool is undone and recorded as `unattributed_write`.
- If the tool is down or the checker fails, nothing is stored.
- **The hook itself is watched.** The store reads its hooks once a second, and right after a hook was removed and installed again it can be without one for a moment. On 15 November one write got through unchecked that way. Since then: the tool counts as ready only after the hook has answered without a gap for three such intervals (`GET /health` says `starting` until then); every unit opens only after the hook was seen working; and when the store's own counters say a statement changed something but the hook was not called, the tool records `hook_missing`, tells the agent that the harness has a fault, and runs nothing more in that unit. A unit with a `hook_missing` record is invalid and has to be repeated from the state before it.

Needs `apoc.trigger.enabled=true` and, because the store calls the tool, a store that can reach the tool's address (`deploy/harness/` runs Neo4j on the host network, bound to localhost).

## How a write is checked (Fuseki)

Fuseki has no hook before the commit. The cell's dataset is a logged dataset ([`deploy/harness/fuseki/cell.ttl`](../../../deploy/harness/fuseki/cell.ttl)): after every update the store appends one block to a change log, with the triples the update added and removed. The tool ([`fuseki_cell.py`](fuseki_cell.py)) runs the statement, reads its block, hands the changes to the checker, and on a rejection takes them out again through the store's patch endpoint. It then reads the block of that undo and compares: the graph has to be exactly as before, blank nodes included.

- The cost is proportional to the change, not to the graph. Measured on this machine (22 November 2026): about 145 to 170 ms for a stored statement, which is what the store alone needs for a commit (143 to 159 ms); about 270 to 305 ms for a rejected one, which is two commits; about 0.2 s for 500 nodes in one statement and 0.6 s for 2,500 nodes (10,000 triples). With 200,000 triples in the graph a small statement took 177 ms and opening a unit 149 ms.
- A statement that only reads never reaches the checker and leaves no block.
- **The check comes after the store has taken the write.** Between a write and its undo the graph holds the rejected triples. One statement runs at a time per cell, and the agent has neither the store's address nor its login.
- **The tool keeps its own list of the graph.** The log also names a triple that was already there and the removal of one that was not, so only the tool's list tells what really changed (nothing changed = `already_present`). The list is rebuilt from the log when the tool starts, and before every unit its size is compared with the store's own count.
- **Numbers are kept by value.** The store (TDB2) keeps whole numbers, decimals, floating-point numbers, truth values and moments in time by their value and gives them back in its own spelling: `2.50` comes back as `2.5`, `05` as `5`, `"1"^^xsd:boolean` as `true`. The tool's list, `changes`, the record and the hashes use one normal form per value (`normal_form`). A value that is not a valid form of its datatype stays as written, and so does text. **This is a property of the RDF arm: a number written as a number does not keep its spelling.**
- **A write that did not come through the tool** shows as a block no statement accounts for. The tool finds it before the next statement, before the next unit and at its start, takes it out and records `unattributed_write`. Unlike on Neo4j, such a write is in the graph until then.
- **A mark beside the records** (`<log_dir>/<cell>.settled`) says up to where the log has been dealt with. A block behind the mark at the start of the tool is a write nobody judged (the tool died between a write and its verdict, or something wrote while it was down) and is taken out. Without a mark the whole log is taken as kept, with a warning.
- **In place of the hook check:** before every unit the tool sends the store an update that changes nothing and has to find its empty block in the log. A tool that reads another file than the store writes, or a store that does not log, opens no unit. If a statement ever leaves no block, an undo does not go through, or the tool itself fails after the store has taken a statement, the cell shuts for that unit as on the Neo4j side (`log_missing`, `undo_failed`, `tool_fault`).
- Named graphs stay empty: the check on the text refuses `GRAPH`, `FROM`, `WITH` and `USING`, and a statement that writes into one after all is taken out again.

Needs the dataset's change log as a file the tool can read (a shared folder or volume), and the endpoints `sparql`, `update` and `patch` on the dataset. Dataset, log and mark belong together: to empty a cell, stop Fuseki and delete all three.

## The handover point for the validator

A checker is an object with one method, `check(changes, lookup) -> list of faults`; each fault has `rule_id`, `element`, `expected`, `found`. It may also keep a list `observations` of what it counted without rejecting, and offer `not_checkable()`; both go into the record of the write. [`checker.py`](checker.py) describes `changes`: the nodes and relationships the statement created or changed, in their state after the statement, with every node a relationship touches under `ends`, and the ids of what it deleted. `lookup.find(label, property, value)` returns what was in the graph before the statement.

**On the RDF side `changes` has the same shape.** A node is a subject with a class or a literal value, named by its IRI (a blank node by `_:` and the label the store keeps); a relationship is a triple whose object is a resource, named by its N-Triples line; the values hung on it with the annotation form (`s p o {| q v |}`) are its properties. A class or predicate inside the schema's namespace (`namespace` in `declaration.json`) is given by its local name, `rdfs:label` as `label`, and **any other IRI in full**, so that a name from another vocabulary is not taken for the schema's name. A typed value is `{"value", "datatype", "datatype_iri"}` with `datatype` in the validator's words (`integer`, `decimal`, `boolean`, `date`, `datetime`, `duration`); a datatype outside that list has only `datatype_iri` and is checked as text. `triples: {added, removed}` holds the exact change as canonical N-Triples lines. `ends` has `exists` for every end: whether it is a node of the graph. `lookup.entity_exists(key)` answers the same for a checker that asks.

The tool contains no rule of its own. `"checker": "none"` in the configuration lets everything pass; `"package.module:factory"` loads a checker. Thesis B's validator is loaded with `"checker": "validator.checker:checker"` and the tool started with `PYTHONPATH=experiments/thesis-b`; it reads the schema of the cell from `schemas/treatments/<realisation>/declaration.json` (`KG_VALIDATOR_ROOT` names the repository root when the two are not in one checkout). In mode `log_only` faults are recorded and the write is kept.

## The record

Every call is appended to `<log_dir>/<cell>.jsonl`, in order. A write attempt is an `insert_attempt` with the fields of the shared minimum log that the tool can know: `ts`, `run_id`, `unit_id`, `source_id`, `stratum` (from the open unit), `item_id`, `retry_index`, `tool_name`, `payload_sha256`, `evidence` (`quote`, `locator`), `validator_mode`, `validator_result`, `rejections`, `engine_result`, `engine_error`, `element_ids`, `accepted`, `latency_ms`; plus the full `statement`, the `changes`, and the checker's `observations` and `validator_not_checkable`. In RDF a triple has no id, so `element_ids` holds the SHA-256 of each written triple as one canonical N-Triples line (the lines themselves are under `changes.triples.added`); for `already_present` it names the triples that were already there. In the property graph an `already_present` write names the nodes and relationships the statement returned, if it returned any (see open point 4). `attempt_id`, `call_id` and `step_index` belong to the model call and are joined from the agent framework's records. Other record types: `graph_read`, `statement_error`, `refused`, `unattributed_write`, `hook_missing`, `warning`, `unit_open`, `unit_close`.

The harness opens a unit before each job (`POST /cells/<cell>/unit`) and closes it afterwards; without an open unit the tool runs nothing.

## Deleting, and the warnings file

An agent may delete and change what earlier units wrote (decided 16 November 2026). Without that an early mistake could not be corrected and every later unit would have to build around it. Nothing about a delete is rejected. Every delete is in the record with the ids of what went, so a fact that was stored and later removed can be told from one that was never stored.

What a person should look at is appended to `<log_dir>/warnings.jsonl`, one file for all cells, and printed on standard error. A warning never changes what the agent is told or what is stored.

| `kind` | When |
|---|---|
| `bulk_delete` | one statement deleted at least `warn_deleted` elements (default 25); the warning says how many of them earlier units wrote and how large the graph is afterwards. Neo4j counts nodes and relationships, **Fuseki counts triples**, so the same number is reached sooner there: a node with four values is five triples |
| `unattributed_write` | a write reached the store without passing the tool; it was undone (Fuseki: `undone` says whether that worked) |
| `hook_missing` | Neo4j: a write was committed without being checked; the unit has to be repeated |
| `log_missing`, `undo_failed`, `tool_fault` | Fuseki: the store took a statement and logged nothing, a rejected statement could not be taken out, or the tool itself failed after the store had taken a statement (which is then taken out); the unit has to be repeated |
| `list_rebuilt` | Fuseki: the tool's list had another size than the store and was read from the store again |
| `settle_mark_missing` | Fuseki: the tool started on a change log without its mark and took everything in it as kept |
| `checker_fault` | the checker raised an error; nothing was stored |

## Open

1. **RDF: a reference to nothing.** In the property graph a relationship cannot point to a node that is not there. In RDF it can: `d:rule :ROUTES_TO d:nowhere` is a valid triple. Validator 0.2.0 has no way to ask the tool about an end outside the write, reports CS3 as not checkable (`validator_not_checkable`) and the write is kept. The tool offers the answer (`ends[..].exists`, `lookup.entity_exists`); the validator has to pass it on. A test shows both outcomes.
2. **Agent framework.** The tool speaks plain HTTP. It still has to be offered to the agent as a tool.
3. **`engine_result` of a rejected write.** The shared log says `not_sent`. Here the store ran the statement and undid it; the record uses `not_sent` in the sense "not kept".
4. **`already_present` for a statement.** Used when a writing statement changed nothing, also when it created something and removed it again. A statement that creates a second copy of something is a change and goes to the checker. **Its `element_ids` in the property graph are complete only when the statement returns what it found.** The shared log asks that `element_ids` of such a write name the elements that were already there (rule 9). The RDF side can: the change log names every triple a statement wrote, also one that was there. Neo4j tells the tool only what a statement changed, so for a `MERGE` that found everything it tells nothing. Since 22 November the tool takes the element ids from the rows the statement returned (`MERGE (m:Material {uid: ...}) RETURN m`) and checks that they are in the graph; a statement that returns nothing leaves `element_ids` empty, which the log's checker reports. Closing the gap needs either a line in the prompt asking for a `RETURN` on a write, or the rule relaxed for the property graph. **Decided 23 November: the rule is relaxed for the property graph** (to agree with Thesis B, whose log schema it is); the prompt asks for no `RETURN`, and the tool keeps taking the ids from returned rows when there are any.
5. **Uniqueness.** When the constraints of a schema file are also created in Neo4j, the store itself refuses a duplicate and the agent sees the store's message, not the validator's.
6. **Accounts.** The refusal of schema and set-up commands is a check on the text. In the runs the account of a cell must not have the right to run them.
7. **One database per cell.** The hook is installed per database; tried on one database.
8. **A limit on deleting.** Today a large delete is only reported. Whether a statement that deletes more than a fixed number of elements should be rejected, and at which number, is to be decided from the pilot before the freeze on 23 November.
9. **"Stored, later removed".** The record allows this figure per cell; it is not yet part of the technique's tables.
10. **RDF: blank nodes.** The annotation form makes a blank node for every value on a relationship, and an agent may write `[]`. The hash of such a triple uses the label the store keeps for the node, which is stable in the store. An exporter gets the same hash only if it reads the labels the same way (`store_label` decodes the store's N-Triples output).
11. **RDF: the window before an undo.** A rejected statement is in the graph for the time of one commit, and a write past the tool until the tool looks next. Nothing in a run can read the graph in that window except through the tool, which runs one statement at a time; an export or snapshot has to be taken through the tool's lock or while no unit is open.
12. **RDF: the normal form of values** rests on what Fuseki 6.2.0 did in a test with 37 spellings. The store's limits for keeping a number by value (how large, how precise) are taken from that test and from the store's documentation, not from every case. If the tool's list and the store ever differ, the list is read from the store again and `list_rebuilt` is written; that warning means this table has a gap.
13. **RDF: the tool's list is in memory,** and starting the tool reads the whole change log. Measured with 200,000 triples in one cell: about 60 MB (300 bytes per triple), 1.2 s to start the tool on that log (24 MB). Twelve such cells in one tool are well under a gigabyte. The log only grows: it also holds every rejected statement and its undo.
14. **RDF: one Fuseki per cell.** The configuration serves one dataset, `cell`. Several logged datasets in one Fuseki would work the same way with one file each; not tried.
15. **The querying role** needs two tools that only read (to look, and `answer` to hand in), on both sides; not built.
