# Database tool of the experiment harness

The agent's one way to its graph store. The agent writes Cypher (property-graph cells) or SPARQL (RDF cells) itself. The store runs the statement, the tool gets from the store **only what the statement changed**, hands those changes to the validator, and keeps or undoes them. There is no parser for the query languages and no copy of the graph.

**Status: 15 November 2026. The property-graph side works and is tested against Neo4j 5.26. The RDF side is not built yet. Not wired into the agent framework.**

```bash
podman-compose -f deploy/harness/podman-compose.yml up -d            # the stores; Neo4j with the commit hook enabled
python -m pytest experiments/harness/dbtool/tests -q                 # 16 tests against that Neo4j
HARNESS_NEO4J_PASSWORD=harness_dev python experiments/harness/dbtool/serve.py experiments/harness/dbtool/cells.example.json
```

## What the agent sees

One tool per cell, in the cell's own language: `cypher(statement, id, passage)` or `sparql(statement, id, passage)`. Reading and writing go through the same tool.

- `statement`: one statement. It runs as one transaction: all of it is stored, or nothing.
- `id`: the agent's name for the fact it writes. The same id on a corrected statement marks it as a retry.
- `passage`: the passage of the unit the write rests on. Kept in the record, never a reason for a rejection.

| Answer `status` | Meaning |
|---|---|
| `result` | a statement that only reads: `rows`, `row_count`, `truncated` (at most 200 rows by default) |
| `stored` | the changes are in the graph: counts of what was `created`, `changed`, `deleted` |
| `rejected` | the validator found faults; `message` names all of them, one per line; nothing was stored |
| `already_present` | a writing statement that changed nothing, for example a `MERGE` that found everything |
| `error` | the store could not run the statement; `message` is the store's own text |
| `refused` | a command that changes the schema or the set-up of the store; not passed on |

## How a write is checked (Neo4j)

A commit hook in the store (an APOC trigger in the phase `before`) posts the elements a transaction created, changed and deleted to the tool, while the transaction is still open. The tool hands them to the checker. If the answer is "reject", the trigger raises and Neo4j undoes the transaction.

- The cost is proportional to the change, not to the graph.
- A statement that only reads never reaches the checker.
- The store checks every write, whichever way it arrives. A write that did not come through the tool is undone and recorded as `unattributed_write`.
- If the tool is down or the checker fails, nothing is stored.
- **The hook itself is watched.** The store reads its hooks once a second, and right after a hook was removed and installed again it can be without one for a moment. On 15 November one write got through unchecked that way. Since then: the tool counts as ready only after the hook has answered without a gap for three such intervals (`GET /health` says `starting` until then); every unit opens only after the hook was seen working; and when the store's own counters say a statement changed something but the hook was not called, the tool records `hook_missing`, tells the agent that the harness has a fault, and runs nothing more in that unit. A unit with a `hook_missing` record is invalid and has to be repeated from the state before it.

Needs `apoc.trigger.enabled=true` and, because the store calls the tool, a store that can reach the tool's address (`deploy/harness/` runs Neo4j on the host network, bound to localhost).

## The handover point for the validator

A checker is an object with one method, `check(changes, lookup) -> list of faults`; each fault has `rule_id`, `element`, `expected`, `found`. [`checker.py`](checker.py) describes `changes`: the nodes and relationships the statement created or changed, in their state after the statement, with every node a relationship touches under `ends`, and the ids of what it deleted. `lookup.find(label, property, value)` returns what was in the graph before the statement.

The tool contains no rule of its own. `"checker": "none"` in the configuration lets everything pass; `"package.module:factory"` loads a checker. In mode `log_only` faults are recorded and the write is kept.

## The record

Every call is appended to `<log_dir>/<cell>.jsonl`, in order. A write attempt is an `insert_attempt` with the fields of the shared minimum log that the tool can know: `ts`, `run_id`, `unit_id`, `source_id`, `stratum` (from the open unit), `item_id`, `retry_index`, `tool_name`, `payload_sha256`, `evidence`, `validator_mode`, `validator_result`, `rejections`, `engine_result`, `engine_error`, `element_ids`, `accepted`, `latency_ms`; plus the full `statement` and the `changes`. `attempt_id`, `call_id` and `step_index` belong to the model call and are joined from the agent framework's records. Other record types: `graph_read`, `statement_error`, `refused`, `unattributed_write`, `hook_missing`, `unit_open`, `unit_close`.

The harness opens a unit before each job (`POST /cells/<cell>/unit`) and closes it afterwards; without an open unit the tool runs nothing.

## Open

1. **RDF side.** Fuseki has no hook before the commit. Its built-in change log (`patch:LoggedDataset`) gives the triples a statement added and removed; a rejection then takes them out again, and the tool has to keep its own list of what the graph holds, because the log also names a triple that was already there. Tested as a mechanism on 15 November, not built.
2. **Agent framework.** The tool speaks plain HTTP. It still has to be offered to the agent as a tool.
3. **`engine_result` of a rejected write.** The shared log says `not_sent`. Here the store ran the statement and undid it; the record uses `not_sent` in the sense "not kept".
4. **`already_present` for a statement.** Used when a writing statement changed nothing, also when it created something and removed it again. A statement that creates a second copy of something is a change and goes to the checker.
5. **Uniqueness.** When the constraints of a schema file are also created in Neo4j, the store itself refuses a duplicate and the agent sees the store's message, not the validator's.
6. **Accounts.** The refusal of schema and set-up commands is a check on the text. In the runs the account of a cell must not have the right to run them.
7. **One database per cell.** The hook is installed per database; tried on one database.
