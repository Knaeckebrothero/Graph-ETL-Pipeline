# Prompt templates of the main agent

The three prompt templates of the main agent configuration (A0): construction, querying and schema induction. Thesis B's configurations copy them and change one documented thing each.

**Status: draft 0.8 of 24 November 2026. Not frozen; ready for the pilot.** The texts get their hashes at the second step of the freeze. After that a change of a word is a new template set with a new id.

```bash
python experiments/prompts/prompts.py slots       # the slots of every text
python experiments/prompts/prompts.py hashes      # one SHA-256 per text
python -m pytest experiments/prompts -q           # 18 tests
```

## What is here

| File | Content |
|---|---|
| `templates/<role>.system.txt` | The fixed part of a role: the same for every task of a cell |
| `templates/<role>.user.txt` | The task part: one source unit, one question, or the units for the induction |
| `bindings/<role>.<arm>.txt` | The part that differs between the property graph (`pg`) and RDF (`rdf`): data model, engine and version, language, tool. It fills the slot `{{arm_binding}}` |
| `prompts.py` | Lists slots, hashes, fills; builds the induction prompt from the frozen development units |
| `test_prompts.py` | Tests of the rules below |

Template set id: `A-frozen-v1`, the id Thesis B's configuration files already name. Keys for `run_start.prompt_sha256`: `construction.system`, `construction.user`, `binding.construction.pg` and so on, as `prompts.py hashes` prints them.

## Rules the texts obey

1. **Between the four schema variants only `{{schema_description}}` differs.** It is filled by the renderer and never by hand: declared-only for construction, declared plus what the graph holds for querying, never a stored value.
2. **Between the two arms only the binding differs,** and it is the same for the four variants of an arm.
3. **No word and no example of the domain** of the sources. A test checks the texts against a word list.
4. **One task per source unit or per question,** with a fresh context. The fixed part does not change inside a cell, so a provider can cache it.
5. **No budget in the text** (decided 23 November). The budgets per unit and per question, the timeout of a query and the length of the induction reply are settings of the agent configuration; the driver and the tool enforce them, and the agent meets one only when it stops the task. A configuration that changes a budget therefore runs the same texts. The two numbers the texts keep (rows shown, corrections per write) are slots, because the agent sees them in the tool's answers.
6. **Instructions in English; the sources stay in their language** and nothing is translated.
7. **A text defines the result and leaves the way to the agent** (decided 16 November). It says what the graph has to hold, or what the handed-in query has to return, and what the tools answer. It sets no order of work, no size of a statement and no number of query attempts. Two limits remain: the budget per task, and the number of corrections of one rejected write, which is part of the agreed agent configuration.
8. **The agent is told nothing about a unit beyond the unit itself.** A unit carries its own header lines (document, publisher, where it applies, part). Stratum, authority class and bucket are not shown, so a canary unit looks like any other.

## Slots

| Slot | In | Filled by |
|---|---|---|
| `arm_binding` | all three fixed parts | `bindings/<role>.<arm>.txt` |
| `schema_description` | construction, querying | the renderer (frozen on 23 November) |
| `max_self_corrections` | construction | agent configuration; 2 for A0 |
| `max_rows_shown` | construction, querying | the database tool's row limit (200 today) |
| `unit` | construction, task part | the unit file of the sample, unchanged |
| `question` | querying, task part | the question catalogue |
| `units` | induction, task part | every development unit, ordered by unit id, each under a numbered line |

## How a text is hashed

SHA-256 over the UTF-8 text with the slots unfilled, after this normalisation: line feeds as line ends, no white space at the end of a line, no empty lines at the end, one line feed after the last line. The hash of a filled prompt is a different thing; `prompts.py` prints it when it writes one.

## Filling

```bash
# construction or querying: a JSON object with one value per slot (not arm_binding)
python experiments/prompts/prompts.py fill construction --arm pg --values values.json

# induction: the 32 frozen development units, each checked against its hash
python experiments/prompts/prompts.py induction --arm pg \
    --units-csv experiments/sample/frozen/development-pilot.csv \
    --units-dir experiments/sample/out --out experiments/prompts/out
```

Filling refuses a slot without a value and a value without a slot. `out/` is not tracked: a filled prompt contains source text. The filled induction prompt is about 86,000 bytes of units plus 2,700 bytes of instructions.

## The write protocol in the construction text

It states what was settled for the write rules on 15 November (`docs/technique/common-scope-rules.md`, section 8) in the form the database tool has (`experiments/harness/dbtool/`, branch `harness-db-tool`, state of 16 November):

- The agent writes Cypher or SPARQL itself, one statement per call, stored completely or not at all. How many facts a statement holds is the agent's choice.
- Every writing statement carries the passage of the unit it rests on, and a locator if the agent can name one. The passage is recorded and is never a reason for a rejection; the text does not say so, it only asks for the passage.
- On the property graph a statement may take its values separately, in `parameters`; the binding says so.
- The tool names every write and returns the id with the answer. The agent sends the id back only with a correction.
- A rejection names every fault of the statement. A statement the store cannot read gets only the store's message.
- `already_present` is an answer, not a fault, and uses none of the corrections.
- At most `max_self_corrections` corrections under one id, then the agent leaves the item.
- The three rules that hold whatever the schema and are tested at the write (well formed, no dangling reference, datatypes parse) are named in plain words, so that an agent does not have to learn them again from rejections in every unit.
- The text says nothing about deleting. The tool allows an agent to change and delete what earlier units stored (decided 16 November).

## Looking and handing in, in the querying text

The agent may run as many queries as the budget of the question allows, to look at the graph. It hands in the query that answers the question, and may hand in again; the last one handed in is scored. The text sets no number of attempts. The graph description still shows no stored value; what the agent sees of the values, it sees through its own queries.

For the funnel of the technique the first attempt is the first query handed in and the final attempt the last one.

## Only the graph is kept

The graph is the only thing kept from one unit to the next (decided 17 November). The construction text says so in one sentence; it has no section on files, and the task part lists no folder. Nothing an agent leaves in its working folder reaches a later unit, and a querying task starts with nothing but the graph and its question.

## What was taken from Thesis B's construction draft, and what differs

Taken from `experiments/thesis-b/a0-prompt-templates-proposal.md` (pull request 4): the split into a fixed part and a task part; the limits as slots; values written as the source states them, converted only where the schema asks for a datatype; an entity that exists is not created again; the hash rule; the id `A-frozen-v1`.

| Thesis B's draft | Here | Reason |
|---|---|---|
| The text names no tool | The binding names the tool of the arm | Reading and writing go through one tool with a passage; the protocol has to be in the text because a tool description can be cut short |
| Every write names the unit id and a locator | The agent gives only the passage | The tool knows the open unit; the locator is found by searching the passage in the unit after the run |
| Slots for modality, authority class and jurisdiction of the unit | None | Rule 8 above |
| The agent ends with a short summary | The agent ends the task; the text says nothing on a summary | The framework's own tool for ending a job asks for a short summary; a sentence against it made the agent argue (pilot, 24 November) |
| `max_turns` and `max_tool_calls` in the text | No budget in the text | The budget the agreement names (section 4.5) stops a task; the agent is not told it (rule 5) |
| "Open node types may be instantiated ..." | Left out | What is open and what is closed is stated by the schema description, per variant |
| "Before you create an entity, check whether it exists" | "Every entity once: an entity the graph already holds is used again" | Rule 7: the result, not the step |

## What the texts ask of other parts

- **Renderer.** The schema description has to say which lists are closed (a write outside them is rejected) and which are open, and for RDF the namespace in which the agent creates IRIs for data.
- **Database tool.** For the querying role two tools that only read: `cypher(statement)` or `sparql(statement)` to look, and `answer(statement)` to hand in, offered by a connector named `graph` (see below); the second records the query as the answer of the question. The limit of corrections per id is stated in the text and not yet enforced by the tool. In RDF the tool refuses a statement that names a graph, and the text says so.
- **Driver.** It ends a task at its budget (tool calls and tokens per unit or per question; the framework itself has no token limit), joins the task part, and records the hashes of the texts and of the filled schema description at the start of a run. Nothing but the graph may pass from one unit to the next. In the agent framework a job gets a copy of its project's folder, so the project folder of a cell stays empty, and a change that a job makes to it is not applied; the driver checks this after every unit.
- **Validator.** An induced schema may declare required properties, datatypes, lists of allowed values and "at most one" (decided 16 November). These four kinds have to be enforced on what a statement changed.
- **Log schema.** The budget of a question (tool calls, tokens) is a new pair of numbers beside the budget of a unit.
- **Agent framework.** The texts are what this repository adds to the framework's own base prompt. Since draft 0.6 the texts say "end the task": how a job ends is the framework's, with its base prompt and a tool of its own (pilot of 24 November; before, the texts asked for the bare `DONE`, which clashed with it). The framework shows a tool of an external connector to the model as `mcp__<connector>__<tool>`, and the bindings name the tools that way, so the connector is named `graph` in every cell of both theses. The fixed part of a text goes into the job's `instructions` field (the test found that it does not reach the model as part of an inline expert).

## Decided on 16 November

1. The construction text names the kinds of circumstance to keep.
2. A querying agent may change its query after it ran. Beyond that: a text defines the result, and the way there is the agent's (rule 7).
3. *Superseded on 17 November:* files persist from unit to unit, in `cloud/`; the working folder itself is reset after every task.
4. An induced schema may be stricter than the curated one.
5. The induction text does not name the kinds of circumstance. The model designs the schema from the units alone. To state in the thesis: the construction text names the kinds of circumstance, the induction text does not.
6. *Superseded on 17 November:* `cloud/` is one folder per cell, empty at its start, and querying tasks have none.

## Decided on 17 November

1. No folder carries files from unit to unit: the graph is the only state kept between units. The construction text says so in one sentence, the slot `cloud_listing` is gone, and the task part holds only the unit. Reason: a notes folder would be a second channel beside the graph (under raw triples it could become a home-made schema), and one wrong note would steer every later unit of a run.

## Decided on 23 November

1. The descriptions of the tools belong to the harness set-up, not to these texts. They are the short texts in `experiments/harness/mcp/`; the run record pins them with the repository commit, as the framework's own base prompt and tools are pinned with its release.
2. The property-graph binding names the tool's field `parameters`.
3. The bindings name the tools as the model sees them: `mcp__graph__cypher`, `mcp__graph__sparql`, `mcp__graph__answer`.
4. "Numbers as written in the unit" stays. Fuseki keeps a typed number by its value (`2.50` comes back as `2.5`); the scoring compares numbers by value.
5. The text does not ask for a `RETURN` on a write. On the property graph a write that changed nothing may leave `element_ids` empty when the statement returned nothing (rule 9 of the shared log loosened for that case; to agree with Thesis B).
6. The texts state no budget, no query timeout and no length of the induction reply (rule 5). The querying text keeps that a question without a handed-in query counts as not answered, because that defines the result. The induction reply's length is set high enough that it does not cut a schema off; a reply that is cut off is a failed induction and is logged as a deviation.

## Open before the freeze

1. The numbers of the agent configuration, after the dry run (not in the texts): the budget per unit, the budget per question, the timeout of a query, the length of the induction reply.

