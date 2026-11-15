"""The property-graph side: Neo4j runs the statement, and a commit hook hands the
tool what the transaction changed before the store commits it.

The hook is an APOC trigger in the phase `before`. It posts the created, changed
and deleted elements to the tool; when the tool answers "reject", the trigger
raises and Neo4j undoes the whole transaction. The store therefore checks every
write, whichever way it arrives; a write that did not come through the tool
carries no token and is rejected.

Needs `apoc.trigger.enabled=true` (see deploy/harness/podman-compose.yml).
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

TOKEN_KEY = "dbtool"

# What the trigger sends. Trigger statements take no parameters of their own,
# so the address and the key are written into the text.
HOOK = """
WITH apoc.convert.toJson({
  meta: $metaData,
  createdNodes: [n IN $createdNodes | {id: elementId(n), labels: labels(n), props: properties(n)}],
  createdRelationships: [r IN $createdRelationships | {id: elementId(r), type: type(r),
      from: elementId(startNode(r)), to: elementId(endNode(r)), props: properties(r)}],
  ends: [r IN $createdRelationships | [
      {id: elementId(startNode(r)), labels: labels(startNode(r)), props: properties(startNode(r))},
      {id: elementId(endNode(r)), labels: labels(endNode(r)), props: properties(endNode(r))}]],
  deletedNodes: [n IN $deletedNodes | elementId(n)],
  deletedRelationships: [r IN $deletedRelationships | elementId(r)],
  assignedNodeProperties: [k IN keys($assignedNodeProperties) | {key: k,
      changes: [c IN $assignedNodeProperties[k] | {id: elementId(c.node), labels: labels(c.node),
                props: properties(c.node), old: c.old, new: c.new}]}],
  removedNodeProperties: [k IN keys($removedNodeProperties) | {key: k,
      changes: [c IN $removedNodeProperties[k] | {id: elementId(c.node), old: c.old}]}],
  assignedRelationshipProperties: [k IN keys($assignedRelationshipProperties) | {key: k,
      changes: [c IN $assignedRelationshipProperties[k] | {id: elementId(c.relationship),
                type: type(c.relationship), from: elementId(startNode(c.relationship)),
                to: elementId(endNode(c.relationship)), props: properties(c.relationship),
                fromNode: {id: elementId(startNode(c.relationship)), labels: labels(startNode(c.relationship)),
                           props: properties(startNode(c.relationship))},
                toNode: {id: elementId(endNode(c.relationship)), labels: labels(endNode(c.relationship)),
                         props: properties(endNode(c.relationship))},
                old: c.old, new: c.new}]}],
  removedRelationshipProperties: [k IN keys($removedRelationshipProperties) | {key: k,
      changes: [c IN $removedRelationshipProperties[k] | {id: elementId(c.relationship), old: c.old}]}],
  assignedLabels: [k IN keys($assignedLabels) | {label: k,
      nodes: [n IN $assignedLabels[k] | {id: elementId(n), labels: labels(n), props: properties(n)}]}],
  removedLabels: [k IN keys($removedLabels) | {label: k, nodes: [n IN $removedLabels[k] | elementId(n)]}]
}) AS payload
CALL apoc.load.jsonParams('__URL__', {method: 'POST', `Content-Type`: 'application/json', `X-Harness-Key`: '__KEY__'}, payload)
YIELD value
CALL apoc.util.validate(value.reject, '%s', [value.message])
RETURN 1
"""


def changes_from_hook(payload: dict) -> dict:
    """The trigger's lists, merged into one entry per node and per relationship."""
    nodes: dict[str, dict] = {}
    relationships: dict[str, dict] = {}
    ends: dict[str, dict] = {}

    def node(entry: dict, change: str) -> dict:
        item = nodes.setdefault(entry["id"], {"key": entry["id"], "change": change, "changed_properties": {},
                                              "added_labels": [], "removed_labels": []})
        item["labels"], item["properties"] = entry.get("labels", []), entry.get("props", {})
        return item

    def end(entry: dict) -> None:
        ends[entry["id"]] = {"labels": entry.get("labels", []), "properties": entry.get("props", {})}

    deleted_nodes = set(payload.get("deletedNodes", []))
    deleted_relationships = set(payload.get("deletedRelationships", []))
    # what a statement creates and removes again was never in the graph: it is neither created nor deleted
    passing_nodes = {entry["id"] for entry in payload.get("createdNodes", [])} & deleted_nodes
    passing_relationships = {entry["id"] for entry in payload.get("createdRelationships", [])} & deleted_relationships

    for entry in payload.get("createdNodes", []):
        if entry["id"] not in passing_nodes:
            node(entry, "created")
    for entry in payload.get("createdRelationships", []):
        if entry["id"] in passing_relationships:
            continue
        relationships[entry["id"]] = {"key": entry["id"], "type": entry["type"], "from": entry["from"],
                                      "to": entry["to"], "properties": entry.get("props", {}),
                                      "change": "created", "changed_properties": {}}
    for pair in payload.get("ends", []):
        for entry in pair:
            end(entry)
    for group in payload.get("assignedNodeProperties", []):
        for change in group["changes"]:
            if change["id"] in deleted_nodes:
                continue
            item = node(change, "changed") if change["id"] not in nodes else nodes[change["id"]]
            if item["change"] == "changed":
                item["changed_properties"][group["key"]] = {"old": change.get("old"), "new": change.get("new")}
    for group in payload.get("removedNodeProperties", []):
        for change in group["changes"]:
            if change["id"] in nodes and nodes[change["id"]]["change"] == "changed":
                nodes[change["id"]]["changed_properties"][group["key"]] = {"old": change.get("old"), "new": None}
    for group in payload.get("assignedLabels", []):
        for entry in group["nodes"]:
            if entry["id"] in deleted_nodes:
                continue
            item = node(entry, "changed") if entry["id"] not in nodes else nodes[entry["id"]]
            if item["change"] == "changed":
                item["added_labels"].append(group["label"])
    for group in payload.get("removedLabels", []):
        for node_id in group["nodes"]:
            if node_id in nodes and nodes[node_id]["change"] == "changed":
                nodes[node_id]["removed_labels"].append(group["label"])
    for group in payload.get("assignedRelationshipProperties", []):
        for change in group["changes"]:
            if change["id"] in deleted_relationships:
                continue
            item = relationships.get(change["id"])
            if item is None:
                item = relationships[change["id"]] = {
                    "key": change["id"], "type": change["type"], "from": change["from"], "to": change["to"],
                    "properties": change.get("props", {}), "change": "changed", "changed_properties": {}}
                end(change["fromNode"])
                end(change["toNode"])
            if item["change"] == "changed":
                item["changed_properties"][group["key"]] = {"old": change.get("old"), "new": change.get("new")}
    for group in payload.get("removedRelationshipProperties", []):
        for change in group["changes"]:
            if change["id"] in relationships and relationships[change["id"]]["change"] == "changed":
                relationships[change["id"]]["changed_properties"][group["key"]] = {"old": change.get("old"), "new": None}
    used = {r["from"] for r in relationships.values()} | {r["to"] for r in relationships.values()}
    return {
        "nodes": list(nodes.values()),
        "relationships": list(relationships.values()),
        "ends": {key: value for key, value in ends.items() if key in used},
        "deleted": {"nodes": sorted(deleted_nodes - passing_nodes),
                    "relationships": sorted(deleted_relationships - passing_relationships)},
    }


def is_empty(changes: dict) -> bool:
    return not (changes["nodes"] or changes["relationships"]
                or changes["deleted"]["nodes"] or changes["deleted"]["relationships"])


@dataclass
class Outcome:
    kind: str                                  # result | stored | unchanged | rejected | error | unchecked
    rows: list[dict] = field(default_factory=list)
    truncated: bool = False
    changes: dict | None = None
    rejections: list[dict] = field(default_factory=list)
    error_code: str | None = None
    error: str | None = None


@dataclass
class Pending:
    """One statement on its way through the store; the hook fills in the rest."""
    kind: str = "statement"                    # statement | admin | canary
    context: dict = field(default_factory=dict)
    changes: dict | None = None
    rejections: list[dict] | None = None
    rejected: bool = False


class Neo4jCell:
    arm = "pg"

    def __init__(self, cell_id: str, uri: str, user: str, password: str, database: str = "neo4j",
                 max_rows: int = 200, timeout_s: float = 60.0):
        self.cell_id, self.database = cell_id, database
        self.max_rows, self.timeout_s = max_rows, timeout_s
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.pending: dict[str, Pending] = {}
        self.hook_name = f"dbtool-{cell_id}"

    def close(self) -> None:
        self.driver.close()

    # ------------------------------------------------------------------ hook

    def install_hook(self, url: str, key: str = "", wait_s: float = 30.0, refresh_s: float = 1.0) -> None:
        """Install the commit hook and wait until the store applies it and keeps applying it.

        The store reads its hooks every `apoc.trigger.refresh` (refresh_s). Right after a hook of the same
        name was removed and installed again, the store can hold the old one for a moment and then none
        for a moment. So one positive answer is not enough: the hook has to answer without a gap for
        three refresh intervals.
        """
        statement = HOOK.replace("__URL__", url).replace("__KEY__", key)
        with self.driver.session(database="system") as session:
            session.run("CALL apoc.trigger.install($db, $name, $statement, {phase: 'before'})",
                        db=self.database, name=self.hook_name, statement=statement).consume()
        deadline = time.monotonic() + wait_s
        active_since = None
        while True:
            if self.hook_is_active():
                active_since = active_since or time.monotonic()
                if time.monotonic() - active_since >= 3 * refresh_s:
                    return
            else:
                active_since = None
            if time.monotonic() > deadline:
                raise RuntimeError(f"the commit hook of {self.cell_id} did not become active within {wait_s} s")
            time.sleep(refresh_s / 4)

    def remove_hook(self) -> None:
        with self.driver.session(database="system") as session:
            session.run("CALL apoc.trigger.drop($db, $name)", db=self.database, name=self.hook_name).consume()

    def hook_is_active(self) -> bool:
        """Try to write a marker node. An active hook rejects it; without a hook it is stored and removed again."""
        token = "canary:" + uuid.uuid4().hex
        self.pending[token] = Pending(kind="canary")
        try:
            with self.driver.session(database=self.database) as session:
                tx = session.begin_transaction(metadata={TOKEN_KEY: token})
                tx.run("CREATE (:DbToolCanary)").consume()
                tx.commit()
        except Neo4jError:
            return self.pending.pop(token).rejected
        self.pending.pop(token, None)
        self.admin("MATCH (n:DbToolCanary) DELETE n")
        return False

    def admin(self, statement: str, **parameters: Any) -> list[dict]:
        """A statement of the harness itself (reset, set-up). Passes the hook unchecked and is not a write attempt."""
        token = "admin:" + uuid.uuid4().hex
        self.pending[token] = Pending(kind="admin")
        try:
            with self.driver.session(database=self.database) as session:
                tx = session.begin_transaction(metadata={TOKEN_KEY: token})
                rows = [record.data() for record in tx.run(statement, **parameters)]
                tx.commit()
                return rows
        finally:
            self.pending.pop(token, None)

    # ------------------------------------------------------------------ the agent's statement

    def run(self, statement: str, context: dict) -> Outcome:
        token = uuid.uuid4().hex
        pending = self.pending[token] = Pending(context=context)
        rows: list[dict] = []
        truncated = False
        query_type = None
        updates = False
        try:
            with self.driver.session(database=self.database) as session:
                tx = session.begin_transaction(metadata={TOKEN_KEY: token}, timeout=self.timeout_s)
                try:
                    result = tx.run(statement)  # type: ignore[arg-type]
                    for record in result:
                        if len(rows) >= self.max_rows:
                            truncated = True
                            break
                        rows.append(record.data())
                    summary = result.consume()
                    query_type, updates = summary.query_type, summary.counters.contains_updates
                    tx.commit()                 # the hook runs here when the transaction changed something
                finally:
                    if not tx.closed():
                        tx.rollback()
        except Neo4jError as error:
            self.pending.pop(token, None)
            if pending.rejected:
                return Outcome("rejected", changes=pending.changes, rejections=pending.rejections or [])
            return Outcome("error", error_code=error.code, error=error.message)
        self.pending.pop(token, None)
        if pending.changes is not None:
            if is_empty(pending.changes):
                return Outcome("unchanged", rows=rows, truncated=truncated)
            return Outcome("stored", rows=rows, truncated=truncated, changes=pending.changes,
                           rejections=pending.rejections or [])
        if query_type == "r":
            return Outcome("result", rows=rows, truncated=truncated)
        # The hook was not called. Either the statement changed nothing in the end, or the hook is not
        # working and the store has just committed a write that nobody checked. The store's own counters
        # tell the two apart; when they report updates, only a working hook makes "nothing changed" true.
        if updates and not self.hook_is_active():
            return Outcome("unchecked", rows=rows, truncated=truncated)
        return Outcome("unchanged", rows=rows, truncated=truncated)

    # ------------------------------------------------------------------ lookup for the checker

    def find(self, label: str | None, property: str, value: Any) -> list[str]:
        """Element ids of the nodes that are in the graph (committed) with this label and value."""
        name = lambda text: "`" + text.replace("`", "``") + "`"       # the names go into the text, so an index is used
        pattern = f"(n:{name(label)})" if label else "(n)"
        query = f"MATCH {pattern} WHERE n.{name(property)} = $value RETURN elementId(n) AS id"
        with self.driver.session(database=self.database) as session:
            return [record["id"] for record in session.run(query, value=value)]  # type: ignore[arg-type]
