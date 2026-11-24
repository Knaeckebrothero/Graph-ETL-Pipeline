"""The RDF side: Fuseki runs the statement, and its change log tells the tool what
the statement changed.

Fuseki has no hook before the commit. The cell's dataset is a logged dataset
(`patch:LoggedDataset`, see deploy/harness/fuseki/cell.ttl): after every update the
store appends one block to a file, with the triples the update added (`A`) and
removed (`D`). The tool reads that block, hands the changes to the checker, and on
a rejection takes them out again through the store's patch endpoint.

Two properties of the log shape this file:

- It is written after the store has taken the write. So a rejection is an undo, and
  between the write and the undo the graph holds the rejected triples. One statement
  runs at a time per cell, and nobody else has the store's address.
- It also lists a triple that was already there, and the removal of one that was not.
  So the tool keeps its own list of what the graph holds (`Mirror`) and reduces a
  block to what really changed.
- It lists a value as the statement wrote it, while the store keeps numbers, truth
  values and times by their value: `2.50` is stored and given back as `2.5`, `05` as
  `5`. So the tool's list holds every value in one normal form (`canonical`), and that
  form is what `changes`, the record and the hashes of the triples carry.

The list is rebuilt from the log when the tool starts and compared with the store's
own count. A mark beside the records (`<cell>.settled`) says up to where the log has
been dealt with; a block after the mark is a write nobody checked (the tool died
between a write and its verdict, or something wrote while the tool was down) and is
taken out at the start.

Needs the log file readable by the tool (a shared volume or folder) and the endpoints
`sparql`, `update` and `patch` on the dataset.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import math
import os
import re
import socket
import struct
import sys
import time
import urllib.parse
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable

try:
    from . import guard
    from .neo4j_cell import Outcome
except ImportError:  # run as a script
    import guard
    from neo4j_cell import Outcome

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XSD = "http://www.w3.org/2001/XMLSchema#"
RDF_TYPE = f"<{RDF}type>"
RDF_REIFIES = f"<{RDF}reifies>"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

# The datatype names of `changes` (the same words the property-graph side gets from the driver's types).
# A datatype outside this list is handed over as text, with its IRI under `datatype_iri`.
XSD_NAMES = {"date": "date", "dateTime": "datetime", "dateTimeStamp": "datetime", "duration": "duration",
             "boolean": "boolean", "decimal": "decimal", "float": "decimal", "double": "decimal",
             **{name: "integer" for name in ("integer", "int", "long", "short", "byte", "nonNegativeInteger",
                                             "positiveInteger", "nonPositiveInteger", "negativeInteger",
                                             "unsignedInt", "unsignedLong", "unsignedShort", "unsignedByte")}}
INTEGER = re.compile(r"[+-]?\d+")
DECIMAL = re.compile(r"[+-]?\d*\.\d+")
DOUBLE = re.compile(r"[+-]?(\d+\.\d*|\.\d+|\d+)[eE][+-]?\d+")

REPOSITORY = Path(__file__).resolve().parents[3]
_NAMESPACES: dict[str, str | None] = {}


def namespace_of(realisation: str | None) -> str | None:
    """The namespace of a realisation's schema, from its declaration.json; None when it names none (raw triples)."""
    if not realisation:
        return None
    if realisation not in _NAMESPACES:
        # KG_VALIDATOR_ROOT names the folder that holds schemas/treatments when the tool does not run from a
        # checkout (an image); it is the variable Thesis B's validator reads the same declarations from.
        roots = [Path(root) for root in (os.environ.get("KG_VALIDATOR_ROOT"),) if root] + [REPOSITORY]
        _NAMESPACES[realisation] = None
        for root in roots:
            path = root / "schemas" / "treatments" / realisation / "declaration.json"
            try:
                _NAMESPACES[realisation] = json.loads(path.read_text(encoding="utf-8")).get("namespace")
                break
            except (OSError, ValueError):
                continue
    return _NAMESPACES[realisation]


# ---------------------------------------------------------------------- terms of the change log

def term_end(text: str, i: int) -> int:
    """Where the term that starts at `i` ends."""
    if text.startswith("<<(", i):
        j = i + 3
        for _ in range(3):
            while text[j].isspace():
                j += 1
            j = term_end(text, j)
        while text[j].isspace():
            j += 1
        if not text.startswith(")>>", j):
            raise ValueError(f"triple term not closed: {text[i:i + 80]!r}")
        return j + 3
    if text[i] == "<":
        return text.index(">", i) + 1
    if text[i] == '"':
        j = i + 1
        while text[j] != '"':
            j += 2 if text[j] == "\\" else 1
        j += 1
        if text.startswith("^^", j):
            return term_end(text, j + 2)
        if j < len(text) and text[j] == "@":
            while j < len(text) and not text[j].isspace():
                j += 1
        return j
    j = i
    while j < len(text) and not text[j].isspace():
        j += 1
    return j


def split_terms(text: str) -> list[str]:
    """The terms of one line of the log, as written there, without the final dot."""
    terms, i = [], 0
    while i < len(text):
        if text[i].isspace():
            i += 1
        elif text[i] == "." and not text[i + 1:].strip():
            break
        else:
            j = term_end(text, i)
            terms.append(text[i:j])
            i = j
    return terms


_ESCAPES = {"t": "\t", "b": "\b", "n": "\n", "r": "\r", "f": "\f", '"': '"', "'": "'", "\\": "\\"}
_UNESCAPE = re.compile(r"\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|.)", re.S)


def unescape(text: str) -> str:
    if "\\" not in text:
        return text
    return _UNESCAPE.sub(lambda m: chr(int(m.group(1)[1:], 16)) if m.group(1)[0] in "uU" and len(m.group(1)) > 1
                         else _ESCAPES.get(m.group(1), m.group(1)), text)


_CANONICAL = {"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t", "\b": "\\b", "\f": "\\f"}


def escape(text: str) -> str:
    """A string as it stands in a canonical N-Triples line."""
    return "".join(_CANONICAL.get(ch) or (f"\\u{ord(ch):04X}" if ord(ch) < 0x20 or ord(ch) == 0x7F else ch) for ch in text)


def decode(token: str) -> dict:
    """One term of the log: kind (iri, bnode, literal, triple), value, and for a literal its datatype and language."""
    if token.startswith("<<("):
        s, p, o = split_terms(token[3:-3])
        return {"kind": "triple", "s": s, "p": p, "o": o}
    if token.startswith("<_:"):
        return {"kind": "bnode", "value": "_:" + token[3:-1]}
    if token.startswith("_:"):
        return {"kind": "bnode", "value": store_label(token)}
    if token.startswith("<"):
        return {"kind": "iri", "value": token[1:-1]}
    if token.startswith('"'):
        end = 1
        while token[end] != '"':
            end += 2 if token[end] == "\\" else 1
        rest = token[end + 1:]
        literal = {"kind": "literal", "value": unescape(token[1:end]), "datatype": XSD + "string", "lang": None}
        if rest.startswith("^^<"):
            literal["datatype"] = rest[3:-1]
        elif rest.startswith("@"):
            literal["datatype"], literal["lang"] = RDF + "langString", rest[1:]
        return literal
    if token in ("true", "false"):
        return {"kind": "literal", "value": token, "datatype": XSD + "boolean", "lang": None}
    for pattern, datatype in ((INTEGER, "integer"), (DECIMAL, "decimal"), (DOUBLE, "double")):
        if pattern.fullmatch(token):
            return {"kind": "literal", "value": token, "datatype": XSD + datatype, "lang": None}
    # Nothing the store is known to write. Kept as it stands, so that it still has one identity in the tool's list.
    return {"kind": "literal", "value": token, "datatype": "urn:dbtool:unread-term", "lang": None}


def is_literal(term: str) -> bool:
    """For a canonical term."""
    return term.startswith('"')


INTEGERS = {XSD + name for name, kind in XSD_NAMES.items() if kind == "integer"}
_WHOLE = re.compile(r"\s*([+-]?\d+)\s*")
_POINT = re.compile(r"\s*([+-]?(?:\d+\.\d*|\.\d+|\d+))\s*")
_MOMENT = re.compile(r"\s*(-?\d{4,}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)?\s*")


def normal_form(lexical: str, datatype: str) -> str:
    """The written form of a value as the tool keeps it. The store (TDB2) keeps whole numbers, decimals,
    floating-point numbers, truth values and moments in time by value, inside the limits it can pack, and gives
    them back in its own spelling; two spellings of one such value are one triple there. Everything else, and
    every value that is not a valid form of its datatype, stays as written."""
    try:
        if datatype in INTEGERS:
            match = _WHOLE.fullmatch(lexical)
            if match and -2 ** 55 <= int(match.group(1)) < 2 ** 55:
                return str(int(match.group(1)))
        elif datatype == XSD + "decimal":
            match = _POINT.fullmatch(lexical)
            if match:
                number = Decimal(match.group(1)).normalize()
                sign, digits, exponent = number.as_tuple()
                if int("".join(map(str, digits)) or "0") < 2 ** 47 and -128 <= exponent <= 127:
                    text = format(number, "f")
                    return text if "." in text else text + ".0"
        elif datatype in (XSD + "double", XSD + "float"):
            number = float(lexical.strip())
            if math.isfinite(number) and lexical.strip().lower() not in ("inf", "+inf", "-inf", "infinity", "nan"):
                if datatype == XSD + "float":            # single precision: the shortest spelling of what is kept
                    packed = struct.pack("f", number)
                    number = next((float(f"{number:.{digits}g}") for digits in range(1, 10)
                                   if struct.pack("f", float(f"{number:.{digits}g}")) == packed), number)
                return repr(number)
        elif datatype == XSD + "boolean":
            return {"true": "true", "1": "true", "false": "false", "0": "false"}.get(lexical.strip(), lexical)
        elif datatype in (XSD + "dateTime", XSD + "dateTimeStamp"):
            match = _MOMENT.fullmatch(lexical)
            if match:
                fraction = (match.group(2) or "").rstrip("0")
                if len(fraction) <= 3:
                    return match.group(1) + (f".{fraction}" if fraction else "") + (match.group(3) or "")
    except (ValueError, OverflowError, InvalidOperation, struct.error):
        pass
    return lexical


def canonical(token: str) -> str:
    """A term of the log, or of the store's N-Triples, as a canonical N-Triples term: a value in its normal form
    with its datatype written out, a blank node as `_:` and the label the store keeps for it."""
    term = decode(token)
    if term["kind"] == "iri":
        return f"<{term['value']}>"
    if term["kind"] == "bnode":
        return term["value"]
    if term["kind"] == "triple":
        return f"<<( {canonical(term['s'])} {canonical(term['p'])} {canonical(term['o'])} )>>"
    if term["lang"]:
        return f'"{escape(term["value"])}"@{term["lang"]}'
    text = '"' + escape(normal_form(term["value"], term["datatype"])) + '"'
    return text if term["datatype"] == XSD + "string" else f"{text}^^<{term['datatype']}>"


nt = canonical


def nt_line(triple: tuple[str, str, str]) -> str:
    """A canonical triple as one N-Triples line."""
    return f"{triple[0]} {triple[1]} {triple[2]} ."


def store_label(label: str) -> str:
    """The blank node label of the store's N-Triples output (`_:B` and the label, encoded) as the store keeps it."""
    if not label.startswith("_:B"):
        return label
    return "_:" + re.sub(r"X(X|[0-9A-Fa-f]{2})", lambda m: "X" if m.group(1) == "X" else chr(int(m.group(1), 16)), label[3:])


def triple_id(line: str) -> str:
    """What the record names a written triple by: the SHA-256 of its canonical N-Triples line."""
    return hashlib.sha256(line.encode("utf-8")).hexdigest()


def key_of(term: str) -> str:
    """How `changes` names a node: its IRI, or `_:label` for a blank node."""
    return term[1:-1] if term.startswith("<") and not term.startswith("<<(") else term


def term_of(key: str) -> str:
    """The canonical term of a key."""
    return key if key.startswith(("_:", "<<(")) else f"<{key}>"


def value_of(token: str) -> Any:
    """A value as `changes` carries it: text as text; a typed value as {"value", "datatype", "datatype_iri"}
    in its written form, so that the checker can test it; text with a language as {"value", "lang"}."""
    term = decode(token)
    if term["kind"] != "literal":
        return token
    if term["lang"]:
        return {"value": term["value"], "lang": term["lang"]}
    if term["datatype"] == XSD + "string":
        return term["value"]
    typed = {"value": term["value"], "datatype_iri": term["datatype"]}
    name = XSD_NAMES.get(term["datatype"][len(XSD):]) if term["datatype"].startswith(XSD) else None
    if name:
        typed["datatype"] = name
    return typed


# ---------------------------------------------------------------------- blocks of the change log

class Block:
    """One transaction as the store logged it."""

    def __init__(self) -> None:
        self.id: str | None = None
        self.lines: list[tuple[str, list[str]]] = []     # (A | D, terms)
        self.committed = False
        self.end = 0                                     # byte offset behind the block


def read_blocks(path: Path, offset: int) -> tuple[list[Block], int, bool]:
    """The complete blocks behind `offset`: (blocks, offset behind the last complete one, whether a part follows)."""
    try:
        with path.open("rb") as handle:
            handle.seek(offset)
            data = handle.read()
    except FileNotFoundError:
        return [], offset, False
    blocks: list[Block] = []
    block, position, done = Block(), 0, 0
    while True:
        newline = data.find(b"\n", position)
        if newline < 0:
            break
        line = data[position:newline].decode("utf-8").strip()
        position = newline + 1
        if not line:
            continue
        head, _, rest = line.partition(" ")
        if head == "H":
            name, _, value = rest.partition(" ")
            if name == "id":
                block.id = value.rstrip(" .").strip("<>")
        elif head in ("A", "D"):
            block.lines.append((head, split_terms(rest)))
        elif head in ("TC", "TA"):
            block.committed, block.end = head == "TC", offset + position
            blocks.append(block)
            block, done = Block(), position
        # TX opens the block; PA and PD (prefixes) do not touch the triples
    return blocks, offset + done, bool(data[done:].strip())


# ---------------------------------------------------------------------- the tool's own list of the graph

class Mirror:
    """The triples of the default graph in their canonical form, by subject."""

    def __init__(self) -> None:
        self.by_subject: dict[str, set[tuple[str, str]]] = {}
        # (predicate, written form of a literal) -> the subject that has it, or a set of them when several do
        self.by_value: dict[tuple[str, str], Any] = {}
        self.reifiers: dict[str, set[str]] = {}                   # triple term -> the subjects that reify it
        self.count = 0

    def has(self, triple: tuple[str, str, str]) -> bool:
        return (triple[1], triple[2]) in self.by_subject.get(triple[0], ())

    def of(self, subject: str) -> set[tuple[str, str]]:
        return self.by_subject.get(subject, set())

    def with_value(self, predicate: str, lexical: str) -> list[str]:
        """The subjects that have this predicate with a literal of this written form."""
        held = self.by_value.get((predicate, lexical))
        return [] if held is None else sorted(held) if isinstance(held, set) else [held]

    def add(self, triple: tuple[str, str, str]) -> None:
        s, p, o = sys.intern(triple[0]), sys.intern(triple[1]), triple[2]      # names repeat; keep one copy of each
        o = o if is_literal(o) else sys.intern(o)
        held = self.by_subject.setdefault(s, set())
        if (p, o) in held:
            return
        held.add((p, o))
        self.count += 1
        if p == RDF_REIFIES:
            self.reifiers.setdefault(o, set()).add(s)
        elif is_literal(o):
            key = (p, decode(o)["value"])
            others = self.by_value.get(key)
            if others is None:
                self.by_value[key] = s
            elif isinstance(others, set):
                others.add(s)
            elif others != s:
                self.by_value[key] = {others, s}

    def remove(self, triple: tuple[str, str, str]) -> None:
        s, p, o = triple
        held = self.by_subject.get(s)
        if not held or (p, o) not in held:
            return
        held.discard((p, o))
        self.count -= 1
        if not held:
            del self.by_subject[s]
        if p == RDF_REIFIES:
            subjects = self.reifiers.get(o)
            if subjects:
                subjects.discard(s)
                if not subjects:
                    del self.reifiers[o]
        elif is_literal(o):
            lexical = decode(o)["value"]
            if any(q == p and is_literal(v) and decode(v)["value"] == lexical for q, v in self.of(s)):
                return                                # another literal of the same written form is still there
            others = self.by_value.get((p, lexical))
            if isinstance(others, set):
                others.discard(s)
                if len(others) == 1:
                    self.by_value[(p, lexical)] = next(iter(others))
            elif others == s:
                del self.by_value[(p, lexical)]

    def net(self, blocks: list[Block]) -> tuple[dict[tuple, list[str]], dict[tuple, list[str]], list[tuple]]:
        """What the committed blocks changed against this list: (added, removed, lines for a named graph).
        Added and removed map a canonical triple to its terms as the log wrote them, which is the form a patch
        that takes the change out again has to use."""
        now: dict[tuple, tuple[bool, list[str]]] = {}
        quads: list[tuple] = []
        for block in blocks:
            if not block.committed:
                continue
            for op, terms in block.lines:
                if len(terms) == 3:
                    now[tuple(canonical(term) for term in terms)] = (op == "A", terms)
                elif len(terms) == 4:
                    quads.append((op, *terms))
        added = {t: raw for t, (present, raw) in now.items() if present and not self.has(t)}
        removed = {t: raw for t, (present, raw) in now.items() if not present and self.has(t)}
        return added, removed, quads

    def apply(self, added: Any, removed: Any) -> None:
        for triple in removed:
            self.remove(triple)
        for triple in added:
            self.add(triple)


# ---------------------------------------------------------------------- the statement as text

QUERY_FORMS = ("SELECT", "ASK", "CONSTRUCT", "DESCRIBE")
keywords = guard.sparql_words


def form_of(statement: str) -> str:
    """SELECT, ASK, CONSTRUCT or DESCRIBE for a statement that reads; UPDATE for everything else."""
    for word in keywords(statement):
        if word.upper() in ("PREFIX", "BASE"):
            continue
        return word.upper() if word.upper() in QUERY_FORMS else "UPDATE"
    return "UPDATE"


# ---------------------------------------------------------------------- rows of a reading statement

_SHORT = {XSD: "xsd:", RDF: "rdf:"}


def plain(binding: dict) -> Any:
    """One value of a result row. An IRI as <iri>, text as text, whole numbers and truth values as such, other
    numbers as numbers, every other typed value with its datatype, text with a language with its language."""
    kind, value = binding.get("type"), binding.get("value")
    if kind == "uri":
        return f"<{value}>"
    if kind == "bnode":
        return f"_:{value}"
    if kind == "triple":
        return f"<<( {plain(value['subject'])} {plain(value['predicate'])} {plain(value['object'])} )>>"
    language, datatype = binding.get("xml:lang"), binding.get("datatype")
    if language:
        return {"value": value, "lang": language}
    if not datatype or datatype == XSD + "string":
        return value
    name = XSD_NAMES.get(datatype[len(XSD):]) if datatype.startswith(XSD) else None
    if name == "integer" and INTEGER.fullmatch(value):
        return int(value)
    if name == "boolean" and value in ("true", "false"):
        return value == "true"
    if name == "decimal":                        # as a number where that loses nothing
        try:
            number = float(value)
            if math.isfinite(number) and (datatype != XSD + "decimal" or Decimal(repr(number)) == Decimal(value)):
                return number
        except (ValueError, InvalidOperation):
            pass
    for long, short in _SHORT.items():
        if datatype.startswith(long):
            datatype = short + datatype[len(long):]
    return {"value": value, "datatype": datatype}


class StoreError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code, self.message = code, message


class FusekiCell:
    arm = "rdf"

    def __init__(self, cell_id: str, endpoint: str, user: str, password: str, log_path: Path | str,
                 state_path: Path | str | None = None, namespace: str | None = None,
                 max_rows: int = 200, timeout_s: float = 60.0, log_wait_s: float = 5.0):
        """`endpoint` is the dataset's address (http://host:3030/cell), `log_path` the dataset's change log as the
        tool can read it, `state_path` the tool's own mark of how far the log is dealt with."""
        self.cell_id = cell_id
        self.endpoint = urllib.parse.urlsplit(endpoint.rstrip("/"))
        self.authorization = "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        self.log_path = Path(log_path)
        self.state_path = Path(state_path) if state_path else self.log_path.with_suffix(".settled")
        self.namespace = namespace                   # fixed for the cell; otherwise read per realisation
        self.max_rows, self.timeout_s, self.log_wait_s = max_rows, timeout_s, log_wait_s
        self.mirror = Mirror()
        self.offset = 0                              # bytes of the log that are dealt with
        self.not_ready = f"the change log of {cell_id} was not checked yet; no unit was opened"
        self.judge: Callable[[dict], Any] | None = None        # set by the service: the checker and the mode
        self.report: Callable[..., None] | None = None         # set by the service: record and warn
        self.connection: http.client.HTTPConnection | None = None
        self._delta: tuple[dict, dict] | None = None           # the statement being checked: added and removed by subject
        self._ns: str | None = namespace

    def attach(self, judge: Callable[[dict], Any], report: Callable[..., None]) -> None:
        self.judge, self.report = judge, report

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    # ------------------------------------------------------------------ talking to the store

    def _http(self, service: str, body: bytes, content_type: str, accept: str = "*/*",
              timeout_s: float | None = None) -> tuple[int, bytes]:
        path = f"{self.endpoint.path}/{service}"
        headers = {"Authorization": self.authorization, "Content-Type": content_type, "Accept": accept}
        for attempt in (1, 2):                       # a connection the store closed in the meantime is opened again once
            fresh = self.connection is None
            if fresh:
                self.connection = http.client.HTTPConnection(self.endpoint.hostname, self.endpoint.port or 80,
                                                             timeout=timeout_s or self.timeout_s)
            try:
                self.connection.request("POST", path, body=body, headers=headers)
                response = self.connection.getresponse()
                return response.status, response.read()
            except socket.timeout:
                self.close()
                raise StoreError("Fuseki.Timeout", f"the store did not answer within {timeout_s or self.timeout_s:g} s")
            except (http.client.HTTPException, OSError) as error:
                self.close()
                if fresh or attempt == 2:
                    raise StoreError("Fuseki.Unreachable", f"the store cannot be reached: {type(error).__name__}: {error}")
        raise AssertionError("not reached")

    @staticmethod
    def _failure(status: int, body: bytes) -> StoreError:
        text = body.decode("utf-8", "replace").strip()
        if text.startswith("<"):                     # an error page: keep its text only
            text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text)).strip()
        if status == 400 and text.startswith(("Parse error", "Lexical error", "Encountered")):
            return StoreError("Fuseki.ParseError", text)
        return StoreError(f"Fuseki.{status}", text or f"the store answered {status}")

    def _query(self, statement: str, form: str) -> tuple[list[dict], bool]:
        accept = "application/n-triples" if form in ("CONSTRUCT", "DESCRIBE") else "application/sparql-results+json"
        status, body = self._http("sparql", statement.encode("utf-8"), "application/sparql-query", accept)
        if status != 200:
            raise self._failure(status, body)
        if form in ("CONSTRUCT", "DESCRIBE"):
            lines = [line for line in body.decode("utf-8").splitlines() if line.strip()]
            return [{"triple": line} for line in lines[:self.max_rows]], len(lines) > self.max_rows
        result = json.loads(body)
        if "boolean" in result:
            return [{"ask": result["boolean"]}], False
        names, bindings = result["head"].get("vars", []), result["results"]["bindings"]
        rows = [{name: plain(binding[name]) if name in binding else None for name in names}
                for binding in bindings[:self.max_rows]]
        return rows, len(bindings) > self.max_rows

    def _update(self, statement: str) -> None:
        status, body = self._http("update", statement.encode("utf-8"), "application/sparql-update")
        if status not in (200, 204):
            raise self._failure(status, body)

    def _blocks(self, expect: bool) -> list[Block]:
        """The blocks the log gained. With `expect`, wait until a complete one is there."""
        deadline = time.monotonic() + (self.log_wait_s if expect else 0.05)
        while True:
            blocks, offset, partial = read_blocks(self.log_path, self.offset)
            if (blocks or not expect) and not partial:
                self.offset = offset
                return blocks
            if time.monotonic() > deadline:
                self.offset = offset
                return blocks
            time.sleep(0.002)

    def _settle(self) -> None:
        """Mark the log as dealt with up to here."""
        temporary = self.state_path.with_name(self.state_path.name + ".tmp")
        temporary.write_text(json.dumps({"offset": self.offset, "triples": self.mirror.count}), encoding="utf-8")
        os.replace(temporary, self.state_path)

    # ------------------------------------------------------------------ taking changes out again

    def _undo(self, added: dict[tuple, list[str]], removed: dict[tuple, list[str]], quads: list[tuple] = ()) -> bool:
        """Take `added` out and put `removed` back, through the store's patch endpoint. True when the log then
        shows exactly that. Lines for a named graph are reversed as they stand: named graphs are kept empty."""
        lines = ["TX ."]
        lines += [f"{'D' if op == 'A' else 'A'} {' '.join(terms)} ." for op, *terms in reversed(quads)]
        lines += [f"D {' '.join(raw)} ." for raw in reversed(list(added.values()))]
        lines += [f"A {' '.join(raw)} ." for raw in removed.values()]
        lines.append("TC .")
        try:
            status, _ = self._http("patch", ("\n".join(lines) + "\n").encode("utf-8"), "application/rdf-patch")
        except StoreError:
            return False
        if status not in (200, 204):
            return False
        blocks = [block for block in self._blocks(expect=True) if block.committed]
        if len(blocks) != 1:                          # the store answers 200 to a patch it could not read, and aborts
            return False
        after: dict[tuple, bool] = {**{t: True for t in added}, **{t: False for t in removed}}
        for op, terms in blocks[0].lines:
            if len(terms) == 3:
                after[tuple(canonical(term) for term in terms)] = op == "A"
        return all(present == self.mirror.has(triple) for triple, present in after.items())

    def _foreign(self, blocks: list[Block], where: str) -> bool:
        """Blocks no statement of the tool accounts for: undo them and report. False when the undo failed."""
        added, removed, quads = self.mirror.net(blocks)
        if not (added or removed or quads):
            self._settle()
            return True
        undone = self._undo(added, removed, quads)
        if self.report:
            self.report("unattributed_write",
                        f"a write reached the store without passing the tool ({where}); "
                        + ("it was undone" if undone else "it could NOT be undone"),
                        changes={"triples": {"added": [nt_line(t) for t in added], "removed": [nt_line(t) for t in removed]},
                                 "named_graph_lines": [" ".join(q) for q in quads]}, undone=undone)
        if undone:
            self._settle()
        return undone

    def _catch_up(self, where: str) -> bool:
        blocks = self._blocks(expect=False)
        return self._foreign(blocks, where) if blocks else True

    # ------------------------------------------------------------------ start and readiness

    def start(self, *_: Any, wait_s: float = 30.0) -> None:
        """Rebuild the tool's list from the log, take out what nobody checked, and compare with the store."""
        deadline = time.monotonic() + wait_s
        while True:                                   # the store may still be starting
            try:
                self.counts()
                break
            except StoreError as error:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"{self.cell_id}: {error.message}")
                time.sleep(0.5)
        self.mirror, self.offset = Mirror(), 0
        try:
            mark = json.loads(self.state_path.read_text(encoding="utf-8"))["offset"]
        except (OSError, ValueError, KeyError):
            mark = None
        blocks, end, _ = read_blocks(self.log_path, 0)
        if mark is not None and mark > end:
            raise RuntimeError(f"{self.cell_id}: the change log {self.log_path} is shorter than the tool's mark; "
                               "the log and the mark do not belong together")
        settled = [block for block in blocks if mark is None or block.end <= mark]
        for block in settled:
            self.mirror.apply(*self.mirror.net([block])[:2])
        self.offset = settled[-1].end if settled else 0
        if mark is None and settled and self.report:
            self.report("settle_mark_missing", "the tool had no mark for this change log; everything in it was taken as kept")
        if mark is None:
            self._settle()
        if not self._catch_up("found at the start of the tool"):
            raise RuntimeError(f"{self.cell_id}: writes nobody checked are in the store and could not be taken out")
        if not self.ready():
            raise RuntimeError(f"{self.cell_id}: {self.not_ready}")

    def rebuild(self) -> None:
        """Read the whole graph from the store into the tool's list. The log is the normal way; this is for a
        list that has come apart from the store, which a person should then look at."""
        status, body = self._http("sparql", b"CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", "application/sparql-query",
                                  "application/n-triples", timeout_s=max(self.timeout_s, 600.0))
        if status != 200:
            raise self._failure(status, body)
        mirror = Mirror()
        for line in body.decode("utf-8").splitlines():
            if line.strip():
                mirror.add(tuple(canonical(term) for term in split_terms(line)))
        self.mirror = mirror

    def _in_step(self) -> tuple[bool, int, int]:
        """Whether the tool's list has as many triples as the store; rebuilt from the store once if not."""
        triples, named = self.counts()
        if triples != self.mirror.count:
            listed = self.mirror.count
            self.rebuild()
            triples, named = self.counts()
            if self.report:
                self.report("list_rebuilt", f"the tool's list of {self.cell_id} had {listed} triples and the store "
                            f"{triples}; the list was read from the store again", listed=listed, stored=triples)
        return triples == self.mirror.count and not named, triples, named

    def counts(self) -> tuple[int, int]:
        """The store's own count: triples in the default graph, triples in named graphs."""
        rows, _ = self._query("SELECT ?d ?n { { SELECT (COUNT(*) AS ?d) { ?s ?p ?o } } "
                              "{ SELECT (COUNT(*) AS ?n) { GRAPH ?g { ?s ?p ?o } } } }", "SELECT")
        return rows[0]["d"], rows[0]["n"]

    def ready(self) -> bool:
        """In place of the hook check: the store logs what it is told, the tool reads that log, and the tool's
        list has as many triples as the store. `not_ready` says what failed."""
        try:
            if not self._catch_up("found before a unit"):
                self.not_ready = f"a write nobody checked is in the store of {self.cell_id} and could not be taken out; no unit was opened"
                return False
            self._update("DELETE WHERE { <urn:dbtool:canary> <urn:dbtool:canary> ?nothing }")
            blocks = self._blocks(expect=True)
            if len(blocks) != 1 or not blocks[0].committed or blocks[0].lines:
                self.not_ready = (f"the store of {self.cell_id} does not write its changes to {self.log_path}, "
                                  "or something else writes to it; no unit was opened")
                if blocks:
                    self._foreign(blocks, "found with the readiness check")
                return False
            self._settle()
            in_step, triples, named = self._in_step()
        except StoreError as error:
            self.not_ready = f"the store of {self.cell_id} did not answer the readiness check ({error.message}); no unit was opened"
            return False
        if not in_step:
            self.not_ready = (f"the tool's list of {self.cell_id} has {self.mirror.count} triples, the store {triples} "
                              f"(and {named} in named graphs); no unit was opened")
            return False
        return True

    hook_is_active = ready

    def admin(self, statement: str) -> list[dict]:
        """A statement of the harness itself (reset, set-up). Not checked and not a write attempt."""
        form = form_of(statement)
        if form != "UPDATE":
            return self._query(statement, form)[0]
        self._update(statement)
        added, removed, _ = self.mirror.net(self._blocks(expect=True))
        self.mirror.apply(added, removed)
        self._settle()
        return []

    def size(self) -> dict:
        return {"triples": self.mirror.count}

    # ------------------------------------------------------------------ the agent's statement

    def run(self, statement: str, context: dict, parameters: dict | None = None) -> Outcome:
        self._ns = self.namespace or namespace_of(context.get("realisation"))
        try:
            if not self._catch_up("found before a statement"):
                return Outcome("unchecked", error_code="undo_failed",
                               error="a write that nobody checked is in the store and could not be taken out")
            form = form_of(statement)
            if form != "UPDATE":
                rows, truncated = self._query(statement, form)
                return Outcome("result", rows=rows, truncated=truncated)
            self._update(statement)
        except StoreError as error:
            # A failed update leaves an aborted block: nothing changed. A committed one here is somebody else's.
            committed = [block for block in self._blocks(expect=False) if block.committed]
            if committed and not self._foreign(committed, "found after a failed statement"):
                return Outcome("unchecked", error_code="undo_failed",
                               error="a write that nobody checked is in the store and could not be taken out")
            self._settle()
            return Outcome("error", error_code=error.code, error=error.message)

        # From here on the store holds the statement. Whatever goes wrong below, it must not stay unjudged.
        added: dict[tuple, list[str]] = {}
        removed: dict[tuple, list[str]] = {}
        try:
            blocks = [block for block in self._blocks(expect=True) if block.committed]
            if not blocks:
                return Outcome("unchecked", error_code="log_missing",
                               error=f"the store took a statement and wrote nothing to {self.log_path}")
            added, removed, quads = self.mirror.net(blocks)
            return self._judged(statement, context, blocks, added, removed, quads)
        except Exception as error:                    # a fault of the tool itself
            undone = bool(added or removed) and self._undo(added, removed)
            return Outcome("unchecked", error_code="tool_fault",
                           error=f"the tool failed on a statement the store had taken ({type(error).__name__}: {error}); "
                                 + ("the statement was taken out again" if undone else "the statement may still be in the store"))

    def _judged(self, statement: str, context: dict, blocks: list[Block], added: dict, removed: dict,
                quads: list[tuple]) -> Outcome:
        if len(blocks) > 1 or quads:
            # Two blocks: something else wrote while the statement ran, and the two cannot be told apart.
            # A line for a named graph: the statement left the cell's one graph.
            if not self._undo(added, removed, quads):
                return Outcome("unchecked", error_code="undo_failed",
                               error="a statement that was not checked could not be taken out of the store")
            self._settle()
            if quads and len(blocks) == 1:
                return Outcome("error", error_code="DbTool.NamedGraph",
                               error="The statement wrote into a named graph. The cell has one graph; nothing was stored.")
            if self.report:
                self.report("unattributed_write", "something else wrote to the store while a statement ran; "
                            "both were undone", statement=statement, undone=True)
            return Outcome("error", error_code="DbTool.Interference",
                           error="The store was written to from outside while the statement ran. Nothing was stored; send it again.")
        if not (added or removed):
            self._settle()
            written = [nt_line(tuple(canonical(term) for term in terms))
                       for op, terms in blocks[0].lines if op == "A" and len(terms) == 3]
            return Outcome("unchanged", existing=[triple_id(line) for line in dict.fromkeys(written)])

        changes = self._changes(list(added), list(removed))
        self._delta = (self._by_subject(added), self._by_subject(removed))
        try:
            verdict = self.judge({**context, **changes}) if self.judge else None
        finally:
            self._delta = None
        if verdict is not None and verdict.rejected:
            if not self._undo(added, removed):
                return Outcome("unchecked", error_code="undo_failed",
                               error="a rejected statement could not be taken out of the store")
            self._settle()
            return Outcome("rejected", changes=changes, rejections=verdict.rejections or [],
                           observations=verdict.observations, not_checkable=verdict.not_checkable)
        self.mirror.apply(added, removed)
        self._settle()
        return Outcome("stored", changes=changes, rejections=(verdict.rejections or []) if verdict else [],
                       observations=verdict.observations if verdict else [],
                       not_checkable=verdict.not_checkable if verdict else [])

    # ------------------------------------------------------------------ from triples to `changes`

    @staticmethod
    def _by_subject(triples: Any) -> dict[str, set[tuple[str, str]]]:
        out: dict[str, set[tuple[str, str]]] = {}
        for s, p, o in triples:
            out.setdefault(s, set()).add((p, o))
        return out

    def _name(self, iri_token: str) -> str:
        """A class or predicate as the schema names it: the local name inside the schema's namespace, `label`
        for rdfs:label, the whole IRI for everything else, so that a name from another vocabulary stays visible."""
        iri = key_of(iri_token)
        if self._ns and iri.startswith(self._ns):
            return iri[len(self._ns):]
        return "label" if iri == RDFS_LABEL else iri

    def _describe(self, triples: set[tuple[str, str]]) -> tuple[list[str], dict[str, Any], set[tuple[str, str]]]:
        """One subject's triples as (labels, properties, relations)."""
        labels, values, relations = [], {}, set()
        for p, o in sorted(triples):
            if p == RDF_TYPE and not is_literal(o):
                labels.append(self._name(o))
            elif p == RDF_REIFIES:
                continue
            elif is_literal(o) or o.startswith("<<("):
                values.setdefault(self._name(p), []).append(value_of(o))
            else:
                relations.add((p, o))
        return labels, {name: found[0] if len(found) == 1 else found for name, found in values.items()}, relations

    def _annotations(self, term: str, state: Callable[[str], set], reifiers: set[str]) -> dict[str, Any]:
        values: dict[str, list] = {}
        for reifier in sorted(reifiers):
            for p, o in sorted(state(reifier)):
                if p != RDF_REIFIES:
                    values.setdefault(self._name(p), []).append(value_of(o))
        return {name: found[0] if len(found) == 1 else found for name, found in values.items()}

    def _changes(self, added: list[tuple], removed: list[tuple]) -> dict:
        """The statement's triples as nodes and relationships, in the shape of checker.py.

        A node is a subject with a class or a literal value. A relationship is a triple whose object is a
        resource; the values hung on it with the annotation form are its properties.
        """
        plus, minus = self._by_subject(added), self._by_subject(removed)
        before = self.mirror.of
        after = lambda s: (before(s) - minus.get(s, set())) | plus.get(s, set())
        reifiers_before = lambda term: self.mirror.reifiers.get(term, set())

        def reifiers_after(term: str) -> set[str]:
            now = set(reifiers_before(term))
            now -= {s for s, pairs in minus.items() if (RDF_REIFIES, term) in pairs}
            now |= {s for s, pairs in plus.items() if (RDF_REIFIES, term) in pairs}
            return now

        nodes, relationships, ends = [], [], {}
        deleted = {"nodes": [], "relationships": []}
        touched_terms: dict[str, None] = {}              # relationships whose annotations changed
        subjects = list(dict.fromkeys([s for s, _, _ in added] + [s for s, _, _ in removed]))

        def end(token: str) -> str:
            key = key_of(token)
            labels, properties, _ = self._describe(after(token))
            ends[key] = {"labels": labels, "properties": properties, "exists": bool(labels or properties)}
            return key

        for s in subjects:
            was, now = before(s), after(s)
            if any(p == RDF_REIFIES for p, _ in was | now):
                for p, o in was | now:
                    if p == RDF_REIFIES:
                        touched_terms[o] = None
                continue
            labels_was, values_was, relations_was = self._describe(was)
            labels, values, relations = self._describe(now)
            if (labels, values) != (labels_was, values_was):
                if labels or values:
                    nodes.append({
                        "key": key_of(s), "labels": labels, "properties": values,
                        "change": "changed" if labels_was or values_was else "created",
                        "changed_properties": {name: {"old": values_was.get(name), "new": values.get(name)}
                                               for name in sorted(set(values) | set(values_was))
                                               if values.get(name) != values_was.get(name)} if labels_was or values_was else {},
                        "added_labels": [label for label in labels if label not in labels_was] if labels_was or values_was else [],
                        "removed_labels": [label for label in labels_was if label not in labels]})
                else:
                    deleted["nodes"].append(key_of(s))
            for p, o in sorted(relations - relations_was):
                term = f"<<( {s} {p} {o} )>>"
                touched_terms.pop(term, None)
                relationships.append({"key": nt_line((s, p, o)), "type": self._name(p), "from": end(s), "to": end(o),
                                      "properties": self._annotations(term, after, reifiers_after(term)),
                                      "change": "created", "changed_properties": {}})
            for p, o in sorted(relations_was - relations):
                deleted["relationships"].append(nt_line((s, p, o)))

        created = {r["key"] for r in relationships}
        for term in touched_terms:                       # a value on a relationship was added, changed or removed
            inner = decode(term)
            s, p, o = inner["s"], inner["p"], inner["o"]
            if nt_line((s, p, o)) in created:
                continue
            asserted = (p, o) in after(s)
            was = self._annotations(term, before, reifiers_before(term))
            now = self._annotations(term, after, reifiers_after(term))
            if not asserted and (p, o) in before(s):
                continue                                 # the relationship went; it is listed under deleted
            relationship = {"key": nt_line((s, p, o)), "type": self._name(p), "from": end(s), "to": end(o),
                            "properties": now, "change": "changed" if asserted else "created",
                            "changed_properties": {name: {"old": was.get(name), "new": now.get(name)}
                                                   for name in sorted(set(was) | set(now)) if was.get(name) != now.get(name)}}
            if not asserted:
                relationship["asserted"] = False         # values hung on a statement the graph does not hold
            relationships.append(relationship)

        return {"nodes": nodes, "relationships": relationships, "ends": ends, "deleted": deleted,
                "triples": {"added": [nt_line(t) for t in added], "removed": [nt_line(t) for t in removed]}}

    # ------------------------------------------------------------------ lookup for the checker

    def _candidates(self, name: str) -> list[str]:
        """The predicates or classes a name of the schema can stand for."""
        if "://" in name or name.startswith("urn:"):
            return [f"<{name}>"]
        found = [f"<{self._ns}{name}>"] if self._ns else []
        return found + [f"<{RDFS_LABEL}>"] if name == "label" else found

    def find(self, label: str | None, property: str, value: Any) -> list[str]:
        """Keys of the nodes that were in the graph before the statement, of this class and with this value.
        Values are compared in their written form, whatever their datatype."""
        wanted = value if isinstance(value, (list, tuple)) else [value]
        wanted = {str(v["value"]) if isinstance(v, dict) and "value" in v else str(v) for v in wanted}
        classes = self._candidates(label) if label else []
        found: dict[str, None] = {}
        for predicate in self._candidates(property):
            for lexical in wanted:
                for subject in self.mirror.with_value(predicate, lexical):
                    if not classes or any((RDF_TYPE, cls) in self.mirror.of(subject) for cls in classes):
                        found[key_of(subject)] = None
        return list(found)

    def entity_exists(self, key: str) -> int:
        """1 when the key names a node of the graph (a subject with a class or a literal value), counting what
        the statement being checked wrote; 0 otherwise. For a reference that points outside the statement."""
        token = term_of(key)
        triples = self.mirror.of(token)
        if self._delta is not None:
            plus, minus = self._delta
            triples = (triples - minus.get(token, set())) | plus.get(token, set())
        labels, values, _ = self._describe(triples)
        return 1 if labels or values else 0
