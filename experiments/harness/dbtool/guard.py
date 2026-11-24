"""Statements the tool does not pass on: commands that change the schema or the
set-up of the store, not the data, and in SPARQL everything that leaves the one
graph of the cell.

This is a check on the text and a second line of defence. In the runs the first
line is the account of the cell, which has no right to run these commands; on the
RDF side the tool also undoes a statement that wrote into a named graph after all.
"""

from __future__ import annotations

import re

_STRINGS = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"", re.S)
_COMMENTS = re.compile(r"//[^\n]*|/\*.*?\*/", re.S)

_CYPHER = [
    (re.compile(r"\b(CREATE|DROP)\s+(OR\s+REPLACE\s+)?(CONSTRAINT|INDEX|RANGE\s+INDEX|TEXT\s+INDEX|POINT\s+INDEX|"
                r"VECTOR\s+INDEX|FULLTEXT\s+INDEX|LOOKUP\s+INDEX|DATABASE|COMPOSITE\s+DATABASE|ALIAS|USER|ROLE)\b", re.I),
     "constraints, indexes, databases and accounts are fixed for the run"),
    (re.compile(r"\b(ALTER|GRANT|DENY|REVOKE|START|STOP)\s+(DATABASE|USER|ROLE|ALIAS|\w+\s+ON)\b", re.I),
     "databases and accounts are fixed for the run"),
    (re.compile(r"\bapoc\.(trigger|schema\.assert|systemdb|cypher\.runSchema)\b", re.I),
     "this procedure changes the set-up of the store"),
    (re.compile(r"\bdbms\.\w+", re.I), "this procedure changes the set-up of the store"),
    (re.compile(r"\bIN\s+(\d+\s+)?(CONCURRENT\s+)?TRANSACTIONS\b", re.I),
     "a statement runs as one transaction; split it into several statements"),
    (re.compile(r"^\s*(USE|:\w+)\b", re.I), "the database of the cell is fixed"),
]


# SPARQL: what is left when strings (with a language tag), IRIs, comments, variables and prefixed names are gone
# are the words of the language itself.
_SPARQL_STRINGS = re.compile(r'(?:"""(?:\\.|[^\\])*?"""|\'\'\'(?:\\.|[^\\])*?\'\'\'|"(?:\\.|[^"\\\n])*"|\'(?:\\.|[^\'\\\n])*\')'
                             r'(?:@[A-Za-z0-9-]+)?', re.S)
_SPARQL_IRIS = re.compile(r'<[^<>"{}|^`\\\x00-\x20]*>')
_SPARQL_COMMENTS = re.compile(r"#[^\n]*")
_SPARQL_NAMES = re.compile(r"[?$]\w+|(?:[A-Za-z_][\w.-]*)?:[\w.%:-]*")

_SPARQL = [
    ({"LOAD"}, "the store reads nothing from an address outside it"),
    ({"SERVICE"}, "a statement stays inside the store of the cell"),
    ({"CREATE", "DROP", "CLEAR", "COPY", "MOVE", "ADD"},
     "graphs are fixed for the run; to take data out, use DELETE"),
    ({"GRAPH", "FROM", "USING", "WITH", "NAMED"},
     "the cell has one graph; named graphs are not used"),
]


def sparql_words(statement: str) -> list[str]:
    """The bare words of a SPARQL statement, in order."""
    bare = _SPARQL_COMMENTS.sub(" ", _SPARQL_IRIS.sub(" <> ", _SPARQL_STRINGS.sub(' "" ', statement)))
    return re.findall(r"[A-Za-z_]+", _SPARQL_NAMES.sub(" ", bare))


def strip(statement: str) -> str:
    """The statement without string contents and comments."""
    return _COMMENTS.sub(" ", _STRINGS.sub("''", statement))


def refuse(statement: str, arm: str) -> str | None:
    """A reason when the statement is not passed on, otherwise None."""
    if not statement or not statement.strip():
        return "the statement is empty"
    if arm == "rdf":
        words = {word.upper() for word in sparql_words(statement)}
        for refused, reason in _SPARQL:
            if words & refused:
                return reason
        return None
    if arm != "pg":
        return None
    bare = strip(statement)
    for pattern, reason in _CYPHER:
        if pattern.search(bare):
            return reason
    return None
