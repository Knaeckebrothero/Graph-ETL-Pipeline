"""Statements the tool does not pass on: commands that change the schema or the
set-up of the store, not the data.

This is a check on the text and a second line of defence. In the runs the first
line is the account of the cell, which has no right to run these commands.
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


def strip(statement: str) -> str:
    """The statement without string contents and comments."""
    return _COMMENTS.sub(" ", _STRINGS.sub("''", statement))


def refuse(statement: str, arm: str) -> str | None:
    """A reason when the statement is not passed on, otherwise None."""
    if not statement or not statement.strip():
        return "the statement is empty"
    if arm != "pg":
        return None
    bare = strip(statement)
    for pattern, reason in _CYPHER:
        if pattern.search(bare):
            return reason
    return None
