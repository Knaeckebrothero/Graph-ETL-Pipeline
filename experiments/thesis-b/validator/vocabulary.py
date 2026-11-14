"""Bindings the common-scope rules read: the equivalence table (canonical predicates and types) and the unit list.

Both are Thesis A's Domain Binding (Eval 2 package v0.1, section 1.4). Until Thesis A delivers them, a rule that needs
one reports `not_checkable` instead of pass or reject, so that nothing is silently approved.

File formats accepted here (placeholders until Thesis A names the format):

    equivalence.json   {"predicates": {"canonical_predicate": ["alias", ...], ...},
                        "types":      {"CanonicalType": ["alias", ...], ...}}
    units.json         {"units": ["kg", "g", "l", ...]}
"""
from __future__ import annotations

import json
import re
from pathlib import Path


def _norm(s: str) -> str:
    """Lemma-light normalisation: case, surrounding space, inner runs of space/underscore/hyphen."""
    return re.sub(r"[\s_\-]+", "_", str(s).strip().lower())


class EquivalenceTable:
    """Maps surface predicates and types to the canonical vocabulary."""

    def __init__(self, predicates: dict[str, list[str]] | None = None, types: dict[str, list[str]] | None = None):
        self.loaded = predicates is not None or types is not None
        self._pred: dict[str, str] = {}
        self._type: dict[str, str] = {}
        for canon, aliases in (predicates or {}).items():
            self._pred[_norm(canon)] = canon
            for a in aliases:
                self._pred[_norm(a)] = canon
        for canon, aliases in (types or {}).items():
            self._type[_norm(canon)] = canon
            for a in aliases:
                self._type[_norm(a)] = canon

    @classmethod
    def load(cls, path: str | Path) -> "EquivalenceTable":
        d = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(d.get("predicates") or {}, d.get("types") or {})

    def canonical_predicate(self, surface: str) -> str | None:
        return self._pred.get(_norm(surface))

    def canonical_type(self, surface: str) -> str | None:
        return self._type.get(_norm(surface))


class UnitList:
    def __init__(self, units: list[str] | None = None):
        self.loaded = units is not None
        self._units = {u.strip() for u in (units or [])}

    @classmethod
    def load(cls, path: str | Path) -> "UnitList":
        d = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(list(d.get("units") or []))

    def known(self, unit: str) -> bool:
        return unit.strip() in self._units
