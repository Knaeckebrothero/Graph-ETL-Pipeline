"""Bindings the rules read: the equivalence table (canonical predicates and types), the format and unit table.

Both are Thesis A's (Eval 2 package v0.1, section 1.4; common-scope rule list v0.2, CS4 "needs"). The format and unit
table is owed by Thesis A by 23 November 2026 and does not exist yet; until then `FormatTable()` carries Thesis B's
provisional formats (ISO 8601 dates, plain integers and decimals with point or comma, yes/no words) and reports every
unit as `not_checkable`. A rule that needs a missing binding reports `not_checkable` instead of pass or reject, so that
nothing is silently approved.

File formats accepted here (placeholders until Thesis A names the format):

    equivalence.json   {"predicates": {"canonical_predicate": ["alias", ...], ...},
                        "types":      {"CanonicalType": ["alias", ...], ...}}
    formats.json       {"units": ["kg", "g", "l", ...],
                        "formats": {"date": ["%Y-%m-%d"], "datetime": ["iso"], "decimal": ["point", "comma"], ...}}
    units.json         {"units": [...]}            (the unit list alone, older placeholder)
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

DATATYPES = {"string", "integer", "decimal", "boolean", "date", "datetime", "duration", "quantity"}
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_DURATION = re.compile(r"^P(?!$)(\d+Y)?(\d+M)?(\d+W)?(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+S)?)?$")
BOOL_WORDS = {"true", "false", "yes", "no", "ja", "nein", "1", "0"}
QUANTITY = re.compile(r"^\s*([+-]?\d+(?:[.,]\d+)?)\s*([^\d\s].*)?$")


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


class FormatTable:
    """The format and unit table of rule CS4. `check(value, datatype)` returns None when the value is a valid form of the
    datatype, else a short statement of what was expected. `unit_status(unit)` is "known", "unknown" or "not_checkable"
    (no unit list loaded)."""

    def __init__(self, units: list[str] | None = None, formats: dict[str, list[str]] | None = None, version: str | None = None):
        self.units_loaded = units is not None
        self._units = {u.strip() for u in (units or [])}
        self.formats = formats or {}
        self.version = version or ("provisional (Thesis B, until Thesis A's table of 23 Nov)" if not formats else "loaded")

    @classmethod
    def load(cls, path: str | Path) -> "FormatTable":
        d = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(list(d.get("units") or []) if "units" in d else None, d.get("formats"), d.get("version"))

    @property
    def loaded(self) -> bool:
        return self.units_loaded

    def unit_status(self, unit: str | None) -> str:
        if not self.units_loaded:
            return "not_checkable"
        return "known" if unit is not None and unit.strip() in self._units else "unknown"

    def known(self, unit: str) -> bool:
        return self.unit_status(unit) == "known"

    def check(self, value: Any, datatype: str | None) -> str | None:
        if datatype in (None, "string"):
            return None
        if datatype not in DATATYPES:
            return f"one of {sorted(DATATYPES)}"
        if datatype == "integer":
            if isinstance(value, bool) or not (isinstance(value, int) or (isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()))):
                return "whole number"
        elif datatype == "decimal":
            if isinstance(value, bool):
                return "decimal number"
            if not isinstance(value, (int, float)):
                try:
                    float(str(value).strip().replace(",", "."))
                except ValueError:
                    return "decimal number"
        elif datatype == "boolean":
            if not (isinstance(value, bool) or str(value).strip().lower() in BOOL_WORDS):
                return "yes or no"
        elif datatype == "date":
            tn = type(value).__name__
            if tn in ("date", "Date"):
                return None
            if not (isinstance(value, str) and ISO_DATE.match(value.strip())):
                return "ISO 8601 date (YYYY-MM-DD)"
            try:
                date.fromisoformat(value.strip())
            except ValueError:
                return "ISO 8601 date (YYYY-MM-DD)"
        elif datatype == "datetime":
            tn = type(value).__name__
            if tn in ("datetime", "DateTime"):
                return None
            try:
                datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
            except ValueError:
                return "ISO 8601 date and time"
        elif datatype == "duration":
            if type(value).__name__ in ("timedelta", "Duration"):
                return None
            if not (isinstance(value, str) and ISO_DURATION.match(value.strip())):
                return "ISO 8601 duration"
        elif datatype == "quantity":
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return None
            if not (isinstance(value, str) and QUANTITY.match(value)):
                return "number with unit"
        return None


class UnitList(FormatTable):
    """Older placeholder name: a format table that holds only the unit list."""

    def __init__(self, units: list[str] | None = None):
        super().__init__(units=units)

    @classmethod
    def load(cls, path: str | Path) -> "UnitList":
        d = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(list(d.get("units") or []))
