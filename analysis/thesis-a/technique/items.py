"""The item table of one cell: one state per reference item (package section 3).

Section 3.2: an assertion takes exactly one of F1, F2, F3 or success; a CQ takes exactly one of
F1 to F6 or success; inventory items have one extra state, "not in source".
Section 3.4: a CQ is eligible only when every assertion in Req(q) is in state success; otherwise
it inherits the worst upstream state of its required assertions (F1 before F2 before F3) and
carries the partial-presence flag "k of n required assertions present".
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Callable, Iterable, Iterator, Mapping, Optional


class ItemType(str, Enum):
    ASSERTION = "assertion"
    CQ = "cq"
    INPUT_STATEMENT = "input_statement"
    INVENTORY_ITEM = "inventory_item"


class State(str, Enum):
    F1 = "F1"                    # not representable
    F2 = "F2"                    # representable but not extracted
    F3 = "F3"                    # extracted but wrongly encoded
    F4 = "F4"                    # no query
    F5 = "F5"                    # query invalid (sub-code: parse, runtime)
    F6 = "F6"                    # answer wrong
    SUCCESS = "success"
    NOT_IN_SOURCE = "not_in_source"   # inventory items only


class Tier(str, Enum):
    CORE = "core"
    SUPPORTING = "supporting"
    EXCLUDED = "excluded"


UPSTREAM = (State.F1, State.F2, State.F3)              # worst first (section 3.4)
ALLOWED_STATES = {
    ItemType.ASSERTION: {State.F1, State.F2, State.F3, State.SUCCESS},
    ItemType.INPUT_STATEMENT: {State.F1, State.F2, State.F3, State.SUCCESS},
    ItemType.INVENTORY_ITEM: {State.F1, State.F2, State.F3, State.SUCCESS, State.NOT_IN_SOURCE},
    ItemType.CQ: {State.F1, State.F2, State.F3, State.F4, State.F5, State.F6, State.SUCCESS},
}
SUB_CODES = {
    State.F5: {"parse", "runtime"},
    State.F6: {"accidental", "memorised_constant", "empty_without_discrimination", "boolean_without_witness"},
}
FUNNEL_LEVELS = ("produced", "parses", "executes", "semantically_correct")   # section 3.6


class ItemTableError(ValueError):
    """The table breaks a rule of section 3."""


def funnel_level(state: State, sub_code: Optional[str]) -> int:
    """Number of funnel levels a query attempt reached: 0 none ... 4 semantically correct."""
    if state is State.F4:
        return 0
    if state is State.F5:
        if sub_code == "parse":
            return 1
        if sub_code == "runtime":
            return 2
        raise ItemTableError("state F5 needs the sub-code parse or runtime")
    if state is State.F6:
        return 3
    if state is State.SUCCESS:
        return 4
    raise ItemTableError(f"state {state.value} is not a query outcome")


@dataclass(frozen=True)
class Item:
    item_id: str
    item_type: ItemType
    state: State
    sub_code: Optional[str] = None
    # from the frozen reference
    tier: Optional[Tier] = None                 # assertions
    exclusion_reason: Optional[str] = None      # excluded-tier assertions
    stratum: Optional[str] = None
    jurisdiction: Optional[str] = None
    authority_class: Optional[str] = None
    phenomenon_tags: tuple[str, ...] = ()
    certainty_flags: tuple[str, ...] = ()
    canary: bool = False
    unresolved: bool = False                    # BE-12
    # matcher
    match_tier: Optional[int] = None            # 1 to 4, or None (unmatched)
    # questions
    cluster_id: Optional[str] = None
    required: tuple[str, ...] = ()              # Req(q)
    pass_id: Optional[int] = None               # 1 to 3
    first_state: Optional[State] = None         # state at the first attempt
    first_sub_code: Optional[str] = None
    attempt_index: Optional[int] = None         # index of the final attempt (0 = first)
    partial_presence: Optional[tuple[int, int]] = None   # (k, n) for an ineligible CQ
    family: Optional[str] = None
    archetype: Optional[str] = None
    complexity_class: Optional[str] = None
    bucket: Optional[str] = None                # development or held-out question families
    # inventory
    inventory_id: Optional[str] = None
    mentioned: Optional[bool] = None            # does the frozen sample mention the item (mention scan)
    # trace
    evidence: Optional[str] = None              # pointer to mapping, validator, matcher, query log or answer-set record

    @property
    def key(self) -> tuple[str, str, Optional[int]]:
        return (self.item_type.value, self.item_id, self.pass_id)

    @property
    def is_success(self) -> bool:
        return self.state is State.SUCCESS

    @property
    def representable(self) -> bool:
        return self.state is not State.F1

    @property
    def eligible(self) -> bool:
        """For a CQ: not carrying an inherited upstream state."""
        return self.item_type is ItemType.CQ and self.state not in UPSTREAM


Predicate = Callable[[Item], bool]


def worst_upstream(states: Iterable[State]) -> Optional[State]:
    """F1 before F2 before F3; None when every state is success."""
    seen = set(states)
    for s in UPSTREAM:
        if s in seen:
            return s
    return None


def cq_row(cq_id: str, cluster_id: str, required: Iterable[str], assertion_states: Mapping[str, State], *,
           pass_id: int, final: Optional[tuple[State, Optional[str]]] = None,
           first: Optional[tuple[State, Optional[str]]] = None, attempt_index: Optional[int] = None,
           **reference_fields) -> Item:
    """Build the row of one CQ in one pass from the assertion states and the query outcome.

    Ineligible: the row takes the inherited state and the flag (k, n); a query outcome, if one was
    logged, is not used. Eligible: `final` (and `first`, default the same) are (state, sub_code)
    with state in F4, F5, F6 or success.
    """
    required = tuple(required)
    missing = [a for a in required if a not in assertion_states]
    if missing:
        raise ItemTableError(f"CQ {cq_id}: required assertions not in the table: {missing}")
    states = [assertion_states[a] for a in required]
    inherited = worst_upstream(states)
    if inherited is not None:
        k = sum(1 for s in states if s is State.SUCCESS)
        return Item(cq_id, ItemType.CQ, inherited, cluster_id=cluster_id, required=required, pass_id=pass_id,
                    first_state=inherited, partial_presence=(k, len(states)), **reference_fields)
    if final is None:
        raise ItemTableError(f"CQ {cq_id}, pass {pass_id}: eligible but no query outcome given")
    first = first if first is not None else final
    return Item(cq_id, ItemType.CQ, final[0], final[1], cluster_id=cluster_id, required=required, pass_id=pass_id,
                first_state=first[0], first_sub_code=first[1],
                attempt_index=attempt_index if attempt_index is not None else (0 if first == final else 1),
                **reference_fields)


@dataclass
class ItemTable:
    """All items of one cell. `validate()` enforces the rules of sections 3.2 and 3.4."""
    items: list[Item] = field(default_factory=list)
    cell: Mapping[str, object] = field(default_factory=dict)   # treatment, arm, replication, condition set

    def __iter__(self) -> Iterator[Item]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def of_type(self, item_type: ItemType, where: Optional[Predicate] = None) -> list[Item]:
        return [it for it in self.items if it.item_type is item_type and (where is None or where(it))]

    def assertion_states(self) -> dict[str, State]:
        return {it.item_id: it.state for it in self.items if it.item_type is ItemType.ASSERTION}

    def assertion(self, item_id: str) -> Item:
        for it in self.items:
            if it.item_type is ItemType.ASSERTION and it.item_id == item_id:
                return it
        raise KeyError(item_id)

    def passes(self) -> list[int]:
        return sorted({it.pass_id for it in self.items if it.item_type is ItemType.CQ and it.pass_id is not None})

    def subset(self, where: Predicate) -> "ItemTable":
        return ItemTable([it for it in self.items if where(it)], self.cell)

    def validate(self, *, allow_regression: bool = True) -> "ItemTable":
        """Raise ItemTableError if a rule of section 3 is broken.

        The final attempt of a CQ is its last attempt and is the one scored. A revision can break a
        query that worked, so the final attempt may be below the first one in the funnel (card Q.2 as
        corrected on 13 November 2026); the repair gain is then negative. allow_regression=False
        turns such a row into an error, for a harness that only ever repairs after a failure.
        """
        errors: list[str] = []
        seen: set = set()
        for it in self.items:
            if it.key in seen:
                errors.append(f"{it.key}: more than one row; an item takes exactly one state per cell")
            seen.add(it.key)
            if it.state not in ALLOWED_STATES[it.item_type]:
                errors.append(f"{it.key}: state {it.state.value} is not allowed for a {it.item_type.value}")
            allowed = SUB_CODES.get(it.state, set())
            if it.sub_code is not None and it.sub_code not in allowed:
                errors.append(f"{it.key}: sub-code {it.sub_code!r} does not belong to state {it.state.value}")
            if it.state is State.F5 and it.sub_code is None and it.item_type is ItemType.CQ:
                errors.append(f"{it.key}: F5 needs the sub-code parse or runtime")
            if it.match_tier not in (None, 1, 2, 3, 4):
                errors.append(f"{it.key}: match tier must be 1 to 4 or empty")
            if it.item_type is ItemType.ASSERTION and it.tier is None:
                errors.append(f"{it.key}: an assertion needs a tier")
            if it.item_type is not ItemType.CQ and it.pass_id is not None:
                errors.append(f"{it.key}: only CQ rows carry a pass id")
            if it.item_type is ItemType.INVENTORY_ITEM:
                if it.inventory_id is None:
                    errors.append(f"{it.key}: an inventory item needs its inventory id")
                if it.mentioned is None:
                    errors.append(f"{it.key}: an inventory item needs the mention flag")
                elif it.state is State.NOT_IN_SOURCE and it.mentioned:
                    errors.append(f"{it.key}: not in source contradicts mentioned")
                elif it.state in (State.F2, State.F3, State.SUCCESS) and not it.mentioned:
                    errors.append(f"{it.key}: an item the sample does not mention takes the state not in source (or F1)")
        assertions = {it.item_id: it for it in self.items if it.item_type is ItemType.ASSERTION}
        per_cq: dict[str, list[Item]] = {}
        for it in self.items:
            if it.item_type is not ItemType.CQ:
                continue
            per_cq.setdefault(it.item_id, []).append(it)
            if it.pass_id not in (1, 2, 3):
                errors.append(f"{it.key}: a CQ row needs a pass id 1 to 3")
            if it.cluster_id is None:
                errors.append(f"{it.key}: a CQ needs a cluster id")
            missing = [a for a in it.required if a not in assertions]
            if missing:
                errors.append(f"{it.key}: required assertions not in the table: {missing}")
                continue
            if not it.canary:
                bad = [a for a in it.required if assertions[a].tier is not Tier.CORE or assertions[a].canary]
                if bad:
                    errors.append(f"{it.key}: Req(q) must lie in the core tier (section 6.1); not core or canary: {bad}")
            states = [assertions[a].state for a in it.required]
            inherited = worst_upstream(states)
            if inherited is None:
                if it.state in UPSTREAM:
                    errors.append(f"{it.key}: every required assertion is success, so the CQ is eligible and cannot carry {it.state.value}")
                if it.partial_presence is not None:
                    errors.append(f"{it.key}: an eligible CQ carries no partial-presence flag")
                if it.first_state is None:
                    errors.append(f"{it.key}: an eligible CQ needs its first-attempt state")
                elif it.state not in UPSTREAM:
                    try:
                        if (funnel_level(it.first_state, it.first_sub_code) > funnel_level(it.state, it.sub_code)
                                and not allow_regression):
                            errors.append(f"{it.key}: the final attempt is below the first attempt in the funnel")
                    except ItemTableError as exc:
                        errors.append(f"{it.key}: {exc}")
            else:
                if it.state is not inherited:
                    errors.append(f"{it.key}: ineligible, must inherit {inherited.value} (F1 before F2 before F3), has {it.state.value}")
                k = sum(1 for s in states if s is State.SUCCESS)
                if it.partial_presence != (k, len(states)):
                    errors.append(f"{it.key}: partial-presence flag must be ({k}, {len(states)}), is {it.partial_presence}")
        for cq_id, rows in per_cq.items():
            if len({(r.cluster_id, r.required, r.canary) for r in rows}) > 1:
                errors.append(f"cq {cq_id}: cluster, required set or canary flag differ between its passes")
        if errors:
            raise ItemTableError("; ".join(errors))
        return self


def with_states(table: ItemTable, new_states: Mapping[str, State]) -> ItemTable:
    """A copy with the given assertion states replaced (CQ rows are not recomputed)."""
    return ItemTable([replace(it, state=new_states[it.item_id])
                      if it.item_type is ItemType.ASSERTION and it.item_id in new_states else it
                      for it in table.items], table.cell)
