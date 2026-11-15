# Eval 2 second reading (open point 15): checklist for 13 to 16 Nov 2026

Input from Thesis A on 13 Nov: GQM tables (one per dimension plus cross-cutting), metric cards, formulas,
requirement table. Effort cap: one day. Output: this table filled in, sent to Niklas by 16 Nov; outcome logged in
Thesis A's change log; Thesis B cites the review date.

## The four checks (EV-02, BU-06)

| Check | Question per card | Fails when |
|---|---|---|
| C1 unit and denominator | Is the unit of observation named? Is the denominator explicit, with the n.e. rule for an empty denominator? | "rate" without a denominator; a denominator that mixes units (assertions and CQs); n.e. undefined |
| C2 no double counting | Does any item enter two components of the same aggregate? Do F1 to F6 states partition the items? | an assertion counted in coverage and populatability of the same composite; a CQ in two eligibility sets |
| C3 forward trace | Does the card point to a requirement id, and does the requirement point back to the card and to an analysis? | orphan card; requirement without card; analysis using a metric with no card |
| C4 no domain constant | Does the definition work on any domain without a fessi-specific constant, list or threshold? | a fee, a waste type, a jurisdiction name or a fixed count in the definition (allowed only in the domain binding table) |

## Review table (one row per card)

| Card | Component | C1 | C2 | C3 | C4 | Finding | Severity (blocker / fix / note) |
|---|---|---|---|---|---|---|---|
| | functional populatability | | | | | | |
| | document-level population success | | | | | | |
| | common-scope conformance | | | | | | |
| | treatment-scope conformance | | | | | | |
| | vocabulary coherence | | | | | | |
| | reference precision | | | | | | |
| | supportedness | | | | | | |
| | run-to-run stability | | | | | | |
| | pre-commit friction | | | | | | |
| | resource efficiency | | | | | | |
| | coverage (not F1) | | | | | | |
| | conditional queryability | | | | | | |
| | end-to-end queryability | | | | | | |

## Questions to carry into the reading (from Thesis B's side)

1. Which fields of `insert_attempt` and `source_end` does each populatability card consume? (Name the field; if
   it is not in `kgc-trace/1`, it is a change request for F3.)
2. Denominator of resource efficiency: populated assertions, correct assertions, or representable ones?
3. Pre-commit friction: which of rejects, retries, abandoned units enter it, and is it per source or per run?
4. Run-to-run stability: which statistic (range, agreement, Krippendorff) and over which items?
5. Where does the equivalence table live, and is identity (duplicate nodes) treated as a match or a miss?

## Log entry template (for Thesis A's change log)

`2026-11-16 | Eval 2 second reading by Yagubi | cards read: n | blockers: n | fixes: n | notes: n | time: h h`
