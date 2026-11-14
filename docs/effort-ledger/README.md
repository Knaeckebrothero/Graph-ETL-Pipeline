# Effort ledger

Both authors log their hours per week and activity from 3 November 2026 (first row: the week starting Monday 2 November). Thesis A uses the ledger to report what applying the evaluation technique costs (its feasibility criterion): fixed cost once, marginal cost per further schema realisation. Run cost in tokens and euro comes from the run logs, not from this ledger.

| File | What it is |
|---|---|
| `ledger_template.csv` | Empty ledger. Copy it to `ledger_A.csv` or `ledger_B.csv` and add one row per week and activity |
| `activities.csv` | The eleven activity codes, with fixed or marginal and what counts |

## Columns

| Column | Content | Example |
|---|---|---|
| `week_start` | Monday of the week, ISO date | `2026-11-09` |
| `author` | `A` or `B` | `B` |
| `activity` | A code from `activities.csv` | `verification` |
| `hours` | Hours in that week, quarter-hour steps, decimal point | `3.5` |
| `serves` | Whose result the work serves: `A`, `B` or `shared` | `A` |
| `realisation` | Only for `realisation_prep` and `run_operation`: arm and treatment, else empty | `PG-S4` |
| `note` | Free text, optional. No names of third persons | `calibration round 1` |

## Rules

- One row per week, author, activity and `serves`. Add up within the week; do not log single days. Log at the end of each week.
- Thesis writing is not logged. The ledger covers the work the technique needs, not the text about it.
- Hours that serve only Thesis B (`serves` = `B`) are logged for completeness and are not part of Thesis A's cost figures.

## Example rows (not data)

```
week_start,author,activity,hours,serves,realisation,note
2026-11-09,A,coordination,4.0,shared,,meeting 12 Nov and agreement draft
2026-11-30,B,verification,3.5,A,,calibration on 30 assertions
2026-11-23,A,run_operation,2.0,A,PG-S4,round 1
```
