# Technique (Thesis A)

The schema-fitness evaluation technique of Thesis A on paper.

| File | What it is |
|---|---|
| `eval2-package-v0.1.md` | The package for the second reading (Eval 2): what is measured, the requirements, the stage model, one table of goals, questions and metrics per dimension, one card per metric, the formulas with the order of aggregation, and the four decision scenarios. Version 0.1 of 12 November 2026, corrected to 14 November 2026 |
| `common-scope-rules-draft-0.1.md` | The seven rules every stored fact must pass whatever the schema (card P.3), written out for the validator: what each rule tests at the write and on the export, the rejection message, what it needs. Draft 0.1 of 14 November 2026; six points open, listed in its section 8 |

**Status: draft, not frozen.** Version 1.0 is tagged on 23 November 2026 (freeze F4) after the second reading and the pilot. The reading is done by Thesis B against the four checks named in section 0 of the package; its checklist is `experiments/thesis-b/eval2-review-checklist.md`.

The scoring code that implements sections 3, 5 and 6 is in `analysis/thesis-a/`.
