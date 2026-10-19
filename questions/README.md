# Competency questions

Thesis A needs about 500 competency questions, each linked to one or more assertions of the tiered reference benchmark, and split into structure-answerable questions (for example item → rule → stream) and text-answerable ones (for example handling tips). Thesis B uses the same questions. Nothing of that set exists yet.

## Seeds

[`seed/wise2425-gruppe1/`](seed/wise2425-gruppe1/) holds the only earlier question set: the evaluation data of WiSe 2024/25 Projekt Fessie, Gruppe 1 (Backend), from `Eval_Chatbot_V2.zip` → `eval/data/`, dated March 2025.

| File | Entries | Content |
|---|---|---|
| `final_extended_transformed_test.json` | 119 | 80 of them "Wo/Wie entsorge ich X?" |
| `abfall_abc_eval_25_final_ergänzt.json` | 25 | "Wie entsorge ich X?" on Abfall-ABC items (the "Chroma-basiert" set) |
| `Fragekatalog_Studierende.json` | 17 | Mixed questions, for example on collection intervals |
| `test.json` | 4 | Smoke test |
| `*_kurz.json` | | Short-answer variants with the same questions and gold answers |

Each entry has `question`, `expected_output` and persona fields (`sinus_milieu_kurzel`, `age_group_class_num`, `language`, `answer_length`, `model`). The persona fields are not needed.

Quality, checked on 19 October 2026:

- **160 unique questions** in the three main sets, not 161: one question appears in two sets. `test.json` adds one more.
- **94 are templated** from the first 69 routed rows of the Recyclingzentrum list and the bin DOCX bullet lists.
- **8 of the 25 Abfall-ABC gold answers are wrong:** they put excluded items into the bin. Some items have conflicting gold answers (energy-saving lamps, batteries). Opening hours and fees are a 2024/25 snapshot.
- **No AVV, KrWG or GESTIS questions.**
- Roughly **110–120** are usable as seeds for questions a graph can answer (inference). Re-derive every gold answer from a dated source in `sources/` before use.

Reuse of other students' material needs Prof. Fink's permission, which has not been asked yet. Cite the set as unpublished project material of WiSe 2024/25 Gruppe 1.

## Further seeds

- the three worked flows in the v4.0 specification;
- the five AVV test items in the "Basic Powerpoint" deck: Energiesparlampe, Computermaus, AA-Batterie, Pizzakarton, Glasflasche;
- the scenario types in `docs/prior-work/presentations/fessi_presentation.html`: ambiguous, state-dependent, multi-step, edge case;
- the hard cases of WiSe 2024/25 Gruppe 2: ambiguous ("Birne"), over-specific ("55 Zoll Samsung 4K Smart TV"), state-dependent ("Farbeimer mit Farbresten");
- AVV mirror entries and the chemical-to-AVV mappings.
