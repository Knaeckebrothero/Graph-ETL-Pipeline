# Source sample (draft)

Cuts the sources on disk into self-contained source units and writes a draft manifest. **Nothing here is frozen.**
The freeze is a tagged commit that records the manifest hash; until then the choices in `sample_rules.yaml`
that are not marked as decided are proposals.

| File | What it is |
|---|---|
| `sample_rules.yaml` | Every choice: which source is in, its modality stratum, jurisdiction and authority class, what the agent sees as its identity, the chunk size per modality, the seed, the bucket sizes. Each excluded file is listed with its reason. |
| `cut_units.py` | Deterministic cutter and selector. `--check` re-cuts and compares with `out/`. |
| `out/units/<unit_id>.txt` | One file per unit: exactly what an agent is handed (identity lines, `---`, content). Not tracked. |
| `out/manifest_draft.csv` | One row per unit: hashes, source, locator, stratum, jurisdiction, authority class, bucket, chunk rule, seed. Not tracked. |

```bash
.venv/bin/python experiments/sample/cut_units.py           # cut and select
.venv/bin/python experiments/sample/cut_units.py --check   # reproducibility check
```

## State on 15 November 2026

- 1,014 units from 30 source files; all four modality strata are present. Rules version `draft-0.3` (980 units under `draft-0.2` of 13 November, before the fee sections were cut).

| Stratum | Frankfurt | Campus | Federal | Munich | Berlin | Products |
|---|---|---|---|---|---|---|
| numeric tabular | 8 | 0 | 0 | 59 | 2 | 0 |
| tabular-categorical | 49 | 107 | 49 | 0 | 0 | 0 |
| catalogue | 0 | 0 | 0 | 0 | 0 | 200 |
| free text | 110 | 0 | 305 | 74 | 51 | 0 |

Decided on 13 November 2026 and applied in `sample_rules.yaml`:

- **Statutes and federal acts are in the sample.**
- **The two city statistics tables are out:** city totals, not rules for a household.
- **Eight development and three pilot units per stratum.** The three numeric pilot units come from the Munich and Berlin fee statutes (`pilot_allowed`); those sources never enter development.
- **Shares** (Frankfurt and campus at least 70 %, each contrast city about 15 %) apply to the units tied to a city or the campus. Federal and product units are reported as their own groups. The cutter prints the shares; today they are 60 %, 29 % and 12 % over all 460 such units, before any draw of the held-out sample. The cut of the fee sections added more Munich than Frankfurt units; the draw of the held-out sample has to bring the shares to the target.
- **Campus:** planned without the intranet rules; the campus sources are the two student compilations.
- **Operators' content** (guides, site list, collection calendar of FES, AWM and BSR) needs consent. Consent is requested; the sample is planned without it.

What follows from that:

- **A fee statute is free text except its fee sections.** The sections that hold the fee tables are labelled numeric tabular (`stratum_overrides`), for Frankfurt, Munich and Berlin alike. Before, the Munich and Berlin fee statutes were numeric as a whole, which put 37 text sections into the numeric stratum.
- **Fee sections are cut at their numbered paragraphs** (decided 15 November 2026; `split_paragraphs`). Each numbered paragraph, (1), (2), ..., is one unit, with the section's heading lines repeated in front and the paragraph number in the `Teil` line. A piece is numeric tabular if at least two of its lines hold an amount (`chunk.table_min_amount_lines`); a piece with one amount or none is a sentence and stays free text, like the rest of the statute. A section without at least two numbered paragraphs stays whole; that is the case for both Berlin fee sections, which are divided by letters, not by numbered paragraphs.
- **The numeric stratum now holds 23 fee tables** (seven before the cut): Frankfurt 8, Munich 13, Berlin 2. All eight Frankfurt tables are the development units of the stratum. The three pilot units are one Berlin section and two Munich paragraphs. **Twelve fee tables remain for the held-out sample** (Munich 11, Berlin 1; two before the cut), beside 46 units of Munich site lists. Eighteen paragraph pieces are sentences and went to free text (Munich 14, Frankfurt 4).
- The numeric pilot units come from the sources marked `pilot_allowed` only (`buckets.pilot_only_from_pilot_allowed`); before, that followed from Frankfurt having too few numeric units and was not stated as a rule.
- The cut changed which Frankfurt fee-statute text section is in development (section 7 in place of section 10), because the seeded shuffle runs per source and stratum. No unit that kept its id changed its content.
- Munich and Berlin have no A-to-Z guide in the sample.
- 25 snapshots were added on 12 November (statutes of the three cities, five federal acts as XML, open-data tables) with rows in `sources/manifest.csv`; the pending Frankfurt amendments for 1 January 2027 are on disk but not cut.
- Sources marked `held_out_only` (Munich, Berlin, federal acts other than the waste list) never enter development; they enter the pilot only where `pilot_allowed` says so. Every unit outside development and pilot is `candidate`.
- Not in the sample yet: Open Food Facts (needs the subsample rule), EU Ecolabel (needs a snapshot), the Hessian act (save by hand, the portal needs a browser), the canary units (written by hand by the author).
- Unit ids are neutral (`u` plus twelve hex digits of a hash of source and locator), so a unit's name tells an agent nothing about its bucket.
- `out/` is ignored by Git: several sources have no recorded licence.

## Known limits of this draft

- PDF text comes from `pdftotext -layout`; the version is written into each row's chunk rule. Line breaks and hyphenation are those of the PDF.
- Statutes are cut at section headings: a number alone on a line (Frankfurt) or "§ n Title" on one line (Munich, Berlin). The body starts at the last heading numbered 1 and a heading is accepted only if its number follows the previous one; check the section list of every new statute.
- The manifest has one `bucket` field (development, pilot, candidate; later held-out and canary). The register also reports results by whether the schema designers had seen the material (BE-05); that is a second label per source and is not in the manifest yet.
- A fee statute is one source with one stratum; single sections are moved to another stratum with `stratum_overrides`, and the sections under `split_paragraphs` are cut at their numbered paragraphs. A paragraph that points to another one ("the fees of paragraph 2") cannot be read completely from its own piece. Pieces of one section are not independent of each other; an analysis by unit has to treat them as one cluster. The largest fee unit is now a whole Berlin section (7,749 bytes).
- The Munich statutes carry a page number and a running footer line ("... Stand: 01.01.2025") inside the text, also in the middle of a table; only the Frankfurt statutes have such lines dropped (`drop_lines`).
- The identity lines (`Dokument`, `Herausgeber`, `Gilt für`, `Teil`) are in German because the sources are; whether jurisdiction belongs in the identity of a table unit is a choice to confirm.
