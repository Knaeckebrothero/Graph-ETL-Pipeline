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

## State on 13 November 2026

- 980 units from 30 source files; all four modality strata are present. Rules version `draft-0.2`.

| Stratum | Frankfurt | Campus | Federal | Munich | Berlin | Products |
|---|---|---|---|---|---|---|
| numeric tabular | 2 | 0 | 0 | 49 | 2 | 0 |
| tabular-categorical | 49 | 107 | 49 | 0 | 0 | 0 |
| catalogue | 0 | 0 | 0 | 0 | 0 | 200 |
| free text | 106 | 0 | 305 | 60 | 51 | 0 |

Decided on 13 November 2026 and applied in `sample_rules.yaml`:

- **Statutes and federal acts are in the sample.**
- **The two city statistics tables are out:** city totals, not rules for a household.
- **Eight development and three pilot units per stratum.** The three numeric pilot units come from the Munich and Berlin fee statutes (`pilot_allowed`); those sources never enter development.
- **Shares** (Frankfurt and campus at least 70 %, each contrast city about 15 %) apply to the units tied to a city or the campus. Federal and product units are reported as their own groups. The cutter prints the shares; today they are 62 %, 26 % and 12 % over all 426 such units, before any draw of the held-out sample.
- **Campus:** planned without the intranet rules; the campus sources are the two student compilations.
- **Operators' content** (guides, site list, collection calendar of FES, AWM and BSR) needs consent. Consent is requested; the sample is planned without it.

What follows from that:

- **A fee statute is free text except its fee sections.** The sections that hold the fee tables are labelled numeric tabular (`stratum_overrides`), for Frankfurt, Munich and Berlin alike. Before, the Munich and Berlin fee statutes were numeric as a whole, which put 37 text sections into the numeric stratum.
- **The numeric stratum holds seven fee tables in all:** Frankfurt 2, Munich 3, Berlin 2. Both Frankfurt tables fall into development and three of the other five into the pilot, so **two fee tables remain for the held-out sample**, beside 46 units of Munich site lists. Cutting the large fee sections into table segments would raise that number; it is not decided.
- The development bucket has two numeric units, not eight: only Frankfurt's fee tables are free for development.
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
- A fee statute is one source with one stratum; single sections are moved to another stratum with `stratum_overrides`. A fee section is one unit however long its table is (up to 14,786 bytes).
- The identity lines (`Dokument`, `Herausgeber`, `Gilt für`, `Teil`) are in German because the sources are; whether jurisdiction belongs in the identity of a table unit is a choice to confirm.
