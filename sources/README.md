# Sources

Everything the graphs are built from. One rule holds throughout: every source file is listed in [`manifest.csv`](manifest.csv) with its origin, licence, size, SHA-256 and date, so that every imported fact and every gold answer can be traced to a dated file.

| Folder | Content | In Git |
|---|---|---|
| [`raw/`](raw/) | Small source files, one folder per source | Yes |
| [`large/`](large/README.md) | Open Food Facts and Wiktionary dumps (about 7.7 GB) | No; see `large/README.md` |
| [`catalogue/`](catalogue/) | Candidate source list and the LLM research behind it | Yes |

## Manifest columns

| Column | Meaning |
|---|---|
| `id` | Stable identifier, also used as the folder name under `raw/` |
| `path` | File path from the repository root; empty for sources fetched at run time |
| `bytes`, `sha256` | Size and checksum of the file as it is stored here |
| `file_date` | Modification date of the stored file. This is not necessarily the retrieval date, which was never recorded for the inherited files |
| `title`, `publisher` | What the file is and who published it |
| `kind`, `format` | Content type (tabular text, product catalogue, legal text…) and file format |
| `stratum` | Candidate stratum for the sample, or `reference benchmark` if the source must stay out of the sample |
| `licence` | Licence as far as known. "none recorded" means unknown, not free |
| `importer` | Code that reads the file |
| `status` | Whether and when the file was imported in the earlier semesters |
| `notes` | Known problems and processing hints |

Regenerate sizes and checksums whenever a file changes. A changed file is a new snapshot and gets a new row, so earlier runs stay traceable.

## Adding a source

1. Download a snapshot. Never import straight from a live API or web page; the EU Ecolabel import of February 2026 cannot be reproduced for exactly that reason.
2. Put it in `raw/<id>/` if it is under about 25 MB, otherwise in `large/<id>/`, and say in `large/README.md` where to get it.
3. Add a manifest row with the retrieval date in `notes`, the licence and the stratum.
4. Check the file for personal data before committing it.

## What the inherited sources are

From [`docs/prior-work/`](../docs/prior-work/README.md), checked against the files on 19 October 2026:

- **`fes-abfall-abc`** is the only source behind the 548 `WasteItem`s of December 2025. It appears to merge the FES Abfall-ABC with the Recyclingzentrum list (inference). No code or document records who produced it.
- **`fes-facility-map`** is generated output: the WiSe 2024/25 chatbot rebuilt it from FES pages at every start-up. This is the snapshot of 17 February 2025.
- **`avv-workbook`** was built by SoSe 2025 Projekt DS Gruppe 2. Only chapters 01–20 (842 codes) are the official AVV. Chapters 21 and 22 are invented pseudo-codes for Abfall-ABC and lab items, which the February 2026 import loaded as `AVVCode` nodes. Use the official AVV text for the reference benchmark; this file is useful for its campus routes. The campus-route column is redacted (see below).
- **`group3-abfall-abc`** (606 rows) separates FES and FRA UAS routes. `abfall_bereinigt.csv` has placeholder garbage in its first row.
- **The FES bin guides** exist as DOCX and as PDF renders of the same text, which makes them usable as a format control.
- **`frankfurt-abfallsatzung`** is a reference-benchmark candidate. Keep benchmark documents (AVV, Abfallsatzung, LAGA) out of the population sample, or the evaluation becomes circular.
- **Open Food Facts** (4.4 GB Parquet) needs a documented subsample. **Open Beauty Facts** has packaging text for 28 % of its records and no material tags.
- **EU Ecolabel** was imported from its live API without a snapshot.

## Candidate catalogue

[`catalogue/sources_catalog.csv`](catalogue/sources_catalog.csv) has 324 rows on 243 hosts, about 256 distinct real sources. Treat it as an **unverified, LLM-generated candidate list**:

- It comes from deep-research reports (probably Gemini Deep Research, January 2026) steered by the prompts in [`catalogue/research/prompts/`](catalogue/research/prompts/). The reports are in [`catalogue/research/results/`](catalogue/research/results/) and were merged into [`catalogue/research/discovered_sources.md`](catalogue/research/discovered_sources.md).
- Nobody verified the entries. The six verification columns in [`catalogue/quellenliste/`](catalogue/quellenliste/) (440 sources with stable IDs, from the Teams folder) are empty in every row.
- Format, licence and relevance are empty in about 60 % of the rows; only 8 rows name a real licence. About 38 % of the rows contain parse artefacts.
- URL deduplication dropped EU Ecolabel (which was imported) and LAGA M20 (Tier 1). KrWG is missing entirely.
- None of the Frankfurt sources that were actually imported appears in it.

## Gaps

- **Stratified sample:** not drawn. The exposé plans numeric CSV, product catalogues and free text. Truly numeric CSVs are rare and none is local; candidates are the Destatis Abfallbilanz, Bavarian volume-to-mass factors, Schleswig-Holstein AVV volumes and the NREL Battery Failure Databank. The most common local type, tabular text (Abfall-ABC, AVV workbook), fits no planned stratum, so a tabular-categorical stratum may be worth adding.
- **Reference benchmark:** official AVV text (gesetze-im-internet, EU Vocabularies SKOS, EEA CSV), KrWG and GESTIS are not here. GESTIS allows linking but not scraping.
- **Readers:** no deterministic reader exists for PDF or DOCX.

## Restricted material

Not in this folder and not to be committed:

- **Staff contact details.** The original AVV workbook carries names, phone numbers and emails of university staff in its campus-route column. `avv_workbook_redacted.csv` replaces them with `[Name entfernt]`, `[Telefon entfernt]` and `[E-Mail entfernt]` and is otherwise identical. The original is in `private/`.
- **Post Direkt and Sinus-milieu persona data** from WiSe 2024/25: proprietary, never part of any graph. Removed.
- **Third-party stock images** of paint cans from WiSe 2024/25: removed.
- **FES material:** an NDA with FES was signed on 10 November 2025. Its scope is not yet checked, so do not publish FES-derived content.

All of these remain in the Git history up to the tag `wise2526-submission`; the repository is private for that reason.
