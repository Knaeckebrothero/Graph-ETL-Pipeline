# Cleaning report for `sources_catalog_clean.csv`

Date of this pass: 2026-11-12. **Everything in the clean file is a pre-fill for a human check.** No value has been verified by the thesis author; the column `verified_by_author` is empty in all 348 rows. Classes, screen decisions, publishers and repaired names were filled in by a language model from the catalogue text, the research notes and page titles. Treat them as proposals.

## Files

| File | Content |
|---|---|
| `sources_catalog_clean.csv` | 348 rows, 24 columns: the 324 catalogue rows plus 24 added rows |
| `clean_catalogue.py` | The rule-based steps. Standard library only |
| `prefill_judgements.csv` | Every value that needed judgement: 324 rows keyed by `orig_row`, 24 added rows keyed by `source_id`. A blank cell means "let the rules decide" |
| `CLEANING_REPORT.md` | This file. The tables come from `clean_catalogue.py --stats` |

```bash
python3 sources/catalogue/clean_catalogue.py                 # rebuild without network; reproduces the file byte for byte
python3 sources/catalogue/clean_catalogue.py --check-access  # re-check every URL and re-stamp checked_on
python3 sources/catalogue/clean_catalogue.py --stats         # print the tables below
```

To change a pre-filled value, edit `prefill_judgements.csv` and rebuild. The access columns are carried over from the existing output, so a rebuild does not need the network.

## Reading the clean file

- `source_id` identifies the URL-level entry; `orig_row` identifies the catalogue row. One pair of rows (278, 279) has the same URL and therefore the same id; the second is marked as a duplicate.
- `manifest_id` can hold several ids separated by `;` when several files belong to one source (a DOCX and its PDF render, the three Gruppe 3 files, the two Open Food Facts exports).
- `duplicate_of` holds the `source_id` of the canonical row. **Distinct sources are the rows where `duplicate_of` is empty.** Duplicates are kept and inherit the classes of their canonical row.
- `repaired` says what was changed and by which rule (P1 to P9, U). `prefill_basis` says, per column, whether a value came from the `catalogue`, the `manifest`, a `rule` or `judgement`.
- `notes` is a list of segments separated by ` | `: `origin:` (file and section the row came from), `desc:` (the catalogue description, cleaned), `note:` (remarks from the judgement file), `licence quote:`, `manifest:`, `duplicate:` and `access:` (the raw result of the request).
- A screen reason that starts with `borderline:` marks a decision that could reasonably go the other way.
- The columns are in the order of the task's list, so `verified_by_author` is the second to last column and `notes` the last.

## Rules applied

### Cleaning

| Rule | What it detects | What happens |
|---|---|---|
| U | URL form | Stray quotes stripped, scheme and host lower-cased, a bare host gets a slash, tracking parameters and empty fragments dropped, illegal characters percent-encoded. `http` is not upgraded to `https` and trailing slashes on deeper paths are kept, because both changes can break a URL. The untouched URL stays in `url_original` |
| P1 | Description is parser residue (`URL`, `curl ""`, `#`) | Dropped |
| P2 | Name is the host | Replaced by a name from the research notes or the page title (judgement file, basis named in `repaired`) |
| P3 | Name is a section label (`Data Source Categories`, `Categories`, `URLs`) | Replaced likewise |
| P4 | Name is markup residue (`# ...`, `URL: ...`, `@prefix`) | Replaced likewise |
| P5 | Format cell holds prose or a piece of a URL | Moved to `notes` |
| P6 | License cell holds prose | Moved to `notes` |
| P7 | Description only repeats the name | Dropped |
| P8 | URL split across columns at a comma or a `\|` (rows 187 and 280) | Rejoined from the research notes; both rejoined URLs answered 200 |
| P9 | URL is a template with a placeholder (`{region}`, `{id}`) | Flagged; no request is possible |

Of the 114 replaced names, 90 come from `research/discovered_sources.md` or `research/datasources.md`, 20 from the page title and 4 from judgement. Separately, 81 catalogue rows point to a site root instead of a document or dataset; this is noted in `notes` and not counted as a parse artefact.

### Identifiers and duplicates

- `source_id` is the quellenliste id when the normalised URL is the same (188 rows), or the same once hyphens are ignored (69 rows). The quellenliste has lost hyphens inside URLs (`gesetze-iminternet.de`; it looks like line-break damage), and 64 of its URLs end in a stray quote, so an exact comparison alone would miss a quarter of the matches. Where the quellenliste holds two ids for one URL, the one without the stray quote is used and the other is named in `notes`.
- All other rows get `cat-<host>-<nn>`, counted per host in catalogue order. Sources without any URL (the local files) get `cat-local-<nn>`; this extension of the scheme is mine.
- D1: same normalised URL (1 pair). D2: same source under another URL, by judgement (50 rows): an alias host, a site root next to the document itself, an API base next to an example call, a code repository next to the project page, a mirror or wrapper by another publisher. Different exports of one database (Open Food Facts as Parquet, JSONL and API) also count as one source. The full list is at the end of this report.

### Licence

A licence value is copied, never inferred. It is one of:

1. a licence name in the catalogue's License column (7 rows) or Description column (5 rows), both LLM-stated and unverified;
2. the manifest's wording, verbatim (20 rows);
3. a statement read on the source's own page on 2026-11-12, quoted with its URL in `notes` (8 rows, all in the two contrast cities);
4. `none recorded` (308 rows).

The catalogue's `Public`, `Free`, `Open` and `Commercial` are access terms, not licences. They are kept in `licence_basis` and the value is `none recorded` (108 rows).

### Format

In this order: judgement file, manifest (for the local files), URL extension (F1), the catalogue's Format cell mapped to the vocabulary (F2), observed content type (F3), API-like URL (F4), "the URL serves HTML" (F5), otherwise `unclear`. Several formats are joined with `;`. F2 repeats an unverified claim of the catalogue: where a row points to a site root, the stated format describes a document that the URL does not locate.

### Modality, authority class, jurisdiction

The definitions of the task were applied as given. Host rules (H) fill these columns for statute portals (gesetze-im-internet.de, EUR-Lex), community platforms (GitHub, Kaggle, Roboflow, PyPI, Wikipedia, Wikidata, OpenStreetMap) and the Open Food Facts family. Everything else is judgement. Tie-breakers used:

- **Modality.** One-page sorting sheets of AWM and BSR are `free text`, like the equivalent FES flyer in the manifest. Substance databases are `catalogue`. Image data, software and lexical networks fit no stratum and are `unclear`. Where the URL is only a site root and the catalogue description does not settle it, the value is `unclear`.
- **Authority class.** Official registers, statutory accident insurers, standards bodies and foreign agencies are C. Industry associations, manufacturers, NGOs, research data and third-party copies of official texts are D. Technical rules (TRBA, TRGS) are C, although they carry a presumption of conformity.
- **Jurisdiction.** `federal` also covers nationwide bodies of the Länder (LAGA, IPA). `other municipality or state` is used for German municipalities, states and institutions outside Frankfurt, Munich city and Berlin. Foreign sources and all product catalogues are `none`. Other universities are `other municipality or state`, including Gießen (in Hessen) and TU München (in Munich), because their rules do not apply on the Frankfurt campus or to Munich households.

### Population screen

`in` is given to (a) disposal rules published by the city of Frankfurt, FES or the university, including copies on disk; (b) federal, Hessian or EU **law** in the waste field that household or campus disposal rests on; (c) product, item and substance catalogues. `contrast` is given to household disposal rules of AWM/Munich and BSR/Berlin and the statutes and state law behind them. Everything else is `out`.

This is a strict reading. Nationwide guidance that is neither law nor published by the city, FES or the university is `out` and marked borderline (UBA, LAGA, the dual systems, consumer associations). Substance databases are `in` as lab supplies, because the task's definitions name them; the broad ones (PubChem, ChemSpider, CAS, ChEBI, NIST) are marked borderline. Manufacturer data-sheet libraries for batteries are `in`, marked borderline. Classification schemes, vocabularies, lexical resources, image datasets, software and research papers are `out`.

### Access check

One HEAD request per URL, a small GET if the HEAD does not succeed, one retry, 15 second timeout with a hard deadline (name resolution included), at most 4 requests at a time, `User-Agent: Mozilla/5.0 (compatible; catalogue-check)`. Only status, final URL and content type are kept. Page titles were read in a first pass the same evening to repair names; they are not stored.

- `ok (200)` means the URL answered. It says nothing about the content: a site root answers 200 whether or not the named document exists, and script-driven portals answer 200 for any id.
- `redirect -> host` is used when the final host differs. Redirects inside one host are `ok (200)` with a remark in the `access:` segment.
- `error 403` is in most cases bot protection, not a dead link.
- `on disk` is taken from the manifest; the check confirmed that all 25 files named there were present on the machine this pass ran on.
- The result is a snapshot of one evening. Between two passes on the same evening, some hosts changed their answer (dsmz.de 503 then 200, css.umich.edu 200 then 403).

## Counts

| Measure | Count |
|---|---|
| Catalogue rows in | 324 |
| Rows added | 24 |
| Rows out | 348 |
| Marked as duplicate | 51 |
| ... by rule D1 (same normalised URL) | 1 |
| ... by judgement D2 (same source under another URL) | 50 |
| Distinct sources (rows without duplicate_of) | 297 |
| ... from the catalogue | 273 |
| ... added | 24 |
| Catalogue rows with at least one repair | 148 |
| Catalogue rows with a parse artefact (rules P1 to P9) | 144 |
| Catalogue rows whose URL is only a site root | 81 |
| Rows with a quellenliste id | 257 |
| ... matched only when hyphens are ignored | 69 |
| Rows with a new cat- id | 91 |
| Rows linked to the manifest | 23 |

The catalogue has 324 rows on 243 hosts, as the README says. Counting P7 (a description that only repeats the name), 144 rows (44 %) carry a parse artefact; without P7 it is 125 rows (39 %), which matches the README's "about 38 %".

### Population screen (distinct sources)

| screen | sources | of these marked borderline |
|---|---|---|
| in | 44 | 16 |
| contrast | 11 | 0 |
| out | 234 | 24 |
| unclear | 8 | 0 |

### Repairs by rule (catalogue rows; a row can carry several)

| Rule | Rows |
|---|---|
| P1 | 69 |
| P2 | 63 |
| P3 | 29 |
| P4 | 13 |
| P5 | 6 |
| P6 | 9 |
| P7 | 32 |
| P8 | 2 |
| P9 | 4 |
| U | 2 |

## Cross-table: modality stratum by jurisdiction

### Modality stratum by jurisdiction (distinct sources screened in or contrast)

| modality stratum | Frankfurt | campus | Hessen | federal | EU | Munich | Berlin | none | total |
|---|---|---|---|---|---|---|---|---|---|
| numeric tabular | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| tabular-categorical | 3 | 2 | 0 | 1 | 1 | 1 | 2 | 0 | 10 |
| catalogue | 0 | 0 | 0 | 1 | 3 | 0 | 0 | 22 | 26 |
| free text | 7 | 0 | 0 | 3 | 1 | 4 | 3 | 0 | 18 |
| unclear | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 1 |
| total | 10 | 2 | 0 | 5 | 5 | 6 | 5 | 22 | 55 |

### The same without the sources marked borderline

| modality stratum | Frankfurt | campus | Hessen | federal | EU | Munich | Berlin | none | total |
|---|---|---|---|---|---|---|---|---|---|
| numeric tabular | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| tabular-categorical | 3 | 2 | 0 | 1 | 0 | 1 | 2 | 0 | 9 |
| catalogue | 0 | 0 | 0 | 1 | 2 | 0 | 0 | 10 | 13 |
| free text | 6 | 0 | 0 | 2 | 1 | 4 | 3 | 0 | 16 |
| unclear | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 1 |
| total | 9 | 2 | 0 | 4 | 3 | 6 | 5 | 10 | 39 |

### The same, only sources that are on disk

| modality stratum | Frankfurt | campus | Hessen | federal | EU | Munich | Berlin | none | total |
|---|---|---|---|---|---|---|---|---|---|
| numeric tabular | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| tabular-categorical | 3 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 5 |
| catalogue | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 2 | 2 |
| free text | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| unclear | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| total | 10 | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 14 |

What the table shows:

- **`numeric tabular` is empty** for every jurisdiction that is in scope. The catalogue does contain 19 numeric sources (next table), but they are statistics, conversion factors, other municipalities' collection calendars or foreign data. The four candidates the README names for a numeric stratum (Destatis Abfallbilanz, Bavarian volume-to-mass factors, Schleswig-Holstein AVV volumes, NREL Battery Failure Databank) all screen out under the target population, and three of the four are not located by their URL (a home page, a host that refused the connection, a host whose name did not resolve).
- **`Hessen` is empty.** Neither the catalogue nor the manifest holds Hessian waste law.
- **`campus` rests on two student compilations** (the Gruppe 3 Abfall-ABC and the AVV workbook). No document published by the university is in either file.
- **`Frankfurt` exists only on disk.** All ten Frankfurt sources are manifest files without a URL; the catalogue contributes none.
- **Munich has 6 sources and Berlin 5**, and 7 of these 11 were added in this pass. Each city has its A-to-Z guide, one sorting sheet and its statutes; Berlin also has the open-data copy of the guide, Munich a small open dataset of recycling centres.
- **`catalogue` is carried by `none`**: 22 product and substance catalogues, 12 of them borderline.

### Numeric tabular sources, all screens (distinct)

| row | source_id | name | jurisdiction | screen | access |
|---|---|---|---|---|---|
| 9 | 713e2d8754b8 | Baustoffrecycling Bayern EBV Tables | other municipality or state | out | ok (200) |
| 21 | 70fb412cf0dd | Abfall+ / Abfall.io | unclear | unclear | ok (200) |
| 42 | dd3e877daf32 | Beton Eisack: Gewichtstabelle 2021 (bulk densities) | none | out | error 404 |
| 80 | 3ab2f940a665 | Destatis (Abfallbilanz) | federal | out | ok (200) |
| 110 | eb9a01883fcd | EPA Volume-to-Weight Factors | none | out | ok (200) |
| 129 | 9c51aa94bd3a | StrlSchV Anlage 4 (Radioactive Clearance) | federal | out | ok (200) |
| 194 | 444e49a70abe | Mannheim open data: Altkleidercontainer (used-clothes contai | other municipality or state | out | ok (200) |
| 198 | cat-mymuell-de-01 | MyMüll.de | other municipality or state | out | ok (200) |
| 206 | bd781aeebefd | NREL Battery Failure Databank | none | out | unreachable |
| 208 | 9b4c51e7657c | Aachen Schadstoffmobil | other municipality or state | out | ok (200) |
| 213 | 169e0ad0774c | SH Open Data | other municipality or state | out | unreachable |
| 214 | 589e81392503 | Würzburg open data: Abfallkalender (collection calendar) | other municipality or state | out | ok (200) |
| 242 | c6fb9149eed9 | Bayerisches Landesamt für Statistik | other municipality or state | out | ok (200) |
| 264 | cat-ur-ch-01 | Kanton Uri: Umrechnungsfaktoren für mineralische Bauabfälle  | none | out | ok (200) |
| 275 | a72ff03aacf2 | Urban Mine Platform (ProSUM) | none | out | ok (200) |
| 276 | 0ac116ac8324 | City of Vienna MA 22 | none | out | ok (200) |
| 288 | da5e1083e9c8 | WRAP: Material bulk densities (report) | none | out | ok (200) |
| 310 | a9bd89a28b57 | Hamburg geodata service: waste and recycling infrastructure  | other municipality or state | out | error 400 |
| 315 | be8614c7e64e | Offene Daten Köln: Schadstoffmobil | other municipality or state | out | error 404 |

Row 21 (the Abfall+ interface) is the one numeric candidate that might be in scope: the research notes claim that FES uses AbfallPlus. This is unverified.

## Authority class (distinct sources)

| class | all | in or contrast |
|---|---|---|
| A | 12 | 7 |
| B | 5 | 5 |
| C | 120 | 23 |
| D | 154 | 19 |
| unclear | 6 | 1 |

## Access status (all rows)

| status | rows | of these in or contrast (distinct) |
|---|---|---|
| ok (200) | 251 | 35 |
| error 403 | 31 | 3 |
| error 404 | 22 | 1 |
| on disk | 20 | 14 |
| unreachable | 13 | 0 |
| redirect -> <other host> | 4 | 1 |
| needs login | 2 | 0 |
| error 400 | 2 | 1 |
| error 503 | 1 | 0 |
| error 406 | 1 | 0 |
| error 502 | 1 | 0 |

## Rows that could not be classified

Ten sources that are in scope, or might be, have a column left `unclear`. A further 66 sources are screened out and have one column `unclear`, mostly because no modality stratum fits images, software and lexical networks, or because the URL is only a site root; they are grouped in the appendix.

### Could not be classified, and not screened out (distinct sources)

| row | source_id | name | modality | class | jurisdiction | screen | what is open |
|---|---|---|---|---|---|---|---|
| 20 | f9d011b9e359 | Abfall+ widget: Abfall-ABC of customer 363 (m | tabular-categorical | C | unclear | unclear | jurisdiction unclear: customer 363 is not named, the widget loads its content by script; screen: would be an operator's Abfall-ABC, but the municipality behind customer 363 is not identified, opening the widget in a browser would decide |
| 21 | 70fb412cf0dd | Abfall+ / Abfall.io | numeric tabular | C | unclear | unclear | jurisdiction unclear: serves many municipalities, Frankfurt not confirmed; screen: collection schedules of many municipalities, in scope only if FES Frankfurt data is served here, the research notes claim FES uses AbfallPlus (unverified) |
| 69 | 3a59c3f4334a | EU Open Data Portal (EWC) | tabular-categorical | C | EU | unclear | screen: would be in scope if it is the EU List of Waste that the AVV transposes, the URL is only the portal root and 'Eurostat EWC' may mean the statistical classification instead |
| 100 | 3593b69ac04e | EEA EWC/LoW List | tabular-categorical | C | EU | unclear | screen: would be in scope if the EU List of Waste is published there, the URL is only the site root |
| 105 | c36bf322750e | Panasonic Technical Data | unclear | D | none | unclear | modality unclear: the URL is only a site root, the document the catalogue names would decide; screen: battery specifications would count as a product catalogue if they cover consumer cells, the catalogue says industrial and the URL is the site root |
| 190 | 0ced73338454 | Legislation.gov.uk EWC | tabular-categorical | A | EU | unclear | screen: EU waste-list decision 2000/532/EC in the UK statute database (retained law, may diverge), the EUR-Lex text would be in scope |
| 211 | 683e71b51e40 | EU Vocabularies (SKOS) | tabular-categorical | C | EU | unclear | screen: would be in scope if the EU List of Waste is published there, the URL is the landing page of all controlled vocabularies |
| 212 | 6c16c15ada75 | Munich AWM Wertstoffhöfe | unclear | C | Munich | contrast | modality unclear: about a dozen recycling centres with coordinates, opening hours and accepted fractions, numeric if coordinates and hours dominate, categorical if fractions do |
| 308 | f1df9ba23ec5 | DIN CERTCO: certified compostable products (S | unclear | D | none | unclear | modality unclear: the page answered 404, a list of certified products would be a catalogue; screen: a list of certified compostable products would count as an eco-labelled product catalogue, the page is gone |
| added | cat-local-01 | Abfall-ABC (merged list) | tabular-categorical | unclear | Frankfurt | in | authority class unclear: C if it is a faithful copy of the FES Abfall-ABC, D as an undocumented merge by an unknown producer (README: no code or document records who produced it) |

## Dead links and other access problems

On 2026-11-12, 36 rows were dead or unreachable: 22 answered 404, 9 could not be reached (name resolution, refused connection, timeout or a broken certificate) and 5 answered 400, 406, 502 or 503. A further 4 rows are URL templates that cannot be requested, 31 answered 403 (in most cases bot protection, so not counted as dead), 4 redirect to another host and 2 need a login. The lists are in the appendix.

5 of the dead rows are screened `in` or `contrast`:

- row 32 (AWM Munich Abfall-ABC): error 404, a duplicate row
- row 67 (Energizer safety data sheet: alkaline batteries (example file)): error 404, a duplicate row
- row 126 (BattG (Batteriegesetz)): error 404
- row 224 (PubChem PUG REST): error 400
- row 311 (Munich geoportal WFS: AWM Wertstoffhöfe (GeoJSON request)): error 404, a duplicate row

Row 126 is the one that matters: the Batteriegesetz page is gone (see point 7 in the section on the README). Rows 32 and 311 are stale Munich URLs whose canonical rows answer. Row 67 is one example file of the Energizer library. Row 224 is an API base URL that answers but expects parameters; the same holds for rows 220 and 310. Row 262 is a namespace URI from a Turtle example in the research report; its host does not exist. Rows 164 and 165 sit on a host whose name did not resolve.

## Added rows

24 rows were added: 14 for manifest sources that had no catalogue row (one row per source, covering 20 of the 25 files; the other 5 files belong to sources the catalogue already lists), and 10 for the gaps named in the task (KrWG, EU Ecolabel, LAGA M20, four Munich rows, three Berlin rows). The table is in the appendix.

How the contrast-city rows were found and verified (all on 2026-11-12):

- **Munich, A-to-Z guide.** The catalogue URL `awm-muenchen.de/abfallarten` answers 404. The AWM home page links the Abfalllexikon at `https://www.awm-muenchen.de/abfall-entsorgen/abfalllexikon` (HTML, 278 entry links in the page source).
- **Munich, statutes.** A web search led to versioned pages of the city's Stadtrecht. The unversioned pages `.../stadtrecht/vorschrift/270.html`, `271.html` and `277.html` list the current version ("gültig seit 31.01.2024" for 270 and 271, "gültig seit 31.07.2026" for 277) and all earlier ones; each has a PDF twin (`270.pdf` and so on). Munich has no single Abfallsatzung: the general statute (270), the household-waste statute (271) and the statute on bulky waste, recyclables and problem waste (277) were added as three rows.
- **Berlin, A-to-Z guide.** The catalogue URL `bsr.de/abfall-abc-20563.php` redirects to `https://www.bsr.de/abfall-abc`, a search form. Its open-data twin (row 77, CC BY) was read in a browser: the dataset page exists, but its two JSON resource links answered 404.
- **Berlin, state law.** The portal `gesetze.berlin.de` answers 200 with a script shell for any id, so the status proves nothing. The document was opened in a headless browser; the portal's own permalink for the current version is `https://gesetze.berlin.de/perma?j=KrW%2FAbfG_BE`.
- **Berlin, statute.** `https://www.bsr.de/rechtsgrundlagen-datenschutz` links the Abfallwirtschaftssatzung (PDF, 21 pages, from the Amtsblatt of 27 December 2024). No "Leistungsbedingungen" are listed there; the terms and conditions on that page cover commercial waste only.

Seen while verifying but **not added**, because the task did not ask for them: the BSR Abfallgebührensatzung 2025/26 (`bsr.de/assets/downloads/abfallgebuehrensatzung.pdf`, 14 pages, fees), the BSR rules for recycling centres (linked on the same page), and the Munich statute on commercial and construction waste (seen in the search results, not opened). A fee statute per city would be the most direct way to fill `numeric tabular`.

## Where the catalogue and the manifest differ from the README

1. **Distinct sources.** The README says "about 256 distinct real sources". This pass finds 273 among the catalogue rows (1 duplicate by URL, 50 by judgement). 256 is exactly the number of catalogue rows whose URL also occurs in the quellenliste.
2. **Origin.** The README describes the catalogue as merged from the deep-research reports in `discovered_sources.md`. 51 of the 324 rows come from other files: 25 from `datasources.md` and 26 from four design notes under `docs/`.
3. **Files on disk.** The manifest has 27 rows but 25 files. `odenet-oewn` and `eu-ecolabel` have no file; they were fetched at run time. All 25 files were present on the machine this pass ran on, including the four large ones that `sources/large/README.md` says exist only on the desktop machine.
4. **Frankfurt.** The README says none of the imported Frankfurt sources is in the catalogue. The gap is wider: no catalogue row names Frankfurt, FES or Frankfurt UAS at all, and none holds Hessian law. Hessen appears once, as the coverage an app vendor claims (row 199), and Uni Gießen (rows 257, 258) is a Hessian university.
5. **Numeric candidates.** See the cross-table: the four candidates of the README's "Gaps" section do not belong to the target population as it is now defined.
6. **Reference benchmark** (no contradiction, but easy to misread). The README lists the official AVV text, EU Vocabularies, the EEA CSV, the KrWG and GESTIS as not on disk. The catalogue has candidate rows for the AVV (row 1), the EEA list (row 100), EU Vocabularies (row 211) and GESTIS (row 130), but rows 100 and 211 point to a site root and a landing page, so the documents themselves are not located.
7. **BattG.** `gesetze-im-internet.de/battg_2009/` answers 404, while `.../battdg/` ("Gesetz zur Durchführung der Verordnung (EU) 2023/1542 betreffend Batterien und Altbatterien") answers 200. The Batteriegesetz appears to have been replaced; this needs a check, because the authority-class definition names the BattG.
8. **Names that promise more than the URL holds.** Row 87 is named "PPWR (EU 2025/40)" but points to a chamber-of-commerce leaflet. Row 267 is named "VerpackG" but points to a commercial information site that now redirects to a dead page. Row 127 is named "PCP-Verbotsverordnung (1989)" but points to the Chemikalien-Verbotsverordnung, at a URL that has moved. Neither the Packaging Act nor the ElektroG is in the catalogue as a statute.
9. **Quellenliste.** Besides the empty verification columns the README mentions, its URLs are damaged (lost hyphens, 64 stray quotes), and for 24 rows it holds more than one id for the same URL.

Confirmed as the README says: 324 rows on 243 hosts; Format, License and Relevance empty in about 60 % of the rows; 8 rows with a licence name in the License column; EU Ecolabel and LAGA M20 lost because their research entries pointed to bare hosts (`data.europa.eu`, `laga-online.de`) shared with other rows; KrWG absent.

## What a human must still check

### 1. Licences first

Of the 55 distinct sources screened `in` or `contrast`, 39 have no licence on record. The appendix lists all 55 with what is recorded.

- **FES material.** Seven on-disk sources carry the manifest's "check NDA scope". The scope of the NDA of 10 November 2025 decides whether they can be used and published at all.
- **Munich.** The AWM site notice allows nothing beyond private use without consent; this covers the Abfalllexikon and, presumably, the sorting flyer. The city's site terms name statutes as official works under § 5 UrhG. The open dataset of recycling centres is under Datenlizenz Deutschland Namensnennung 2.0.
- **Berlin.** The BSR site notice asks to be contacted before reuse. The same content is offered under CC BY on the open-data portal, but the JSON files were not retrievable. The state-law portal declares a text-and-data-mining reservation.
- **Catalogue-stated licences** (ODbL, MIT, CC BY, CC0, Public domain; 13 rows) are LLM claims. Only row 77 was read again on the source page.
- "Public" in 79 catalogue rows is not a licence. Statutes and official registers still need a stated basis for reuse.

### 2. The screen

- The 40 decisions marked `borderline` (16 in, 24 out) and the 8 rows with screen `unclear`.
- Whether the strict reading is the intended one: federal and Länder guidance out, substance databases in. Dropping the borderline rows changes the in-scope total from 55 to 39.
- **Reference-benchmark material is screened `in`** (AVV, KrWG, Frankfurt Abfallsatzung, GESTIS). The README wants it kept out of the sample. The screen column does not encode this; the hint is in `screen_reason` or `notes`.

### 3. Classes

- Authority class of the merged FES Abfall-ABC (`cat-local-01`): C if it is a faithful copy, D as an undocumented merge.
- Whether take-back schemes and the dual systems count as competent operators (rows 158, 197, 245).
- Whether technical rules (TRBA, TRGS) are A or C.
- Modality of the Munich recycling-centre dataset (row 212) and of the sorting sheets (rows 8 and 10).
- Jurisdiction `campus` for the two student compilations, which also carry FES routes and federal AVV codes.

### 4. Duplicates and repairs

- The 50 duplicates by judgement (list in the appendix).
- The 114 replaced names.

### 5. Access

- The 31 rows that answered 403, in a browser. In scope: rows 3 and 4 (ECHA) and row 77 (read in a browser, see above).
- Row 126 (BattG, see above) and row 311 (the Munich WFS request, 404).
- The 81 rows whose URL is a site root: the named document still has to be located.

### 6. Gaps this pass did not fill

Hessian waste law; the ElektroG and the Packaging Act as statutes; the PPWR text; the city's and FES's own web pages (the online Abfall-ABC, the collection calendar, the fee statute); documents published by Frankfurt UAS; a fee statute or collection calendar for any of the three cities.

## What was not done

- The pages behind the catalogue rows were not read, with the exception of the Munich and Berlin pages named above. Classes rest on the catalogue text, the research notes and page titles.
- Licences were looked up on the source page for 8 rows only.
- The 403 rows were not re-checked in a browser, except the Berlin open-data page.
- The local files were checked for presence, not for content; only the key structure of the facility-map JSON was read.
- No source was added beyond the list in the task.
- Two pages (`gesetze.berlin.de`, `daten.berlin.de`) were opened in a headless browser with its stock User-Agent, because the plain client could not see them.

## Appendix: lists

### Added rows

| source_id | manifest_id | name | added because | screen | access |
|---|---|---|---|---|---|
| `cat-gesetze-im-internet-de-01` |  | Kreislaufwirtschaftsgesetz (KrWG) | known gap: README says the KrWG is missing from the catalogue | in | ok (200) |
| `cat-apps-data-env-service-ec-europa-eu-01` | eu-ecolabel | EU Ecolabel product catalogue | known gap: imported in February 2026 (manifest id eu-ecolabel), dropped from the catalogue by URL de-duplication | in | ok (200) |
| `cat-laga-online-de-01` |  | LAGA-Mitteilung 20: Anforderungen an die stoffliche Verwertung von mineralischen | known gap: README says LAGA M20 (Tier 1) was dropped by URL de-duplication | out | ok (200) |
| `cat-awm-muenchen-de-02` |  | AWM Abfalllexikon (A-to-Z disposal guide, Munich) | contrast city Munich: official A-to-Z disposal guide, the catalogue URL (row 32) is dead | contrast | ok (200) |
| `cat-stadt-muenchen-de-01` |  | Allgemeine Abfallsatzung der Landeshauptstadt München (Stadtrecht 270) | contrast city Munich: current general waste statute | contrast | ok (200) |
| `cat-stadt-muenchen-de-02` |  | Hausmüllentsorgungssatzung der Landeshauptstadt München (Stadtrecht 271) | contrast city Munich: current household-waste statute | contrast | ok (200) |
| `cat-stadt-muenchen-de-03` |  | Hausratsperrmüll-, Wertstoff- und Problemmüllsatzung der Landeshauptstadt Münche | contrast city Munich: current statute on bulky waste, recyclables and problem waste | contrast | ok (200) |
| `cat-bsr-de-02` |  | BSR Abfall-ABC (A-to-Z disposal guide, Berlin) | contrast city Berlin: official A-to-Z disposal guide at its current URL (row 51 holds the old one) | contrast | ok (200) |
| `cat-gesetze-berlin-de-01` |  | Kreislaufwirtschafts- und Abfallgesetz Berlin (KrW-/AbfG Bln) | contrast city Berlin: state waste act | contrast | ok (200) |
| `cat-bsr-de-03` |  | Abfallwirtschaftssatzung der Berliner Stadtreinigungsbetriebe (Bekanntmachung vo | contrast city Berlin: current BSR waste statute | contrast | ok (200) |
| `cat-local-01` | fes-abfall-abc | Abfall-ABC (merged list) | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-02` | fes-facility-map | Facility map (disposal points with addresses and opening hours) | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-03` | fes-sorting-flyer | Was kommt wo hinein? Abfall richtig trennen – Wertstoffe sichern | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-04` | fes-bin-alltagscontainer; fes-bin-alltagscontainer-pdf | Alltagscontainer (glass container) | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-05` | fes-bin-altpapiertonne; fes-bin-altpapiertonne-pdf | Altpapiertonne (paper bin) | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-06` | fes-bin-restabfalltonne; fes-bin-restabfalltonne-pdf | Restabfalltonne (residual-waste bin) | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-07` | fes-bin-verpackungstonne; fes-bin-verpackungstonne-pdf | Verpackungstonne (packaging bin) | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-08` | frankfurt-abfallsatzung | Satzung der Stadt Frankfurt am Main über die Vermeidung, Verwertung und Beseitig | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-09` | frankfurt-zero-waste-city | Auf dem Weg zur Zero Waste City: Abfallvermeidung – Recycling – Kreislaufwirtsch | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-10` | recyclingzentrum-abfall-abc | Abfall-ABC Recyclingzentrum Frankfurt | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-11` | group3-abfall-abc; group3-abfall-abc-csv; group3-abfall-bereinigt | Abfall-ABC, final format (FES and FRA UAS routes) | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-12` | avv-workbook | AVV codes with Abfall-ABC items and FRA UAS campus routes | on disk (manifest), no catalogue row | in | on disk |
| `cat-local-13` | off-materials | Packaging-material taxonomy (90 tags with default bins) | on disk (manifest), no catalogue row | out | on disk |
| `cat-local-14` | concepts-derived | Concept clusters from Wiktionary and OdeNet (derived) | on disk (manifest), no catalogue row | out | on disk |

### Licences of the sources screened in or contrast

| jurisdiction | row or id | source | licence as recorded | basis |
|---|---|---|---|---|
| Frankfurt | `cat-local-01` | Abfall-ABC (merged list) | none recorded; FES content, check NDA scope | manifest |
| Frankfurt | `cat-local-02` | Facility map (disposal points with addresses and opening hou | none recorded; FES content, check NDA scope | manifest |
| Frankfurt | `cat-local-03` | Was kommt wo hinein? Abfall richtig trennen – Wertstoffe sic | none recorded; FES content, check NDA scope | manifest |
| Frankfurt | `cat-local-04` | Alltagscontainer (glass container) | none recorded; FES content, check NDA scope | manifest |
| Frankfurt | `cat-local-05` | Altpapiertonne (paper bin) | none recorded; FES content, check NDA scope | manifest |
| Frankfurt | `cat-local-06` | Restabfalltonne (residual-waste bin) | none recorded; FES content, check NDA scope | manifest |
| Frankfurt | `cat-local-07` | Verpackungstonne (packaging bin) | none recorded; FES content, check NDA scope | manifest |
| Frankfurt | `cat-local-08` | Satzung der Stadt Frankfurt am Main über die Vermeidung, Ver | official statute, likely an amtliches Werk under § 5 UrhG (inference) | manifest |
| Frankfurt | `cat-local-09` | Auf dem Weg zur Zero Waste City: Abfallvermeidung – Recyclin | none recorded | manifest |
| Frankfurt | `cat-local-10` | Abfall-ABC Recyclingzentrum Frankfurt | none recorded | manifest |
| campus | `cat-local-11` | Abfall-ABC, final format (FES and FRA UAS routes) | student work, no licence | manifest |
| campus | `cat-local-12` | AVV codes with Abfall-ABC items and FRA UAS campus routes | student work, no licence; AVV text itself is an official statute | manifest |
| federal | 1 | AVV (Abfallverzeichnis-Verordnung) | none recorded | catalogue says 'Public' (access term) |
| federal | 2 | Altholzverordnung (AltholzV) | none recorded | catalogue says 'Public' (access term) |
| federal | 126 | BattG (Batteriegesetz) | none recorded | catalogue says 'Public' (access term) |
| federal | 130 | GESTIS Substance DB | none recorded | nothing stated |
| federal | `cat-gesetze-im-internet-de-01` | Kreislaufwirtschaftsgesetz (KrWG) | none recorded | nothing stated |
| EU | 3 | ECHA SCIP Database | none recorded | catalogue says 'Public' (access term) |
| EU | 4 | ECHA CHEM / REACH | none recorded | catalogue says 'Public' (access term) |
| EU | 68 | ECHA C&L Inventory | none recorded | catalogue says 'Public' (access term) |
| EU | 114 | EU Decision 97/129/EC | none recorded | catalogue says 'Public' (access term) |
| EU | 115 | EU Battery Regulation 2023/1542 | none recorded | catalogue says 'Public' (access term) |
| Munich | 10 | AWM Munich Trennliste | none recorded | catalogue says 'Public' (access term) |
| Munich | 212 | Munich AWM Wertstoffhöfe | Datenlizenz Deutschland Namensnennung 2.0 (dl-by-de) | source page (quote in notes) |
| Munich | `cat-awm-muenchen-de-02` | AWM Abfalllexikon (A-to-Z disposal guide, Munich) | no open licence: site copyright notice, reuse only with consent | source page (quote in notes) |
| Munich | `cat-stadt-muenchen-de-01` | Allgemeine Abfallsatzung der Landeshauptstadt München (Stadt | no licence stated: site terms name statutes as official works under § 5 UrhG | source page (quote in notes) |
| Munich | `cat-stadt-muenchen-de-02` | Hausmüllentsorgungssatzung der Landeshauptstadt München (Sta | no licence stated: site terms name statutes as official works under § 5 UrhG | source page (quote in notes) |
| Munich | `cat-stadt-muenchen-de-03` | Hausratsperrmüll-, Wertstoff- und Problemmüllsatzung der Lan | no licence stated: site terms name statutes as official works under § 5 UrhG | source page (quote in notes) |
| Berlin | 8 | BSR Abfalltrennung Guides | none recorded | catalogue says 'Public' (access term) |
| Berlin | 77 | Berlin BSR Open Data | CC BY (Creative Commons Attribution) | catalogue License column ('CC BY'), read again on the source page (quote in notes) |
| Berlin | `cat-bsr-de-02` | BSR Abfall-ABC (A-to-Z disposal guide, Berlin) | no open licence: site copyright notice, ask BSR before reuse | source page (quote in notes) |
| Berlin | `cat-gesetze-berlin-de-01` | Kreislaufwirtschafts- und Abfallgesetz Berlin (KrW-/AbfG Bln | no licence stated: the portal declares a text-and-data-mining reservation | source page (quote in notes) |
| Berlin | `cat-bsr-de-03` | Abfallwirtschaftssatzung der Berliner Stadtreinigungsbetrieb | none recorded | nothing stated |
| none | 6 | Open Products Facts | ODbL | catalogue License column (LLM-stated, unverified) |
| none | 34 | Barcode Lookup API | none recorded | nothing stated |
| none | 45 | Blue Angel (Blauer Engel) | none recorded | catalogue says 'Free (registration)' (access term) |
| none | 57 | ChemSpider | none recorded | catalogue says 'Free registration' (access term) |
| none | 60 | CAS Common Chemistry | none recorded | catalogue says 'Free (no bulk)' (access term) |
| none | 66 | Energizer Technical Library | none recorded | catalogue says 'Free' (access term) |
| none | 93 | DSMZ (German Collection of Microorganisms) | none recorded | nothing stated |
| none | 95 | Duracell SDS Library | none recorded | catalogue says 'Free' (access term) |
| none | 97 | ChEBI | none recorded | nothing stated |
| none | 107 | EPA CompTox / CPDat | none recorded | catalogue says 'Public' (access term) |
| none | 113 | EPREL | none recorded | catalogue says 'Public (QES for bulk)' (access term) |
| none | 150 | BMECat.net GitHub | none recorded | nothing stated |
| none | 155 | olivere/bmecat (Go) | none recorded | nothing stated |
| none | 173 | Icecat Open Catalog | none recorded | catalogue says 'Free tier' (access term) |
| none | 176 | TCO Certified | none recorded | nothing stated |
| none | 223 | PBSeasy (PBS Network) | none recorded | catalogue says 'Commercial B2B (paid)' (access term) |
| none | 224 | PubChem PUG REST | none recorded | catalogue says 'Open (5 req/s)' (access term) |
| none | 273 | NIST Chemistry WebBook | Public domain | catalogue Description column (LLM-stated, unverified) |
| none | 283 | Open Beauty Facts | ODbL | catalogue License column (LLM-stated, unverified) |
| none | 284 | Open Food Facts product database (data and export page) | ODbL / ODbL (inference) | manifest (for the local copy) |
| none | 286 | Open Pet Food Facts | ODbL | catalogue License column (LLM-stated, unverified) |
| none | `cat-apps-data-env-service-ec-europa-eu-01` | EU Ecolabel product catalogue | not recorded; check upstream | manifest |

### Screened out, with one column left `unclear` (distinct sources)

| reason | rows | orig_row |
|---|---|---|
| modality: the URL is only a site root | 16 | 5, 11, 19, 44, 53, 111, 119, 121, 122, 222, 238, 243, 257, 297, 301, 307 |
| modality: no stratum fits (image data) | 14 | 7, 17, 75, 147, 148, 149, 179, 180, 181, 246, 260, 261, 271, 300 |
| modality: no stratum fits (software or service) | 14 | 83, 84, 109, 135, 136, 139, 143, 145, 151, 154, 226, 227, 237, 321 |
| modality: no stratum fits (lexical network) | 8 | 33, 62, 152, 153, 182, 209, 219, added |
| authority class unclear: C if take-back scheme operators count as competent operators, otherwise D | 2 | 158, 245 |
| modality unclear: format specification plus example files, whether the examples hold real product entries would decide | 1 | 48 |
| modality unclear: the page is gone, a statistics query would be numeric | 1 | 74 |
| authority class unclear: the catalogue calls it an EPA portal, the | 1 | 164 |
| modality unclear: portal root, the content per product group would decide | 1 | 171 |
| authority class unclear: C if the dual systems count as competent operators, otherwise D | 1 | 197 |
| modality unclear: map data, tags and coordinates | 1 | 217 |
| modality unclear: knowledge graph, no stratum fits | 1 | 229 |
| modality unclear: a matrix of fibre ratings, not inspected | 1 | 248 |
| modality unclear: nothing is published at this URI; authority class unclear: not a source; jurisdiction unclear: not a source | 1 | 262 |
| modality unclear: one API serves collection dates (numeric) and site lists (categorical) | 1 | 294 |
| modality unclear: the PDF could not be opened (403), a list or a text would decide | 1 | 309 |
| modality unclear: the page could not be opened (403) | 1 | 317 |

### Dead links (error 404/410, other 4xx/5xx except 403, unreachable)

| row | source_id | status | screen | url |
|---|---|---|---|---|
| 18 | 98bc2fc21f49 | error 404 | out | https://www.alnatura.de/de-de/magazin/oekologisch-sinnvolle-verpackung/faq-verpackung/verpackungswissen_r |
| 24 | 56f712f52fa8 | error 404 | out | https://api.madaster.com/v4.0/ |
| 30 | b257f66855a6 | error 404 | out | https://assets.publishing.service.gov.uk/ |
| 32 | cat-awm-muenchen-de-01 | error 404 | contrast | https://www.awm-muenchen.de/abfallarten |
| 42 | dd3e877daf32 | error 404 | out | https://www.beton-eisack.it/_default_upload_bucket/Gewichtstabelle%202021_1.pdf |
| 55 | 16ee713cfc4f | error 404 | out | https://catalog.data.gov/dataset/unspsc-codes-93778 |
| 67 | e7a85b89a947 | error 404 | in | https://data.energizer.com/wp-content/uploads/2023/01/Alk-Min-AIS-SDS-01-23-Energizer.pdf |
| 73 | 1d9c57adcff2 | error 404 | out | https://data.gov.scot/sepa/waste/ |
| 74 | 083598401823 | error 404 | out | https://data.gov.scot/sepa/waste/wfas-qr.html |
| 76 | 8596eccff2fc | error 503 | out | https://datalab.dbfz.de/ontologies/ |
| 83 | 373806adb6f0 | error 404 | out | https://developer.fedex.com/api/ |
| 104 | cat-energy-panasonic-com-01 | error 404 | out | https://energy.panasonic.com/dam/master/pdf/eu/material/Panasonic_Lithium_Handbook_2025.pdf |
| 118 | 5aaa5a0f4955 | error 404 | out | https://ewccodes.com/api/ewc?code=170101 |
| 126 | f79507e940a9 | error 404 | in | https://www.gesetze-im-internet.de/battg_2009/ |
| 127 | a92354a5cfb5 | error 404 | out | https://www.gesetze-im-internet.de/chemverbotsv/ |
| 164 | cf95496e17a3 | unreachable | out | https://www.hazwasteportal.org/ |
| 165 | 8c7764802594 | unreachable | out | https://www.hazwasteportal.org/search.php |
| 167 | 336c0e719151 | unreachable | out | https://www.hpe.com/global/livingprogress/environmentalprogress/productdata/ |
| 206 | bd781aeebefd | unreachable | out | https://www.nrel.gov/transportation/battery-failure.html |
| 213 | 169e0ad0774c | unreachable | out | https://opendata.schleswig-holstein.de/ |
| 220 | af92053c73df | error 406 | out | https://overpass-api.de/api/interpreter |
| 224 | a7c17d0607ff | error 400 | in | https://pubchem.ncbi.nlm.nih.gov/rest/pug |
| 239 | cat-stadtreinigung-hamburg-01 | error 404 | out | https://www.stadtreinigung.hamburg/abfall-abc/ |
| 247 | e0ea63a3e479 | error 404 | out | https://tes.bam.de/TES/Navigation/EN/DGG-Database/Products/ |
| 259 | bc6337116a47 | error 404 | out | https://www.uni-heidelberg.de/md/zentral/universitaet/beschaeftigte/service/sicherheit/bt-26.pdf |
| 262 | eb7f331dbefe | unreachable | out | http://university-waste.de/ontology |
| 267 | 7d5197d2a101 | error 404 | out | https://verpackungsgesetz-info.de/en/ |
| 271 | e198a9985e25 | unreachable | out | https://wadaba.pcz.pl/ |
| 298 | 381cb4800ecb | error 502 | out | https://api.conceptnet.io/c/de/abfall?limit=10 |
| 301 | cc5dc3c340df | unreachable | out | https://www.awb-landkreis-karlsruhe.de/ |
| 306 | f6192c5f3335 | unreachable | out | https://data.nrel.gov/submissions/153 |
| 308 | f1df9ba23ec5 | error 404 | unclear | https://www.dincertco.de/din-certco/en/main-navigation/products-and-services/certification-of-products/pa |
| 310 | a9bd89a28b57 | error 400 | out | https://geodienste.hamburg.de/HH_WFS_abfall_recycling |
| 311 | cat-geoportal-muenchen-de-01 | error 404 | contrast | https://geoportal.muenchen.de/geoserver/opendata/ows?service=WFS&request=GetFeature&typeName=opendata:vab |
| 312 | cat-gist-githubusercontent-com-01 | error 404 | out | https://gist.githubusercontent.com/bennokr/6d5f1e556a78148ff3087c4fd4618781/raw/ewc.ndjson |
| 315 | be8614c7e64e | error 404 | out | https://offenedaten-koeln.de/dataset/schadstoff-mobil-k |

### URL templates that cannot be requested

| row | source_id | url |
|---|---|---|
| 25 | 03ab781af885 | https://api.madaster.com/v4.0/buildings/{id}/materials |
| 131 | 0abb44a68bf1 | https://gestis-database.dguv.de/data?name={ZVG_NUMBER} |
| 294 | 4ec36fac8d69 | https://{region}-abfallapp.regioit.de/ |
| 295 | 4920c231a9f8 | https://{region}-abfallapp.regioit.de/abfall-app-{region}/rest |

### Answered 403 (often bot protection; open in a browser before calling them dead)

| row | source_id | screen | url |
|---|---|---|---|
| 3 | 4110af92f757 | in | https://echa.europa.eu/scip |
| 4 | c272b8dbd798 | in | https://chem.echa.europa.eu/ |
| 7 | cat-universe-roboflow-com-01 | out | https://universe.roboflow.com/ |
| 35 | d4fd16466b61 | out | https://www.baua.de/DE/Angebote/Regelwerk/TRBA/pdf/TRBA-466.pdf |
| 36 | 11dcbd036b66 | out | https://www.baua.de/DE/Angebote/Regelwerk/TRBA/TRBA-462 |
| 37 | e1eacdddac7c | out | https://www.baua.de/DE/Angebote/Regelwerk/TRBA/TRBA-460 |
| 38 | 17805aca349f | out | https://www.baua.de/DE/Angebote/Regelwerk/TRBA/pdf/TRBA-250.pdf |
| 54 | e243d55a19e6 | out | https://calepa.ca.gov/sitecleanup/corteselist/ |
| 64 | 21f07c26adab | out | https://css.umich.edu/publications/research-publications/life-cycle-assessment-office-furniture-products |
| 77 | 4025d958a241 | contrast | https://daten.berlin.de/datensaetze/bsr-recyclingh-fe-und-abfall-abc |
| 78 | 4538bc40a18a | contrast | https://daten.berlin.de/api/3/action/package_show?id=bsr-recyclingh-fe-und-abfall-abc |
| 79 | 0511fc285e52 | out | https://www.dell.com/support/kbdoc/en-us/000144813/recycling-dell-monitors-how-to-get-disassembly-instruc |
| 84 | 73cded2d8b6f | out | https://developer.ups.com/api/reference |
| 105 | c36bf322750e | unclear | https://energy.panasonic.com/ |
| 160 | fe116bd7c496 | out | https://www.gs1.org/ |
| 161 | de1113cc2e67 | out | https://www.gs1.org/standards/gs1-digital-link |
| 168 | 0e8853911643 | out | https://hsrm.umn.edu/ |
| 193 | cat-makeitfrom-com-01 | out | https://www.makeitfrom.com/ |
| 195 | cat-materialsproject-org-01 | out | https://materialsproject.org/ |
| 196 | d856d8c1d02e | out | https://www.matweb.com/ |
| 204 | 1d3676fbd841 | out | https://next-gen.materialsproject.org/ |
| 233 | cat-researchgate-net-01 | out | https://www.researchgate.net/publication/385081535 |
| 250 | de05e321d55d | out | https://www.tuvsud.com/en-us/-/media/regions/us/pdf-files/whitepaper-report-e-books/tuvsud_overview-of-eu |
| 253 | cat-undp-org-01 | out | https://www.undp.org/unspsc |
| 254 | 4ba50f9c9167 | out | https://unece.org/transport/dangerous-goods/ghs-pictograms |
| 255 | c2535670e90c | out | https://unece.org/transport/dangerous-goods |
| 260 | 1f44fd6e3ca0 | out | https://universe.roboflow.com/pfand48/p48 |
| 261 | 81a069a7a0cb | out | https://universe.roboflow.com/zz-77ljl/bank-bottle |
| 269 | cat-verpackungsregister-org-01 | out | https://www.verpackungsregister.org/fileadmin/files/Mindeststandard/ |
| 309 | db6cc13f789f | out | https://epub.sub.uni-hamburg.de/epub/volltexte/2014/30664/pdf/2_06b.pdf |
| 317 | cat-portal-mytum-de-01 | out | https://portal.mytum.de/archiv/form_gebaeude/form_gebaeude_20030303_113231/20030303_113311/1 |

### Redirects to another host, logins, and deep links that landed on a site root

| row | source_id | status | url |
|---|---|---|---|
| 23 | 6e609cc6824c | needs login | https://api.buildingtransparency.org/api/epds?search=concrete |
| 49 | fc0d1187d8f5 | redirect -> bundesumweltministerium.de | https://www.bmuv.de/ |
| 61 | 73779f747490 | needs login | https://commonchemistry.cas.org/api/detail?cas_rn=67-64-1 |
| 111 | d4f1fe419c2e | redirect -> epba.eu | https://www.epbaeurope.net/ |
| 112 | 432efa9afad8 | ok (200) (landed on the site root) | https://episuite.dev/EpiWebSuite/ |
| 158 | cat-grs-batterien-de-01 | redirect -> grs-pro.de | https://www.grs-batterien.de/fileadmin/Downloads/Sicherheit/GRS_Checkliste_Kennzeichnungspflicht_Sep2023_ |
| 207 | 9c284d1b7064 | ok (200) (landed on the site root) | https://www.oekobaudat.de/api/datasets |
| 223 | cat-pbsnetwork-eu-01 | redirect -> enchase.com | https://pbsnetwork.eu/ |

### Duplicates (row -> canonical row)

| row | name | canonical | canonical name | basis |
|---|---|---|---|---|
| 13 | RegioIT Abfallnavi API, Aachen: disposal sites ( | 12 | RegioIT Abfallnavi API, Aachen: waste fractions  | D2 |
| 22 | Abfall+ API endpoint (index.php) | 21 | Abfall+ / Abfall.io | D2 |
| 23 | EC3 / openEPD API: EPD search (endpoint example) | 52 | OpenEPD / EC3 | D2 |
| 25 | Madaster API: building materials (endpoint templ | 24 | Madaster API | D2 |
| 26 | Materials Project API | 195 | Materials Project | D2 |
| 30 | GOV.UK WM3 Guidance | 31 | Waste classification technical guidance WM3 (PDF | D2 |
| 32 | AWM Munich Abfall-ABC | cat-awm-muenchen-de-02 | AWM Abfalllexikon (A-to-Z disposal guide, Munich | D2 |
| 41 | Beton Eisack (South Tyrol) | 42 | Beton Eisack: Gewichtstabelle 2021 (bulk densiti | D2 |
| 46 | Blauer Engel Fo.KUS | 45 | Blue Angel (Blauer Engel) | D2 |
| 51 | BSR Abfall-ABC (scraping) | cat-bsr-de-02 | BSR Abfall-ABC (A-to-Z disposal guide, Berlin) | D2 |
| 61 | CAS Common Chemistry API: substance detail (endp | 60 | CAS Common Chemistry | D2 |
| 67 | Energizer safety data sheet: alkaline batteries  | 66 | Energizer Technical Library | D2 |
| 78 | Berlin Open Data API: metadata of the BSR datase | 77 | Berlin BSR Open Data | D2 |
| 90 | OpenEPD / EC3 API | 52 | OpenEPD / EC3 | D2 |
| 94 | DSMZ (German Collection of Microorganisms) | 93 | DSMZ (German Collection of Microorganisms) | D2 |
| 112 | EPA EPI Suite, web beta (EpiWebSuite) | 109 | EPA EPI Suite | D2 |
| 118 | ewccodes.com API: code lookup (endpoint example) | 117 | ewccodes.com (EU waste code list with API) | D2 |
| 131 | GESTIS deep link pattern (per substance) | 130 | GESTIS Substance DB | D2 |
| 132 | GESTIS | 130 | GESTIS Substance DB | D2 |
| 137 | TACO Dataset | 246 | TACO Dataset | D2 |
| 141 | bundesAPI/abfallnavi-api | 15 | Abfallnavi API documentation (bund.dev) | D2 |
| 144 | kschmidt-at-dbfz/ewc-onto | 76 | EWC-Onto | D2 |
| 146 | ZeroWaste dataset, code repository | 17 | ZeroWaste dataset (MRF conveyor belt), project p | D2 |
| 160 | GS1 Digital Link | 161 | GS1 Digital Link standard | D2 |
| 165 | HazWaste Portal: search by chemical name | 164 | EPA HazWaste Portal | D2 |
| 169 | Open Food Facts Dataset on Hugging Face | 284 | Open Food Facts product database (data and expor | D2 |
| 170 | Open Food Facts product database, Parquet export | 284 | Open Food Facts product database (data and expor | D2 |
| 174 | Icecat Open Catalog | 173 | Icecat Open Catalog | D2 |
| 183 | kaikki.org raw data downloads page | 182 | German Wiktionary extract (kaikki.org, de-extrac | D2 |
| 199 | MyMüll.de / JumoMind | 198 | MyMüll.de | D2 |
| 204 | Materials Project (next-gen site) | 195 | Materials Project | D2 |
| 220 | Overpass API (OpenStreetMap query endpoint) | 217 | OpenStreetMap | D2 |
| 221 | overpass turbo (web front end for OpenStreetMap  | 217 | OpenStreetMap | D2 |
| 228 | Wikidata Query Service (SPARQL endpoint) | 229 | Wikidata SPARQL | D2 |
| 230 | Open Food Facts packaging materials taxonomy (ra | 140 | Open Food Facts Taxonomies | D2 |
| 241 | Open Food Facts product database, JSONL export ( | 284 | Open Food Facts product database (data and expor | D2 |
| 247 | BAM GEFAHRGUT | 86 | BAM Dangerous Goods Database (DGG) | D2 |
| 263 | Canton Uri (Switzerland) | 264 | Kanton Uri: Umrechnungsfaktoren für mineralische | D2 |
| 272 | NIST Chemistry WebBook (chemistry section) | 273 | NIST Chemistry WebBook | D2 |
| 279 | OpenStreetMap wiki: tag amenity=recycling | 278 | OpenStreetMap wiki: tag amenity=recycling | D1 |
| 280 | Wikidata API: entity labels (endpoint example) | 229 | Wikidata SPARQL | D2 |
| 285 | Open Food Facts API: product by barcode (endpoin | 284 | Open Food Facts product database (data and expor | D2 |
| 289 | ewc-onto — European Waste Catalogue OWL Ontology | 76 | EWC-Onto | D2 |
| 292 | ZKBS: Organismenliste (PDF) | 291 | ZKBS (GMO Organisms) | D2 |
| 295 | RegioIT Abfallnavi API: base URL (template) | 294 | RegioIT Abfallnavi API | D2 |
| 298 | ConceptNet API: German term lookup (endpoint exa | 62 | ConceptNet | D2 |
| 299 | api.store listing: ECHA C&L Inventory API (third | 68 | ECHA C&L Inventory | D2 |
| 303 | buzer.de: Anlage 4 StrlSchV (unofficial consolid | 129 | StrlSchV Anlage 4 (Radioactive Clearance) | D2 |
| 306 | NREL Battery Failure Databank: data downloads | 206 | NREL Battery Failure Databank | D2 |
| 311 | Munich geoportal WFS: AWM Wertstoffhöfe (GeoJSON | 212 | Munich AWM Wertstoffhöfe | D2 |
| 316 | MJU-Waste dataset, mirror on OpenDataLab | 147 | MJU-Waste dataset (RGB-D images) | D2 |

### Manifest ids and the rows that carry them

| manifest id | rows |
|---|---|
| odenet-oewn | cat-github-com-03 (row 152); cat-github-com-04 (row 153) |
| off-food-parquet | cat-huggingface-co-01 (row 169, duplicate); cat-huggingface-co-02 (row 170, duplicate); 949e5c48101f (row 284) |
| kaikki-de | cat-kaikki-org-01 (row 182) |
| off-food-jsonl | 9da1a5a2a286 (row 241, duplicate); 949e5c48101f (row 284) |
| open-beauty-facts | cat-world-openbeautyfacts-org-01 (row 283) |
| product-sample | 949e5c48101f (row 284) |
| eu-ecolabel | cat-apps-data-env-service-ec-europa-eu-01 (added) |
| fes-abfall-abc | cat-local-01 (added) |
| fes-facility-map | cat-local-02 (added) |
| fes-sorting-flyer | cat-local-03 (added) |
| fes-bin-alltagscontainer | cat-local-04 (added) |
| fes-bin-alltagscontainer-pdf | cat-local-04 (added) |
| fes-bin-altpapiertonne | cat-local-05 (added) |
| fes-bin-altpapiertonne-pdf | cat-local-05 (added) |
| fes-bin-restabfalltonne | cat-local-06 (added) |
| fes-bin-restabfalltonne-pdf | cat-local-06 (added) |
| fes-bin-verpackungstonne | cat-local-07 (added) |
| fes-bin-verpackungstonne-pdf | cat-local-07 (added) |
| frankfurt-abfallsatzung | cat-local-08 (added) |
| frankfurt-zero-waste-city | cat-local-09 (added) |
| recyclingzentrum-abfall-abc | cat-local-10 (added) |
| group3-abfall-abc | cat-local-11 (added) |
| group3-abfall-abc-csv | cat-local-11 (added) |
| group3-abfall-bereinigt | cat-local-11 (added) |
| avv-workbook | cat-local-12 (added) |
| off-materials | cat-local-13 (added) |
| concepts-derived | cat-local-14 (added) |

### Licence basis (all rows)

| basis | rows |
|---|---|
| nothing stated | 200 |
| catalogue gives an access term only ('Public', 'Free' ...) | 108 |
| manifest | 15 |
| catalogue License column (LLM-stated, unverified) | 7 |
| source page (quote in notes) | 7 |
| catalogue Description column (LLM-stated, unverified) | 5 |
| manifest (for the local copy) | 5 |
| catalogue License column, read again on the source page | 1 |
