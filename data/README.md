# Data Directory Assessment

This directory contains the data sources used for the Fessi Graph ETL Pipeline. It includes raw documents (PDFs, Word), structured data (CSV, JSON, Excel), and student group contributions.

## Overview

The data is organized into:
*   **Root `data/`**: Contains the primary documents and datasets used for the current pipeline.
*   **`01_Original_Daten/`**: Contains the original raw data, images, and additional contextual metadata (personas, demographics). **Note:** Many files here are duplicated in the root `data/` folder.
*   **`Group_2_Data/` & `Group_3_Data/`**: specific datasets processed or provided by student groups.

## Detailed Contents

### Root `data/`
*   **`Abfall-ABC_new.csv`**: The primary structured dataset mapping waste types ("Abfallart") to disposal methods ("Entsorgungsweg"), addresses, and contact info. This appears to be the most complete version.
*   **`disposal_map_db.json`**: A JSON database mapping unique IDs to specific disposal locations (e.g., "Wertstoffhof Nord") with addresses and opening hours.
*   **Regulations & Guides (PDFs)**:
    *   `Abfallsatzung 8_ Aenderungssatzung.pdf`: Official waste disposal statute.
    *   `Auf dem Weg zur Zero Waste City.pdf`: "Zero Waste City" policy/vision document.
    *   `was_kommt_wo_hinein_de.pdf`: Visual guide for waste sorting.
*   **Container Descriptions (DOCX)**:
    *   `Alltagscontainer.docx`, `Altpapiertonne.docx`, `Restabfalltonne.docx`, `Verpackungstonne.docx`: Detailed descriptions of specific waste containers.
*   **Metadata**:
    *   `pdf_metadata.json`, `word_metadata.json`: Metadata extracted from the respective document types.

### `01_Original_Daten/`
*   **`data/`**:
    *   **`pdfs/` & `word_docs/`**: These subdirectories contain **duplicates** of the PDFs and Word documents found in the root `data/` folder.
    *   `Abfall-ABC.csv`: An older or simplified version of the waste guide.
    *   **Context/Persona Data**: `age_groups.json`, `prompts.json`, `sinus_milieus_new.json`, `sinus_milieu_zuordnung.txt`. These files contain demographic info and "Sinus Milieu" descriptions, likely used to personalize chatbot responses.
*   **`image_storage/`**:
    *   Contains JPG images (e.g., paint cans), likely used for visual recognition training or UI assets.

### `Group_2_Data/`
*   **`Abgabe_Gruppe2_AVV_mit_AbfallABC.xlsx`**: Excel file appearing to map AVV (Abfallverzeichnis-Verordnung) codes to the Waste ABC.

### `Group_3_Data/`
*   **`Abfall_ABC_finalesFormat.xlsx`**: A "final format" Excel version of the Waste ABC.
*   **`abfall_bereinigt.csv`**: A "cleaned" version of the waste data. **Note:** This file contains some artifacts/garbage text in the first few rows (e.g., "jiewjqfi hü") and extra columns ("Entsorgungsweg FRA UAS") compared to the standard ABC.

## Assessment & Recommendations

1.  **Duplicates**: The contents of `01_Original_Daten/data/pdfs` and `01_Original_Daten/data/word_docs` are redundant with the files in the root `data/` directory.
2.  **Primary Source**: `Abfall-ABC_new.csv` should be treated as the source of truth for the flat waste list, while `disposal_map_db.json` provides the canonical location data.
3.  **Data Quality**: `Group_3_Data/abfall_bereinigt.csv` requires further cleaning before use due to the presence of placeholder text.

## File Inventory & Schema Analysis

The following table categorizes files in the root `data/` directory and their relevance to the Neo4j schema (`src/db/schema.cql`).

| File Name | Type | Description | Schema Relevance |
| :--- | :--- | :--- | :--- |
| **`Abfall-ABC_new.csv`** | CSV | **Master List.** Cleanest mapping of items to disposal paths. | `WasteItem`, `DisposalRule`, `Facility` |
| **`Abfall_ABC_finalesFormat.xlsx/.csv`** | Excel/CSV | Alternative source. Seprates FES vs. FRA AUS disposal paths. | `WasteItem`, `DisposalRule` |
| **`abfall_bereinigt.csv`** | CSV | **Dirty Data.** Contains garbage text (e.g., "jiewjqfi hü"). | **Avoid**. Use `Abfall-ABC_new.csv` instead. |
| **`disposal_map_db.json`** | JSON | **Facility Index.** Maps IDs to Facility names and addresses. | `Facility` (Infrastructure Layer). |
| **`word_metadata.json`** | JSON | Metadata for `.docx` files. | Metadata for `Source` nodes. |
| **`pdf_metadata.json`** | JSON | Metadata for `.pdf` files. | Metadata for `Source` nodes. |
| **`Abgabe_Gruppe2_AVV_mit_AbfallABC.csv`** | CSV | **AVV Source.** Maps items to AVV codes & Uni facilities. | `AVVCode`, `Facility` (Uni), `Contact`. |
| **`Alltagscontainer.docx`** | Docx | Info on general recycling bins. | Unstructured `Instruction` source. |
| **`Altpapiertonne.docx`** | Docx | Specifics on paper waste. | Unstructured `Instruction` source. |
| **`Restabfalltonne.docx`** | Docx | Specifics on packaging waste. | Unstructured `Instruction` source. |
| **`Verpackungstonne.docx`** | Docx | Specifics on packaging waste. | Unstructured `Instruction` source. |
| **`Abfallsatzung 8_ Aenderungssatzung.pdf`** | PDF | **Legal Text.** Official waste statutes. | Source for legal `Condition` logic. |
| **`Auf dem Weg zur Zero Waste City.pdf`** | PDF | General info/prose. | Context only. |
| **`was_kommt_wo_hinein_de.pdf`** | PDF | Flyer/Infographic source. | Context for `WasteStream` validation. |

### Analysis: `Abfall-ABC_new.csv`

*   **Content**: Contains waste items (`Abfallart`) mapped to disposal methods (`Entsorgungsweg`), addresses, and contact info.
*   **Suitability for Neo4j**:
    *   **Primary Source**: It is the best candidate for populating the **Concept Layer** (`WasteItem`).
    *   **ETL Challenge**: The `Entsorgungsweg` and `Adresse` columns are **not normalized**. They contain concatenated strings (e.g., "Wertstoffhof Nord Wertstoffhof West...") instead of distinct IDs.
    *   **Implementation Strategy**:
        1.  Extract `WasteItem` nodes from `Abfallart`.
        2.  Parse `Entsorgungsweg` to identify `Facility` or `WasteStream` entities.
        3.  Link them via `DISPOSED_IN` or `HAS_RULE` relationships.

### Analysis: `Abfall_ABC_finalesFormat.xlsx` (and `.csv`)

*   **Content**: Similar to the "new" CSV but splits disposal paths into `Entsorgungsweg FES` (FES-managed) and `Entsorgungsweg FRA AUS` (External/Other).
*   **Columns**: `ABC`, `Abfallart`, `Entsorgungsweg FES`, `Entsorgungsweg FRA AUS`, `Organisationseinheit`, `Zusätzliche Informationen`.
*   **Suitability**:
    *   **Pros**: Distinguishes between FES and non-FES disposal paths, which might be useful for `DisposalRule` logic (e.g., "Managed by FES" vs "Third-party").
    *   **Cons**: Lacks the explicit `Adresse`, `Öffnungszeiten`, and `Kontakt` columns found in `Abfall-ABC_new.csv`. Contains some garbage data in the first row (e.g., "jiewjqfi hü").
        *   **Recommendation**: Use as a **secondary source** to cross-reference disposal types if strict separation between FES/External is needed. Otherwise, `Abfall-ABC_new.csv` is more comprehensive for user-facing details.

### Analysis: `abfall_bereinigt.csv`

*   **Content**: A simplified CSV listing waste items and disposal paths for "Stadt Frankfurt" and "FRA UAS" (University).
*   **Suitability for Neo4j**: **UNSUITABLE**
    *   **Data Corruption**: The critical columns `Entsorgungsweg FRA UAS`, `Organisationseinheit`, and `Zusätzliche Information` contain random garbage strings (e.g., "jiewjqfi hü", "fqcfet") in the first data row and are largely empty/useless in subsequent rows.
    *   **Redundancy**: The valid data (columns 1-2) is a subset of `Abfall-ABC_new.csv` but less structured.
    *   **Schema Mismatch**: The column structure suggests a distinction between City vs. University disposal logic, which *could* have been valuable for `DisposalRule` nodes. However, the data quality prevents this usage.
    *   **Recommendation**: **Do not use.** The file is corrupted. Rely on `Abfall-ABC_new.csv` for the primary dataset and `Abfall_ABC_finalesFormat.csv` for any FES/University logic distinctions if strictly necessary (though that file also shares some artifacts).

### Analysis: `Abfallsatzung 8_ Aenderungssatzung.pdf`

*   **Content**: The official "Statute of the City of Frankfurt am Main on the Avoidance, Recovery and Disposal of Waste".
*   **Suitability for Neo4j**: **HIGH (Canonical Rule Source)**
    *   **Definitions (§3)**: Provides the legal definitions for `WasteStream` nodes (e.g., "Bioabfälle", "Sperrmüll") and excludes specific items (e.g., "Soup is not Bio-waste"). This is the basis for `Condition` logic.
    *   **Infrastructure (§8)**: Defines valid `Container` types and their color coding (Black=Residual, Brown=Bio, Green=Paper).
    *   **Rules (§9)**: Explicitly states what goes into which bin (positive rules) and what is forbidden (negative rules).
    *   **Facilities (§15)**: Lists specific `Facility` names (e.g., "Wertstoffhof Nord") which validates the entries in `disposal_map_db.json`.
    *   **Recommendation**: Use this text as the **Ground Truth** for validating the graph schema and logic. While we won't write a PDF parser for the ETL, the logic in the Python scripts should reflect these statutes.

### Analysis: `Auf dem Weg zur Zero Waste City.pdf`

*   **Content**: A strategic policy document titled "On the Way to a Zero Waste City", detailing Frankfurt's waste reduction goals, recycling statistics (e.g., "280,000 tons of waste annually"), and circular economy measures.
*   **Suitability for Neo4j**: **LOW (Context Only)**
    *   **Nature**: Prose-heavy, descriptive text. Focuses on *strategy* rather than *operational rules*.
    *   **Relevance**: It does not contain structured disposal rules or facility data.
    *   **Use Case**: Valuable only for **unstructured RAG retrieval** (e.g., answering "What are Frankfurt's recycling goals?"), but should **not** be ingested into the structured knowledge graph (Nodes/Relationships).
    *   **Recommendation**: Exclude from the primary ETL graph construction. Keep available for vector-based context retrieval if needed.

### Analysis: `Abgabe_Gruppe2_AVV_mit_AbfallABC.csv`

*   **Content**: A mapping of waste items to **AVV Codes** (Abfallverzeichnis-Verordnung) and university-specific disposal instructions.
*   **Suitability for Neo4j**: **HIGH (AVV & Facility Source)**
    *   **AVV Codes**: This is the *only* source providing hierarchical AVV codes (`AVV_Level1` to `AVV_Level3`), which are critical for the regulatory/concept layer (`AVVCode` nodes).
    *   **University Facilities**: The `Entsorgungsweg FRA_UAS` column contains rich JSON-like data (e.g., `{'name': 'Facility Management melden', 'contact': 'Herr Schwende...'}`). This is a **primary source** for:
        *   `Facility` nodes specific to the university context (e.g., "Gebäude 8, Raum 21").
        *   `Contact` information (Phone, Email) which is missing in other files.
    *   **Hierarchy**: The file structure explicitly models the AVV hierarchy (Chapter -> Section -> Waste Type).
    *   **Schema Impact**:
        *   **New Relationships**: The current schema (`src/db/schema.cql`) defines `AVVCode` but lacks explicit hierarchy support. We need to standardize `(AVVCode)-[:PARENT_OF]->(AVVCode)` relationships.
        *   **Properties**: Ensure `WasteItem` nodes have a `[:CLASSIFIED_AS]` relationship to the leaf `AVVCode`.
        *   **Contact Nodes**: The schema mentions `Facility` but not `Contact`. We should either add a `Contact` label or embed these details into `Facility` nodes.
    *   **Recommendation**:
        1.  Parse `AVV_Level1/2/3` columns to build the `AVVCode` hierarchy.
        2.  Parse the `Entsorgungsweg FRA_UAS` column (eval string to dict) to extract `Facility` and `Contact` nodes.
        3.  Link `WasteItem` nodes to these AVV codes.

### Analysis: `was_kommt_wo_hinein_de.pdf`

*   **Content**: A visual "Flyer/Infographic" style guide for household waste separation in Frankfurt.
*   **Suitability for Neo4j**: **MEDIUM (Validation & Context)**
    *   **Visual Logic**: Defines the color-coded bin system (Blue=Glass, Yellow=Packaging, Green=Paper, Brown=Bio, Grey=Residual).
    *   **Rules**: Provides explicit "YES" (Das kommt hinein) and "NO" (Das kommt nicht hinein) lists for each bin type.
    *   **Specifics**:
        *   **Glass**: Separated by color (White, Green, Brown). Blue/Red goes to Green.
        *   **Packaging**: "Spoon clean" (empty but not washed).
        *   **Bio**: No plastic bags, newspaper wrapping allowed.
    *   **Recommendation**:
        *   Use to **validate** the `DisposalRule` logic derived from `Abfall-ABC_new.csv`.
        *   Useful for generating "Tip" or "Note" properties on `WasteStream` nodes (e.g., "Note: Blue glass belongs in the green container").

### Analysis: Container Descriptions (Verpackungstonne, Restabfalltonne, etc.)

*   **Files**: `Verpackungstonne.pdf`, `Restabfalltonne.pdf`, `Altpapiertonne.pdf`, `Alltagscontainer.pdf` (derived from DOCX).
*   **Content**: Detailed operational manuals for each specific bin type managed by FES.
*   **Suitability for Neo4j**: **HIGH (Rule Detail & Validation)**
    *   **Verpackungstonne (Yellow Lid)**:
        *   **Scope**: Sales packaging *not* made of glass or paper.
        *   **Key Rules**: "Spoon clean" (löffelrein) is sufficient; no need to wash.
        *   **Explicit Includes**: Plastic films, cups, styrofoam, composite cartons (TetraPak), cans.
        *   **Explicit Excludes**: Paper, glass, heavily soiled pizza boxes.
    *   **Restabfalltonne (Gray Lid)**:
        *   **Scope**: Non-recyclable residual waste.
        *   **Explicit Includes**: Hygiene articles, diapers, cigarette butts, vacuum bags, *all* glass shards (including mirrors/drinking glasses), sweeping waste.
        *   **Explicit Excludes**: Electrical waste, batteries, hazardous substances (paint).
    *   **Altpapiertonne (Green Lid)**:
        *   **Scope**: Paper, cardboard, cartons.
        *   **Explicit Includes**: Newspapers, catalogs, books.
        *   **Explicit Excludes**: Coated paper, dirty paper, hygiene paper (tissues/diapers).
    *   **Alltagscontainer (Glass)**:
        *   **Scope**: Container glass (bottles, jars).
        *   **Sorting Logic**: White, Green, Brown. **Crucial Rule**: Blue or other colored glass must go into the **Green** container.
        *   **Explicit Excludes**: Flat glass (windows/mirrors), drinking glasses, ceramics/porcelain (these go to Residual waste).
    *   **Recommendation**:
        *   These files provide the granular **"Yes/No" lists** needed for the `Condition` nodes in the graph.
        *   The "Blue glass -> Green bin" rule is a specific edge case that must be modeled to prevent user error.
