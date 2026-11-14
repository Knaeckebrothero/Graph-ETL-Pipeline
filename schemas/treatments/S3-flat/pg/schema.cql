// =============================================================================
// Fessi flat (S3), property-graph realisation
// =============================================================================
// Derived from schemas/v5.0/schema.cql (sha256 e0c1acf69b6692c8...)
// by schemas/treatments/derive.py, rules draft-0.1. Generated; do not edit by hand.
// Status: draft, not reviewed, not frozen.
// =============================================================================

// AVVCode: Legal Classification per Abfallverzeichnis-Verordnung (European Waste Catalogue)
// Structure: 6-digit code (XX XX XX) where:
//   - First pair:  Chapter (origin/source of waste, 20 chapters total)
//   - Second pair: Subgroup (industry-typical generation)
//   - Third pair:  Specific waste type
// Properties: code, name, level (1|2|3), is_hazardous (codes ending with * are hazardous)
//
// Chapters (Level 1):
//   01 - Abfälle aus Abbau von Bodenschätzen (Mining)
//   02 - Abfälle aus Landwirtschaft/Gartenbau/Nahrungsmittel (Agriculture/Food)
//   03 - Abfälle aus Holzbearbeitung/Papierherstellung (Wood/Paper)
//   04 - Abfälle aus Leder-/Pelz-/Textilindustrie (Leather/Textiles)
//   05 - Abfälle aus Erdölraffination/Kohlepyrolyse (Petroleum/Coal)
//   06 - Abfälle aus anorganisch-chemischen Prozessen (Inorganic Chemistry)
//   07 - Abfälle aus organisch-chemischen Prozessen (Organic Chemistry)
//   08 - Abfälle aus Farben/Lacken/Klebstoffen/Druckfarben (Paints/Coatings)
//   09 - Abfälle aus fotografischer Industrie (Photographic)
//   10 - Abfälle aus thermischen Prozessen (Thermal Processes)
//   11 - Abfälle aus chemischer Oberflächenbearbeitung (Metal Surface Treatment)
//   12 - Abfälle aus mechanischer Formgebung (Mechanical Shaping)
//   13 - Ölabfälle und flüssige Brennstoffe (Oils/Fuels)
//   14 - Abfälle aus organischen Lösemitteln/Kühlmitteln (Solvents/Coolants)
//   15 - Verpackungsabfall und Filtermaterialien (Packaging/Filters)
//   16 - Abfälle, nicht anderswo aufgeführt (Not Otherwise Specified)
//   17 - Bau- und Abbruchabfälle (Construction/Demolition)
//   18 - Abfälle aus medizinischer/tierärztlicher Versorgung (Medical/Veterinary)
//   19 - Abfälle aus Abfall-/Abwasserbehandlung (Waste/Wastewater Treatment)
//   20 - Siedlungsabfälle (Municipal Waste)
//
CREATE CONSTRAINT avv_code_unique IF NOT EXISTS FOR (a:AVVCode) REQUIRE a.code IS UNIQUE;
CREATE INDEX avv_level IF NOT EXISTS FOR (a:AVVCode) ON (a.level);
CREATE INDEX avv_hazardous IF NOT EXISTS FOR (a:AVVCode) ON (a.is_hazardous);

// Building: Campus buildings
CREATE CONSTRAINT building_uid IF NOT EXISTS FOR (b:Building) REQUIRE b.uid IS UNIQUE;

// Container: The physical bins
CREATE CONSTRAINT container_uid IF NOT EXISTS FOR (c:Container) REQUIRE c.uid IS UNIQUE;

// DictionaryEntry: Dictionary-sourced vocabulary nodes (promoted to :WasteItem after LLM classification)
CREATE CONSTRAINT dict_entry_uid IF NOT EXISTS FOR (d:DictionaryEntry) REQUIRE d.uid IS UNIQUE;
CREATE CONSTRAINT dict_entry_name IF NOT EXISTS FOR (d:DictionaryEntry) REQUIRE d.name IS UNIQUE;
CREATE INDEX dict_entry_lang IF NOT EXISTS FOR (d:DictionaryEntry) ON (d.lang);
CREATE INDEX dict_entry_synset IF NOT EXISTS FOR (d:DictionaryEntry) ON (d.synset_id);

// Facility: External or special locations (e.g., "Wertstoffhof")
// Properties: name, address, opening_hours, contact_name, contact_phone, contact_email
CREATE CONSTRAINT facility_uid IF NOT EXISTS FOR (f:Facility) REQUIRE f.uid IS UNIQUE;

// Instruction: Imperative commands (e.g., "Don't flush")
CREATE CONSTRAINT instruction_uid IF NOT EXISTS FOR (i:Instruction) REQUIRE i.uid IS UNIQUE;

// Location: Generic location index (for quick geo-lookups)
CREATE INDEX location_name IF NOT EXISTS FOR (l:Location) ON (l.name);

// Material: Shared reference taxonomy (~200 nodes from Open Food Facts material tags)
// Properties: uid, tag (e.g., "en:clear-glass"), name, name_de
// Hierarchy via IS_A: en:pp-5-polypropylene IS_A en:plastic
CREATE CONSTRAINT material_uid IF NOT EXISTS FOR (m:Material) REQUIRE m.uid IS UNIQUE;
CREATE CONSTRAINT material_tag IF NOT EXISTS FOR (m:Material) REQUIRE m.tag IS UNIQUE;
CREATE INDEX material_name IF NOT EXISTS FOR (m:Material) ON (m.name);

// PackagingComponent: Physical part of a product (e.g., "glass jar", "plastic lid")
// Properties: uid, shape (pot|lid|bottle|wrapper|...), weight_g, food_contact, recycling_hint
CREATE CONSTRAINT pkg_component_uid IF NOT EXISTS FOR (c:PackagingComponent) REQUIRE c.uid IS UNIQUE;
CREATE INDEX pkg_component_shape IF NOT EXISTS FOR (c:PackagingComponent) ON (c.shape);

// Product: Commercial product identified by EAN barcode (e.g., "Nutella 400g")
// Properties: uid, ean, name, brand, quantity, categories_tags, labels_tags,
//             countries_tags, packaging_tags, image_url, source_api
CREATE CONSTRAINT product_uid IF NOT EXISTS FOR (p:Product) REQUIRE p.uid IS UNIQUE;
CREATE CONSTRAINT product_ean IF NOT EXISTS FOR (p:Product) REQUIRE p.ean IS UNIQUE;
CREATE INDEX product_name IF NOT EXISTS FOR (p:Product) ON (p.name);
CREATE INDEX product_brand IF NOT EXISTS FOR (p:Product) ON (p.brand);

// Room: Rooms within buildings
CREATE CONSTRAINT room_uid IF NOT EXISTS FOR (r:Room) REQUIRE r.uid IS UNIQUE;

// Source: Provenance tracking for AI-extracted or document-derived data
// Properties: name, type (pdf|csv|docx|llm), file_path, extraction_date, model_id (for LLM sources)
CREATE CONSTRAINT source_uid IF NOT EXISTS FOR (s:Source) REQUIRE s.uid IS UNIQUE;

// Tip: Helpful hints and validation notes (e.g., "Blue glass goes in green container")
// Properties: uid, text, type (sorting_hint|exception|best_practice)
CREATE CONSTRAINT tip_uid IF NOT EXISTS FOR (t:Tip) REQUIRE t.uid IS UNIQUE;
CREATE INDEX tip_type IF NOT EXISTS FOR (t:Tip) ON (t.type);

// WasteItem: The central node (e.g., "Banana Peel", "Paint Can")
CREATE CONSTRAINT waste_item_uid IF NOT EXISTS FOR (w:WasteItem) REQUIRE w.uid IS UNIQUE;
CREATE CONSTRAINT waste_item_name IF NOT EXISTS FOR (w:WasteItem) REQUIRE w.name IS UNIQUE;
CREATE INDEX waste_item_synonyms IF NOT EXISTS FOR (w:WasteItem) ON (w.synonyms);

// WasteStream: The target fraction (e.g., "Restmüll", "Hazardous Waste")
CREATE CONSTRAINT waste_stream_uid IF NOT EXISTS FOR (s:WasteStream) REQUIRE s.uid IS UNIQUE;
CREATE CONSTRAINT waste_stream_name IF NOT EXISTS FOR (s:WasteStream) REQUIRE s.name IS UNIQUE;


// =============================================================================
// RELATIONSHIP PATTERNS (Reference)
// =============================================================================
// These are not executable - just documentation of expected relationships:
//
//   (AVVCode)-[:HAS_PARENT]->(AVVCode)
//   (Container)-[:HAS_TIP]->(Tip)
//   (Container)-[:LOCATED_AT]->(Facility)
//   (Container)-[:LOCATED_IN]->(Room)
//   (DictionaryEntry)-[:DERIVED_FROM]->(Source)
//   (DictionaryEntry)-[:IS_A]->(DictionaryEntry)
//   (DictionaryEntry)-[:SYNONYM_OF]->(DictionaryEntry)
//   (DictionaryEntry)-[:TRANSLATION_OF]->(DictionaryEntry)
//   (Instruction)-[:DERIVED_FROM]->(Source)
//   (Material)-[:CLASSIFIED_AS]->(AVVCode)
//   (Material)-[:DEFAULT_STREAM]->(WasteStream)
//   (Material)-[:IS_A]->(Material)
//   (PackagingComponent)-[:DERIVED_FROM]->(Source)
//   (PackagingComponent)-[:HAS_INSTRUCTION]->(Instruction)
//   (PackagingComponent)-[:MADE_OF]->(Material)
//   (PackagingComponent)-[:ROUTES_TO]->(WasteStream)
//   (Product)-[:DERIVED_FROM]->(Source)
//   (Product)-[:HAS_COMPONENT {qty, weight_g}]->(PackagingComponent)
//   (Product)-[:IS_A]->(WasteItem)
//   (Room)-[:PART_OF]->(Building)
//   (Tip)-[:DERIVED_FROM]->(Source)
//   (WasteItem)-[:CLASSIFIED_AS]->(AVVCode)
//   (WasteItem)-[:DERIVED_FROM]->(Source)
//   (WasteItem)-[:HAS_INSTRUCTION]->(Instruction)
//   (WasteItem)-[:HAS_TIP]->(Tip)
//   (WasteItem)-[:ROUTES_TO]->(WasteStream)
//   (WasteStream)-[:COLLECTED_IN]->(Container)
//   (WasteStream)-[:HAS_TIP]->(Tip)
//   (WasteStream)-[:REQUIRES_AVV]->(AVVCode)
//
// =============================================================================
