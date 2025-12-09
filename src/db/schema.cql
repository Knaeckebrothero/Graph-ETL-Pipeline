// =============================================================================
// Fessi Knowledge Graph - Database Schema v4.0
// =============================================================================
// ETL Pipeline for University Waste Disposal Chatbot (Graph-RAG)
//
// Schema Layers:
//   1. Core Entities (Begriffs-Ebene) - WasteItem, WasteStream, AVVCode
//   2. Logic & Rules (Regel-Ebene) - DisposalRule, Condition, Instruction
//   3. Infrastructure (Infrastruktur-Ebene) - Container, Building, Room, Facility
// =============================================================================


// -----------------------------------------------------------------------------
// 1. CORE ENTITIES (Begriffs-Ebene)
// -----------------------------------------------------------------------------

// WasteItem: The central node (e.g., "Banana Peel", "Paint Can")
CREATE CONSTRAINT waste_item_uid IF NOT EXISTS FOR (w:WasteItem) REQUIRE w.uid IS UNIQUE;
CREATE CONSTRAINT waste_item_name IF NOT EXISTS FOR (w:WasteItem) REQUIRE w.name IS UNIQUE;
CREATE INDEX waste_item_synonyms IF NOT EXISTS FOR (w:WasteItem) ON (w.synonyms);

// WasteStream: The target fraction (e.g., "Restmüll", "Hazardous Waste")
CREATE CONSTRAINT waste_stream_uid IF NOT EXISTS FOR (s:WasteStream) REQUIRE s.uid IS UNIQUE;
CREATE CONSTRAINT waste_stream_name IF NOT EXISTS FOR (s:WasteStream) REQUIRE s.name IS UNIQUE;

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


// -----------------------------------------------------------------------------
// 2. LOGIC & RULES (Regel-Ebene)
// -----------------------------------------------------------------------------

// DisposalRule: The wrapper for the logic (e.g., "Paint-City-Rule")
CREATE CONSTRAINT rule_uid IF NOT EXISTS FOR (r:DisposalRule) REQUIRE r.uid IS UNIQUE;

// Condition: The question node (e.g., "Is it liquid?")
CREATE CONSTRAINT condition_key IF NOT EXISTS FOR (c:Condition) REQUIRE c.key IS UNIQUE;

// Instruction: Imperative commands (e.g., "Don't flush")
CREATE CONSTRAINT instruction_uid IF NOT EXISTS FOR (i:Instruction) REQUIRE i.uid IS UNIQUE;

// Tip: Helpful hints and validation notes (e.g., "Blue glass goes in green container")
// Properties: uid, text, type (sorting_hint|exception|best_practice)
CREATE CONSTRAINT tip_uid IF NOT EXISTS FOR (t:Tip) REQUIRE t.uid IS UNIQUE;
CREATE INDEX tip_type IF NOT EXISTS FOR (t:Tip) ON (t.type);


// -----------------------------------------------------------------------------
// 3. INFRASTRUCTURE (Infrastruktur-Ebene)
// -----------------------------------------------------------------------------

// Container: The physical bins
CREATE CONSTRAINT container_uid IF NOT EXISTS FOR (c:Container) REQUIRE c.uid IS UNIQUE;

// Building: Campus buildings
CREATE CONSTRAINT building_uid IF NOT EXISTS FOR (b:Building) REQUIRE b.uid IS UNIQUE;

// Room: Rooms within buildings
CREATE CONSTRAINT room_uid IF NOT EXISTS FOR (r:Room) REQUIRE r.uid IS UNIQUE;

// Facility: External or special locations (e.g., "Wertstoffhof")
// Properties: name, address, opening_hours, contact_name, contact_phone, contact_email
CREATE CONSTRAINT facility_uid IF NOT EXISTS FOR (f:Facility) REQUIRE f.uid IS UNIQUE;

// Source: Provenance tracking for AI-extracted or document-derived data
// Properties: name, type (pdf|csv|docx|llm), file_path, extraction_date, model_id (for LLM sources)
CREATE CONSTRAINT source_uid IF NOT EXISTS FOR (s:Source) REQUIRE s.uid IS UNIQUE;

// Location: Generic location index (for quick geo-lookups)
CREATE INDEX location_name IF NOT EXISTS FOR (l:Location) ON (l.name);


// =============================================================================
// RELATIONSHIP PATTERNS (Reference)
// =============================================================================
// These are not executable - just documentation of expected relationships:
//
// Core → Rules:
//   (WasteItem)-[:HAS_RULE]->(DisposalRule)
//   (DisposalRule)-[:HAS_CONDITION]->(Condition)
//   (DisposalRule)-[:HAS_INSTRUCTION]->(Instruction)
//   (DisposalRule)-[:ROUTES_TO]->(WasteStream)
//
// Conditions (Decision Tree):
//   (Condition)-[:IF_TRUE]->(DisposalRule|WasteStream)
//   (Condition)-[:IF_FALSE]->(DisposalRule|WasteStream)
//
// Legal Classification:
//   (WasteItem)-[:CLASSIFIED_AS]->(AVVCode)
//   (WasteStream)-[:REQUIRES_AVV]->(AVVCode)
//   (AVVCode)-[:HAS_PARENT]->(AVVCode)  // e.g., "08 01 11*" -> "08 01" -> "08"
//
// Tips & Hints:
//   (WasteStream)-[:HAS_TIP]->(Tip)
//   (WasteItem)-[:HAS_TIP]->(Tip)
//   (Container)-[:HAS_TIP]->(Tip)
//
// Provenance (for AI-extracted data):
//   (DisposalRule)-[:DERIVED_FROM]->(Source)
//   (Condition)-[:DERIVED_FROM]->(Source)
//   (Instruction)-[:DERIVED_FROM]->(Source)
//   (WasteItem)-[:DERIVED_FROM]->(Source)
//   (Tip)-[:DERIVED_FROM]->(Source)
//
// Infrastructure:
//   (WasteStream)-[:COLLECTED_IN]->(Container)
//   (Container)-[:LOCATED_IN]->(Room)
//   (Room)-[:PART_OF]->(Building)
//   (Container)-[:LOCATED_AT]->(Facility)
//
// =============================================================================
