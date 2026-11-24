// =============================================================================
// Induced schema (S2), property-graph realisation
// =============================================================================
// Written from the reply of the schema induction, schemas/treatments/S2-induced/pg/induction.reply.json
// (sha256 560a3f2523d07815...), by schemas/treatments/derive.py, rules 0.2.
// Generated; do not edit by hand. The schema is the model's: nothing was renamed, dropped or added,
// except that the identifier of a type counts as unique (see declaration.json, "induction").
// Status: rules decided on 15 November 2026; not frozen (freeze: 23 November).
// =============================================================================

// Document
// Properties: document_id (identifier, string, required), title (string, required), publisher (string), scope (string)
CREATE CONSTRAINT document_document_id_unique IF NOT EXISTS FOR (n:Document) REQUIRE n.document_id IS UNIQUE;

// Section
// Properties: section_id (identifier, string, required), reference (string), heading (string), text (string)
CREATE CONSTRAINT section_section_id_unique IF NOT EXISTS FOR (n:Section) REQUIRE n.section_id IS UNIQUE;

// Organisation
// Properties: name (identifier, string, required), address (string), contact (string), url (string)
CREATE CONSTRAINT organisation_name_unique IF NOT EXISTS FOR (n:Organisation) REQUIRE n.name IS UNIQUE;

// WasteType
// Properties: name (identifier, string, required), avv_code (string), avv_level1 (string), avv_level2 (string), avv_level3 (string), description (string), notes (string)
CREATE CONSTRAINT wastetype_name_unique IF NOT EXISTS FOR (n:WasteType) REQUIRE n.name IS UNIQUE;

// WasteCode
// Properties: code (identifier, string, required), designation (string), hazardous (boolean), chapter (string)
CREATE CONSTRAINT wastecode_code_unique IF NOT EXISTS FOR (n:WasteCode) REQUIRE n.code IS UNIQUE;

// DisposalOption
// Properties: name (identifier, string, required), kind (string), address (string), opening_hours (string), contact (string), additional_info (string), link (string)
CREATE CONSTRAINT disposaloption_name_unique IF NOT EXISTS FOR (n:DisposalOption) REQUIRE n.name IS UNIQUE;

// ContainerType
// Properties: name (identifier, string, required), container_kind (string), volume_litres (decimal), lid_colour (string), emptying_interval (string), description (string)
CREATE CONSTRAINT containertype_name_unique IF NOT EXISTS FOR (n:ContainerType) REQUIRE n.name IS UNIQUE;

// Fee
// Properties: fee_id (identifier, string, required), amount (decimal, required), currency (string), unit (string), label (string), conditions (string), reference (string), container_volume_litres (decimal), container_kind (string), valid_from (date), valid_until (date), notes (string)
CREATE CONSTRAINT fee_fee_id_unique IF NOT EXISTS FOR (n:Fee) REQUIRE n.fee_id IS UNIQUE;

// Product
// Properties: barcode (identifier, string, required), product_name (string), product_name_de (string), quantity (quantity), categories (string), packaging (string), labels (string), image_url (string), source_api (string), found (boolean)
CREATE CONSTRAINT product_barcode_unique IF NOT EXISTS FOR (n:Product) REQUIRE n.barcode IS UNIQUE;

// Brand
// Properties: name (identifier, string, required)
CREATE CONSTRAINT brand_name_unique IF NOT EXISTS FOR (n:Brand) REQUIRE n.name IS UNIQUE;

// Category
// Properties: tag (identifier, string, required), name (string), path (string)
CREATE CONSTRAINT category_tag_unique IF NOT EXISTS FOR (n:Category) REQUIRE n.tag IS UNIQUE;

// Packaging
// Properties: name (identifier, string, required)
CREATE CONSTRAINT packaging_name_unique IF NOT EXISTS FOR (n:Packaging) REQUIRE n.name IS UNIQUE;


// =============================================================================
// RELATIONSHIP PATTERNS (Reference)
// =============================================================================
// These are not executable - just documentation of expected relationships:
//
//   (Section)-[:PART_OF]->(Document)   // at most one per start node
//   (Document)-[:PUBLISHED_BY]->(Organisation)   // at most one per start node
//   (Fee)-[:DEFINED_IN]->(Section)   // at most one per start node
//   (Fee)-[:APPLIES_TO]->(WasteType)
//   (Fee)-[:APPLIES_TO]->(ContainerType)
//   (Fee)-[:APPLIES_TO]->(DisposalOption)
//   (WasteType)-[:DISPOSED_VIA {jurisdiction, organisation_unit, location, contact, additional_info}]->(DisposalOption|ContainerType)   // jurisdiction: string; organisation_unit: string; location: string; contact: string; additional_info: string
//   (WasteType)-[:CLASSIFIED_AS]->(WasteCode)
//   (DisposalOption)-[:OPERATED_BY]->(Organisation)
//   (ContainerType)-[:COLLECTS]->(WasteType)
//   (Product)-[:IN_CATEGORY]->(Category)
//   (Product)-[:HAS_BRAND]->(Brand)
//   (Product)-[:HAS_PACKAGING]->(Packaging)
//
// =============================================================================
