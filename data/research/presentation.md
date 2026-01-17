# Fessi Knowledge Graph: Data Sources & Roadmap

**University Waste Disposal Chatbot - Progress Update**

---

## 1. What We Found

### Research Scope

We systematically surveyed available data sources across six domains relevant to university waste disposal:

| Domain | Sources Evaluated | Key Findings |
|--------|-------------------|--------------|
| Food & Grocery | 8 | Open Food Facts covers 800k German products with packaging data |
| Consumer Electronics | 10 | Stiftung EAR provides official ElektroG taxonomy; iFixit has battery removability data |
| Chemicals & Hazardous | 15 | PubChem + GESTIS + BAM cover identity, safety, and transport |
| German Municipal Rules | 9 | Three SaaS platforms (Abfall+, MyMüll, Müllmax) dominate the market |
| AVV/EWC Codes | 12 | ~840 official codes; SEPA thesaurus maps common terms to codes |
| Construction Materials | 14 | LAGA M20 + ÖKOBAUDAT provide classification and LCA data |

### Key Discoveries

**1. The SaaS Oligopoly**
- Three platforms power most German municipal waste apps
- Pre-configured API keys available via open-source Home Assistant integration
- Enables broad coverage without 400+ custom connectors

**2. Mirror Entry Problem**
- Same waste item can have two AVV codes (hazardous vs. non-hazardous)
- Requires conditional logic in chatbot: "Is it contaminated?"
- Graph must model decision trees, not just lookups

**3. Regional Variation**
- Pizza box: Altpapier in some cities, Restmüll in others
- Bioplastics: Banned from Biotonne in most districts
- Graph needs location-aware rule overrides

**4. BAM Database Now Free**
- German dangerous goods database (UN numbers, ADR codes)
- Switched from paid to free in July 2025
- Eliminates need for expensive logistics software

---

## 2. The Data Landscape

### Available Data Volume

| Source | Entity Count | Data Type |
|--------|--------------|-----------|
| PubChem | 111,000,000 | Chemical compounds |
| Icecat | 15,000,000 | Consumer products |
| ECHA C&L Inventory | 5,000,000 | Chemical classifications |
| Open Food Facts (DE) | 800,000 | Food products |
| Stiftung EAR | 50,000 | Electronics producers & devices |
| SEPA Waste Thesaurus | 10,000 | Waste term synonyms |
| GESTIS | 8,800 | Occupational safety substances |
| BSR Abfall-ABC | 2,000 | Waste item vocabulary |
| AVV Codes | 840 | Legal waste classifications |
| Frankfurt Facilities | 50 | Local disposal locations |

**Total if imported naively: 100+ million nodes**

---

## 3. What We Actually Need

### The Distillation

A university waste chatbot answers: *"How do I dispose of X?"*

It does **not** need to know every product ever manufactured.

| Layer | Purpose | Source | Estimated Nodes |
|-------|---------|--------|-----------------|
| Waste Vocabulary | "What can users ask about?" | BSR + university items | ~5,000 |
| Synonyms | "Old paint" → "Altlack" | SEPA Thesaurus | ~8,000 |
| AVV Codes | Legal classification | Gesetze-im-Internet | ~1,000 |
| Disposal Rules | Conditional logic | Municipal docs + statutes | ~10,000 |
| Facilities | Where to bring waste | disposal_map_db.json + OSM | ~200 |
| Lab Chemicals | University-specific | GESTIS subset | ~2,000 |
| Containers | Bin types and colors | Container docs | ~20 |

### Target Graph Size

| Scenario | Nodes | Relationships | Use Case |
|----------|-------|---------------|----------|
| **MVP** | ~25,000 | ~50,000 | Text-based queries |
| **+ Barcode Scanning** | ~150,000 | ~300,000 | Product lookups via API |
| **+ Full Chemistry** | ~500,000 | ~1,000,000 | Lab waste advisor |

**Recommendation: Start with MVP, extend on demand**

---

## 4. Roadmap: First Month

### Phase 1: Foundation (Week 1-2)

**Goal:** Complete core graph with Frankfurt rules

| Task | Data Source | Deliverable |
|------|-------------|-------------|
| Import full AVV hierarchy | Gesetze-im-Internet | 840 AVVCode nodes with parent relationships |
| Enrich with English names | SEPA/EWC lists | Bilingual labels |
| Import BSR vocabulary | Berlin Open Data | ~2,000 WasteItem nodes |
| Add synonym layer | SEPA Thesaurus | ~5,000 altLabel properties |
| Import Frankfurt facilities | disposal_map_db.json | ~50 Facility nodes |

**Validation Query:**
```cypher
MATCH (w:WasteItem)-[:HAS_AVV_CODE]->(a:AVVCode)
WHERE a.isHazardous = true
RETURN w.name, a.code LIMIT 20;
```

### Phase 2: Conditional Logic (Week 2-3)

**Goal:** Model decision trees for ambiguous items

| Task | Data Source | Deliverable |
|------|-------------|-------------|
| Parse container documents | Alltagscontainer.docx, etc. | Yes/No rules per bin type |
| Model mirror entries | AVV pairs | MIRROR_OF relationships |
| Add clarifying questions | HP criteria | Question nodes for chatbot |
| Import Frankfurt statute rules | Abfallsatzung PDF | DisposalRule + Condition nodes |

**Example Decision Tree:**
```
User: "Farbdose" (paint can)
  → Condition: "Is it empty and dry?"
    → Yes: Verpackungstonne (Yellow)
    → No: Schadstoffmobil (Hazardous collection)
```

### Phase 3: University Context (Week 3-4)

**Goal:** Add lab waste and campus-specific facilities

| Task | Data Source | Deliverable |
|------|-------------|-------------|
| Import university facilities | Abgabe_Gruppe2_AVV file | Campus rooms, contacts |
| Add lab chemical mappings | GESTIS subset (~500 common) | Chemical → AVV links |
| Model healthcare waste | LAGA M18 rules | Chapter 18 decision logic |
| Import electronics rules | ElektroG Sammelgruppen | Size-based routing |

**Validation Query:**
```cypher
MATCH (c:Chemical)-[:DISPOSED_AS]->(a:AVVCode)
WHERE c.name CONTAINS "Aceton"
RETURN c.name, a.code, a.isHazardous;
```

### Phase 4: API Integration (Week 4+)

**Goal:** Enable real-time lookups without bulk import

| Task | Integration | Benefit |
|------|-------------|---------|
| PubChem connector | REST API | Resolve chemical synonyms on-demand |
| Open Food Facts connector | REST API | Barcode → packaging info |
| Municipal schedule connector | Abfall+ / MyMüll | Collection dates per address |

---

## 5. Summary

| Metric | Raw Data | Distilled Graph |
|--------|----------|-----------------|
| Sources evaluated | 68 | 12 prioritized |
| Potential nodes | 100,000,000+ | ~25,000 (MVP) |
| Data formats | PDF, Excel, JSON, API, RDF | Unified Neo4j |
| Coverage | Global | Frankfurt + University |

### Next Steps

1. Finalize AVV import pipeline (structured ETL)
2. Build LLM extraction for container documents (unstructured ETL)
3. Deploy Neo4j instance for demo queries
4. Connect to chatbot prototype

---

**Questions?**
