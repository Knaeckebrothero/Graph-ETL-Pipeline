# Research Agent #13: Multilingual Synonyms (Wikidata)

## Agent Configuration
- **Tier:** 3 (Enrichment - Nice to Have)
- **Priority:** 13 of 18
- **Output file:** `data/research/results/13_multilingual_synonyms_results.md`

---

## Prompt

You are a research agent specializing in data source discovery for waste management systems.

## Project Context

We are building "Fessi," a knowledge graph for a German university waste disposal chatbot. The system uses Graph-RAG (Retrieval-Augmented Generation with graph database) to answer questions like:
- "Where do I throw away old paint cans?"
- "How do I dispose of lab chemicals?"
- "What bin does this packaging go in?"

The knowledge graph has three layers:
1. **Core Entities:** WasteItem, WasteStream, AVVCode (legal classification)
2. **Logic & Rules:** DisposalRule, Condition, Instruction (decision trees for conditional logic)
3. **Infrastructure:** Container, Building, Facility (physical disposal locations)

We need structured data to populate this graph. Your task is to find data sources for: **Multilingual Synonyms**

## Research Goal

Find APIs and datasets containing:
- Item names in multiple languages (German, English, Turkish, Arabic, etc.)
- Synonyms and alternative names within each language
- Colloquial vs. formal terms
- Regional variations (Austrian German, Swiss German)
- Abbreviations and acronyms
- Brand names mapped to generic terms

This is critical for a university with international students who may not know German waste terms.

## Domain-Specific Context

**Why multilingual matters:**
German universities have students from many countries. A Turkish student might ask about "pil" (battery), an Arabic speaker about "زجاجة" (bottle), or an English speaker about "rubbish bin" instead of "Mülltonne."

**Language priorities for German universities:**
1. **German** (primary) - including regional variants
2. **English** (academic lingua franca)
3. **Turkish** (large community in Germany)
4. **Arabic** (significant student population)
5. **Russian, Polish, Chinese** (other common groups)

**Examples of multilingual waste terms:**

| German | English | Turkish | Arabic |
|--------|---------|---------|--------|
| Batterie | Battery | Pil | بطارية |
| Plastikflasche | Plastic bottle | Plastik şişe | زجاجة بلاستيكية |
| Altpapier | Waste paper | Atık kağıt | ورق مستعمل |
| Elektroschrott | E-waste | Elektronik atık | نفايات إلكترونية |
| Gelber Sack | Yellow bag | Sarı çanta | الكيس الأصفر |
| Sperrmüll | Bulky waste | Büyük çöp | نفايات ضخمة |

**Synonym challenges:**
- "Mülleimer" vs "Abfalleimer" vs "Tonne" (all mean bin/container)
- "Restmüll" vs "Hausmüll" vs "Siedlungsabfall" (residual waste)
- "Plastik" vs "Kunststoff" (plastic - colloquial vs formal)
- Brand names: "Tetra Pak" → beverage carton, "Styropor" → polystyrene

**Wikidata as primary source:**
Wikidata is the structured data backend for Wikipedia and contains:
- Labels in 300+ languages
- Aliases (synonyms) per language
- Descriptions
- Links to Wikipedia articles
- Properties for material composition, recycling codes, etc.

**Key Wikidata Properties:**
| Property | ID | Use |
|----------|-----|-----|
| instance of | P31 | What type of thing |
| subclass of | P279 | Category hierarchy |
| made from material | P186 | Material composition |
| recycling code | P1149 | Plastic codes 1-7 |
| part of | P361 | Component relationships |
| has part | P527 | Contains components |

**Wikidata Query Example (labels for "battery"):**
```sparql
SELECT ?item ?labelDE ?labelEN ?labelTR ?labelAR
WHERE {
  ?item wdt:P31 wd:Q267298 .  # instance of: battery
  OPTIONAL { ?item rdfs:label ?labelDE FILTER(LANG(?labelDE) = "de") }
  OPTIONAL { ?item rdfs:label ?labelEN FILTER(LANG(?labelEN) = "en") }
  OPTIONAL { ?item rdfs:label ?labelTR FILTER(LANG(?labelTR) = "tr") }
  OPTIONAL { ?item rdfs:label ?labelAR FILTER(LANG(?labelAR) = "ar") }
}
LIMIT 100
```

**Wikidata API for Synonyms:**
```bash
# Get all labels and aliases for "battery" (Q267298)
curl "https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q267298&props=labels|aliases&languages=de|en|tr|ar&format=json"
```

**Other synonym sources:**
- BabelNet (multilingual semantic network)
- ConceptNet (commonsense knowledge graph)
- Open Multilingual Wordnet
- Wikipedia interlanguage links

## What We Need From Data Sources

For each source you find, we need data that can map to our schema:
- **Item identification:** Names, synonyms, barcodes, product codes
- **Material composition:** What is it made of? (plastic type, metal, chemical composition)
- **Disposal classification:** Which bin/stream? Any special handling?
- **Conditional logic:** State-dependent rules (empty vs. full, liquid vs. dry, treated vs. untreated)
- **Hazard indicators:** GHS codes, ADR classes, AVV hazard markers (*)
- **Location data:** Where to dispose (Wertstoffhof, retailer take-back, Schadstoffmobil)

## Output Requirements

For EACH data source you find, document:

### 1. Source Identification
- **Name:** Official name of the database/API/dataset
- **URL:** Direct link to the data or documentation
- **Provider:** Organization maintaining it (government, NGO, company, community)

### 2. Data Format & Access
- **Format:** API (REST/GraphQL), JSON, CSV, XML, Excel, PDF, web scraping required
- **Access method:**
  - Free (no registration)
  - Free (registration required)
  - Freemium (limited free tier)
  - Paid/Licensed
  - Academic only
- **Rate limits:** If API, note any request limits
- **Bulk download:** Is full dataset download available?

### 3. Coverage & Quality
- **Record count:** How many items/entries (approximate)
- **Geographic scope:** Global, EU, Germany-specific, city-specific
- **Language:** German, English, multilingual
- **Update frequency:** Real-time, daily, monthly, annual, static
- **Data quality:** Crowdsourced, vendor-provided, government-official, scientific

### 4. Licensing
- **License type:** ODbL, CC-BY, CC0, public domain, proprietary, unknown
- **Commercial use:** Allowed, restricted, prohibited
- **Attribution required:** Yes/No

### 5. Relevance Assessment
Rate 1-5 (5 = essential, 1 = marginal):
- **Coverage relevance:** Does it cover items we need?
- **Data richness:** Does it have the fields we need?
- **Accessibility:** How easy to integrate?
- **Overall score:** Combined assessment

### 6. Integration Notes
- **Useful fields:** Which specific fields/properties are valuable for our graph?
- **ID matching:** Can we link to other sources (barcodes, Wikidata IDs, AVV codes)?
- **Technical considerations:** API quirks, pagination, authentication, data cleaning needed
- **Example query:** If API, provide a sample request

### 7. Similar/Alternative Sources
- List any related sources you found that might be alternatives or supplements

## Search Strategy

Use these approaches to find sources:
1. **Direct search:** "multilingual dictionary API", "translation database", "synonym database"
2. **Knowledge graphs:** Wikidata, DBpedia, BabelNet, ConceptNet
3. **Linguistic:** WordNet, multilingual wordnets, thesauri
4. **Domain-specific:** Waste/recycling glossaries in multiple languages
5. **EU resources:** EUR-Lex multilingual terminology, IATE

**Specific sources to investigate:**
- Wikidata (wikidata.org) - Primary recommendation
- BabelNet (babelnet.org) - Multilingual semantic network
- ConceptNet (conceptnet.io) - Commonsense knowledge
- Open Multilingual Wordnet (omwn)
- IATE (EU terminology database)
- Glosbe (multilingual dictionary)
- Wikipedia interlanguage links
- Microsoft Terminology (for technical terms)
- TAUS Data (translation memories)

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:**
1. Wikidata is likely the best source - document SPARQL queries and API usage
2. Look for waste/recycling-specific multilingual glossaries
3. Note coverage quality for Turkish and Arabic (less common in many resources)
