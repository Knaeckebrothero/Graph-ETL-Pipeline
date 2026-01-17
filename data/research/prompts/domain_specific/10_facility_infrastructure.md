# Research Agent #10: Facility/Infrastructure Locations

## Agent Configuration
- **Tier:** 2 (Domain-Specific - Medium Impact)
- **Priority:** 10 of 18
- **Output file:** `data/research/results/10_facility_infrastructure_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Facility/Infrastructure Locations**

## Research Goal

Find APIs and datasets containing:
- Wertstoffhof (recycling center) locations and accepted materials
- Recycling container locations (glass, paper, clothing)
- Schadstoffmobil (mobile hazardous waste collection) schedules and routes
- Retailer take-back points (batteries, electronics, light bulbs)
- Opening hours and contact information
- Accepted waste types per facility
- Geographic coordinates for mapping

This enables location-based disposal recommendations.

## Domain-Specific Context

**Types of waste infrastructure in Germany:**

| Facility Type | German Term | What's Accepted | Access |
|---------------|-------------|-----------------|--------|
| **Recycling Center** | Wertstoffhof | Most waste types, bulky items, hazardous | Public, often free for residents |
| **Glass Containers** | Altglascontainer | Glass bottles/jars by color | 24/7, street locations |
| **Paper Containers** | Papiercontainer/Papiertonne | Paper and cardboard | Street or household |
| **Clothing Containers** | Altkleidercontainer | Textiles, shoes | Street locations |
| **Battery Collection** | Batterie-Sammelbox | All portable batteries | Supermarkets, electronics retailers |
| **Mobile Hazardous** | Schadstoffmobil/Giftmobil | Paints, solvents, chemicals | Scheduled stops, varies by city |
| **Electronics Take-back** | Elektroaltgeräte-Rücknahme | E-waste | Large retailers (>400m²) |
| **Bulky Waste Pickup** | Sperrmüllabholung | Furniture, large items | Scheduled appointment |

**OpenStreetMap Tags for Waste Infrastructure:**

```
amenity=recycling                    # General recycling point
  recycling_type=centre              # Wertstoffhof (full service)
  recycling_type=container           # Street containers

  # Material acceptance tags:
  recycling:glass=yes
  recycling:glass_bottles=yes
  recycling:paper=yes
  recycling:cardboard=yes
  recycling:plastic=yes
  recycling:plastic_packaging=yes
  recycling:plastic_bottles=yes
  recycling:cans=yes
  recycling:clothes=yes
  recycling:shoes=yes
  recycling:batteries=yes
  recycling:small_appliances=yes
  recycling:electrical_items=yes
  recycling:scrap_metal=yes
  recycling:wood=yes
  recycling:green_waste=yes
  recycling:cooking_oil=yes
  recycling:engine_oil=yes
  recycling:hazardous_waste=yes
  recycling:paint=yes
  recycling:tyres=yes
  recycling:furniture=yes

amenity=waste_disposal               # Larger waste disposal sites
amenity=waste_basket                 # Street litter bins
amenity=waste_transfer_station       # Transfer stations
landuse=landfill                     # Landfill sites

# Additional useful tags:
opening_hours=*                      # Operating hours (OSM format)
operator=*                           # Operating company
phone=*                              # Contact number
website=*                            # More information
fee=yes/no                           # Whether fees apply
access=*                             # Public, private, customers
```

**Data needed per facility:**
- Name and type
- Address / coordinates
- Opening hours
- Contact information (phone, email, website)
- Accepted waste types (list)
- Fees (if any)
- Access restrictions (residents only, commercial, etc.)
- Operator/company

**German municipal waste companies (examples):**
- Berlin: BSR (Berliner Stadtreinigung)
- Munich: AWM (Abfallwirtschaftsbetrieb München)
- Hamburg: SRH (Stadtreinigung Hamburg)
- Frankfurt: FES
- Cologne: AWB
- Stuttgart: AWS
- Düsseldorf: AWISTA

**Known data sources:**
- OpenStreetMap (best coverage, via Overpass API)
- Municipal websites (often list Wertstoffhof locations)
- Waste apps (MyMüll, AbfallPlus) - may have location data
- Google Maps / Apple Maps (less structured)

**Example Overpass API Query (Wertstoffhöfe in Munich):**
```
[out:json][timeout:25];
area[name="München"][admin_level=6]->.searchArea;
(
  node["amenity"="recycling"]["recycling_type"="centre"](area.searchArea);
  way["amenity"="recycling"]["recycling_type"="centre"](area.searchArea);
);
out center;
```

**Example Overpass Query (Glass containers near coordinates):**
```
[out:json][timeout:10];
(
  node["amenity"="recycling"]["recycling:glass"="yes"]
    (around:1000, 48.1351, 11.5820);
);
out;
```

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
1. **Direct search:** "recycling center locations API", "waste facility database Germany"
2. **German-specific:** "Wertstoffhof Standorte", "Recycling Standorte Deutschland"
3. **OpenStreetMap:** Overpass API, taginfo for waste-related tags
4. **Municipal:** City open data portals, waste company websites
5. **Commercial:** Google Places API, Yelp API (for business data)

**Specific sources to investigate:**
- OpenStreetMap / Overpass API (overpass-turbo.eu)
- OSM Taginfo (taginfo.openstreetmap.org) for recycling tag statistics
- Municipal open data portals (Berlin, Munich, Hamburg, etc.)
- MyMüll.de / AbfallPlus location data
- DSD (Grüner Punkt) container locations
- Stiftung EAR retailer take-back database
- GRS Batterien collection point finder
- Lightcycle (light bulb recycling) collection points
- Google Places API (waste facility category)

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:**
1. OpenStreetMap is likely the best source - document the relevant tags thoroughly
2. Look for APIs that include "accepted materials" lists per facility
3. Schadstoffmobil schedules are often hard to find in structured format - note any APIs
