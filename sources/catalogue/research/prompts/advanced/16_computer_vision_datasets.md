# Research Agent #16: Computer Vision Datasets

## Agent Configuration
- **Tier:** 4 (Advanced Features)
- **Priority:** 16 of 18
- **Output file:** `data/research/results/16_computer_vision_datasets_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Computer Vision Datasets**

## Research Goal

Find image datasets and pre-trained models for:
- Waste item classification (what type of waste is this?)
- Material recognition (plastic, metal, glass, paper, organic)
- Litter detection in outdoor environments
- Construction/demolition waste identification
- Recycling bin sorting
- Object detection and segmentation for waste items

This enables camera-based waste identification as a future feature.

## Domain-Specific Context

**Use cases for computer vision in waste disposal:**

1. **Photo-based queries:** User takes photo of item → system identifies it → provides disposal instructions
2. **Volume estimation:** Estimate pile size for disposal cost calculation
3. **Contamination detection:** Identify "pure" vs "contaminated" rubble
4. **Sorting assistance:** Help users sort items at recycling stations

**Types of CV tasks:**

| Task | Description | Use Case |
|------|-------------|----------|
| Image Classification | Single label per image | "This is a plastic bottle" |
| Object Detection | Bounding boxes | Find multiple items in photo |
| Semantic Segmentation | Pixel-level classification | Precise material boundaries |
| Instance Segmentation | Individual object masks | Count separate items |

**Key dataset characteristics to evaluate:**

- **Size:** Number of images and annotations
- **Classes:** What waste categories are covered
- **Annotation type:** Classification, bounding boxes, segmentation masks
- **Environment:** Studio (controlled) vs. wild (real-world)
- **Quality:** Professional vs. crowdsourced annotations
- **Format:** COCO, YOLO, Pascal VOC, TFRecord, custom

**Known datasets:**

### TACO (Trash Annotations in Context)
- **URL:** tacodataset.org
- **Size:** ~1,500 images, 4,784 annotations
- **Classes:** 60 waste categories
- **Type:** Instance segmentation (COCO format)
- **Environment:** Outdoor/wild
- **License:** CC BY 4.0
- **Limitation:** Small, class imbalance

### TrashNet
- **URL:** github.com/garythung/trashnet
- **Size:** 2,527 images
- **Classes:** 6 (glass, paper, cardboard, plastic, metal, trash)
- **Type:** Classification
- **Environment:** Studio (white background)
- **License:** MIT
- **Limitation:** Very small, not realistic

### WasteNet (Recycleye)
- **Size:** Millions of images (claimed)
- **Focus:** MRF conveyor belt sorting
- **Access:** Commercial/partnership
- **Limitation:** Not publicly available

### Roboflow Universe
- **URL:** universe.roboflow.com
- **Content:** Many community-uploaded waste datasets
- **Search terms:** "waste", "trash", "recycling", "garbage", "litter", "construction waste"
- **Formats:** YOLO, COCO, TFRecord, etc.

**Waste classification taxonomies for CV:**

Simple (6 classes - TrashNet style):
```
1. Glass
2. Paper
3. Cardboard
4. Plastic
5. Metal
6. Trash (other)
```

Detailed (TACO-style, 60 classes):
```
Bottles, Cans, Cups, Lids, Bags, Wrappers, Containers,
Cigarettes, Food waste, Paper, Cardboard, Styrofoam,
Electronics, Batteries, Glass fragments, Metal scraps, etc.
```

Material-based (for recycling):
```
PET, HDPE, PVC, LDPE, PP, PS, Other plastic
Clear glass, Green glass, Brown glass
Aluminum, Steel/Tin
Paper, Cardboard
Organic/Compostable
```

**Construction waste specific:**
```
Concrete, Brick, Tile, Wood, Metal, Glass,
Drywall, Insulation, Roofing, Mixed rubble
```

**Dataset quality considerations:**
- Real-world images >> studio images for deployment
- Diverse lighting, angles, backgrounds needed
- Class balance important for training
- German/European waste items may differ from US/Asian datasets
- Seasonal variations (more organic waste in summer)

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
1. **Direct search:** "waste classification dataset", "trash detection dataset", "recycling image dataset"
2. **Academic:** Google Scholar, Papers with Code, arXiv
3. **Platforms:** Roboflow Universe, Kaggle, HuggingFace Datasets
4. **Research groups:** University waste/recycling research projects
5. **Industry:** Waste management company datasets, sorting technology companies

**Specific sources to investigate:**
- TACO Dataset (tacodataset.org)
- TrashNet (github.com/garythung/trashnet)
- Roboflow Universe (search: waste, trash, recycling, garbage, litter, CDW)
- Kaggle (search: waste classification, garbage, recycling)
- HuggingFace Datasets (search: waste, trash)
- Papers with Code (waste classification benchmarks)
- Google Open Images (waste-related classes)
- COCO dataset (relevant classes)
- ADE20K (scene understanding, may include waste)
- Cityscapes (urban scenes, litter)

**Also look for:**
- Pre-trained models (not just datasets)
- Synthetic data generation tools
- Data augmentation pipelines for waste
- Active learning platforms for waste annotation

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:**
1. Prioritize datasets with real-world images over studio shots
2. Note annotation quality and class definitions
3. Look for European/German waste-specific datasets
4. Identify construction/demolition waste datasets specifically
5. Note any pre-trained models that could be fine-tuned
