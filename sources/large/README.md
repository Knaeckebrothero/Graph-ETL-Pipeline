# Large source files

These files are too large for Git. Their checksums are in [`../manifest.csv`](../manifest.csv); after downloading, compare with `sha256sum`.

| Path | Size | Get it from | Used by |
|---|---|---|---|
| `off_dump/food.parquet` | 4.4 GB | https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet | `src/etl/off_import.py` |
| `off_dump/openfoodfacts-products.jsonl.gz` | 2.8 GB | https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz | not used |
| `dictionaries/de-extract.jsonl.gz` | 293 MB | https://kaikki.org/dictionary/downloads/de/de-extract.jsonl.gz | `src/etl/dictionary_parser.py` |
| `dictionaries/concepts.jsonl` | 186 MB | generated: `python -m src.etl.dictionary` | `src/etl/dictionary_importer.py`, `src/etl/dictionary_classifier.py` |

Upstream files change. A fresh download will not match the stored checksum, so record it as a new snapshot in the manifest. The snapshots used in February 2026 exist only on the desktop machine; copy them to the university server's storage once it is set up.
