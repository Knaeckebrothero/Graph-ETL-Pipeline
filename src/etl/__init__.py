"""
ETL Pipeline for Fessi Knowledge Graph.

Modules:
    facilities: Import disposal facilities from JSON
    waste_items: Import waste items from CSV
    materials: Import material taxonomy from JSON (OFF packaging tags)
    products: Import products from BarcodeResolver JSON (EAN → packaging graph)
    dictionary: Download and parse dictionary data into concept clusters
    dictionary_importer: Bulk import dictionary clusters into Neo4j
    dictionary_classifier: LLM-based classification + promotion to WasteItem
"""

from .facilities import import_facilities
from .waste_items import import_waste_items
from .materials import import_materials
from .products import import_products
from .dictionary import parse_dictionaries
from .dictionary_importer import import_dictionary_entries
from .dictionary_classifier import classify_dictionary_entries

__all__ = [
    'import_facilities',
    'import_waste_items',
    'import_materials',
    'import_products',
    'parse_dictionaries',
    'import_dictionary_entries',
    'classify_dictionary_entries',
]
