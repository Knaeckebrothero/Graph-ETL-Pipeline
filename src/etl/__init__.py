"""
ETL Pipeline for Fessi Knowledge Graph.

Modules:
    facilities: Import disposal facilities from JSON
    waste_items: Import waste items from CSV
    materials: Import material taxonomy from JSON (OFF packaging tags)
    products: Import products from BarcodeResolver JSON (EAN → packaging graph)
    off_import: Bulk import products from Open Food Facts Parquet dump
    ecolabel: Import EU Ecolabel eco-certified products (API → Product nodes)
    open_beauty_fetch: Fetch Open Beauty Facts dump → ProductInfo JSON
    opengtindb_fetch: Fetch opengtindb.org products → ProductInfo JSON
    dictionary: Download and parse dictionary data into concept clusters
    dictionary_importer: Bulk import dictionary clusters into Neo4j
    dictionary_classifier: LLM-based classification + promotion to WasteItem
"""

def __getattr__(name: str):
    """Lazy imports to avoid RuntimeWarning when running modules with python -m."""
    _imports = {
        'import_facilities': ('.facilities', 'import_facilities'),
        'import_waste_items': ('.waste_items', 'import_waste_items'),
        'import_materials': ('.materials', 'import_materials'),
        'import_products': ('.products', 'import_products'),
        'import_off_products': ('.off_import', 'import_off_products'),
        'import_ecolabel': ('.ecolabel', 'import_ecolabel'),
        'fetch_ecolabel_products': ('.ecolabel', 'fetch_ecolabel_products'),
        'parse_dictionaries': ('.dictionary', 'parse_dictionaries'),
        'import_dictionary_entries': ('.dictionary_importer', 'import_dictionary_entries'),
        'classify_dictionary_entries': ('.dictionary_classifier', 'classify_dictionary_entries'),
    }
    if name in _imports:
        module_path, attr = _imports[name]
        from importlib import import_module
        mod = import_module(module_path, __package__)
        return getattr(mod, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    'import_facilities',
    'import_waste_items',
    'import_materials',
    'import_products',
    'import_off_products',
    'import_ecolabel',
    'fetch_ecolabel_products',
    'parse_dictionaries',
    'import_dictionary_entries',
    'classify_dictionary_entries',
]
