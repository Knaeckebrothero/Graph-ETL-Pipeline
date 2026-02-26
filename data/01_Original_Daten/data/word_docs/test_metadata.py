from PyPDF2 import PdfReader
from datetime import datetime

# PDF-Datei laden
reader = PdfReader("/Users/lukasflaig/Downloads/Code_BSCW/data/pdfs/Abfallsatzung 8_ Aenderungssatzung.pdf")

# Metadaten abrufen
metadata = reader.metadata

# CreationDate extrahieren
creation_date = metadata.get('/CreationDate', None)

# Datum extrahieren und formatieren
if creation_date:
    # Entferne Präfix "D:" und parsen
    creation_date_cleaned = creation_date[2:16]  # Relevant: YYYYMMDDHHmmSS
    dt = datetime.strptime(creation_date_cleaned, "%Y%m%d%H%M%S")
    # Datum formatieren
    formatted_date = dt.strftime("%Y-%m-%d")
    print(f"Erstellungsdatum: {formatted_date}")
else:
    print("Erstellungsdatum nicht in den Metadaten enthalten.")