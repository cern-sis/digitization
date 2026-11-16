CATALOGUE_URL = "https://catalogue.library.cern"
ITEMS_API = f"{CATALOGUE_URL}/api/items/"
EITEMS_API = f"{CATALOGUE_URL}/api/eitems/"
LITERATURE_API = f"{CATALOGUE_URL}/api/literature/"
DOCUMENT_URL = f"{CATALOGUE_URL}/literature/{{pid}}"

ITEMS_PAGE_SIZE = 100
PID_CHUNK_SIZE = 40
MAX_RESULTS_PER_TOPIC = None
PUBLICATION_YEARS_WINDOW = None

EMAIL_TITLE = "Latest books/e-books"
EMAIL_FOOTER = "This is an automated message, please do not reply."

# (topic title, [(subject value prefix, subject scheme or None for any scheme)])
TOPICS = [
    ("Administration/Management", [("65", None), ("005", "UDC")]),
    ("Astronomy/Astrophysics", [("52", None)]),
    ("Engineering/Technology", [("62", None)]),
    (
        "Information Technology",
        [("004", None), ("006", None), ("007", None), ("005", "DEWEY")],
    ),
    ("Mathematics", [("51", None)]),
    ("Physics", [("53", None)]),
]
OTHER_TOPIC = "Other"
