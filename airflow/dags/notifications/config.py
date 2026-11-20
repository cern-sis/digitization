# Same catalogue access as the BI DAGs (bi-dags): the catalogue URL is the host of the
# connection, and the backoffice API token is in an Airflow Variable.
CATALOGUE_CONN_ID = "library_catalog_conn"
CATALOGUE_TOKEN_VARIABLE = "CATALOG_API_TOKEN"
# Only used to warn when the connection points to another catalogue (e.g. the sandbox).
PRODUCTION_CATALOGUE_URL = "https://catalogue.library.cern"

ITEMS_PAGE_SIZE = 100
PID_CHUNK_SIZE = 40
MAX_RESULTS_PER_TOPIC = None
PUBLICATION_YEARS_WINDOW = None

EMAIL_TITLE = "Latest books/e-books"
EMAIL_FOOTER = "This is an automated message, please do not reply."

SENT_HISTORY_VARIABLE = "library_newsletter_sent"
SENT_HISTORY_DAYS = 60

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
