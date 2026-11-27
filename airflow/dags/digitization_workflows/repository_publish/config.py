ATOM_CONN_ID = "atom_conn"
REPOSITORY_CONN_ID = "repository_cern_conn"
S3_CONN_ID = "aws_s3_cern"

# JSON Variable with the settings of the environment (bucket, prefix, community...).
# See run_settings.py.
SETTINGS_VARIABLE = "publish_to_repository"
# The resource type of the records of the CERN Archive communities.
DEFAULT_RESOURCE_TYPE = "publication"
DEFAULT_PUBLISHER = "CERN"
DEFAULT_CREATOR = "CERN"
DEFAULT_MAX_DEPOSITS = 50
# Subfolder of the S3 prefix where the files are moved once linked to AtoM.
DEFAULT_DONE_FOLDER = "migrated"
# Subfolder of the S3 prefix for the run reports (publish_report_<run_id>.json).
DEFAULT_REPORTS_FOLDER = "reports"

# AtoM slugs are lowercase letters, digits and hyphens (used with `re.fullmatch`).
SLUG_PATTERN = r"[a-z0-9]+(?:-[a-z0-9]+)*"
# Stop the run after this many errors in a row (e.g. repository.cern or S3 is down).
MAX_CONSECUTIVE_ERRORS = 5

REQUEST_TIMEOUT = 60
# (connect, read) for file uploads: the read timeout applies between two packets.
UPLOAD_TIMEOUT = (60, 600)

SUBMISSION_MESSAGE = (
    "Digitized file of the CERN Archive, deposited by the publish_to_repository DAG."
)
ACCEPT_MESSAGE = "Accepted by the publish_to_repository DAG."
