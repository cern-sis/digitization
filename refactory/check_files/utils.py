import os
from pypdf import PdfReader
from pypdf.errors import PdfReadError

def validate_pdf(file_path: str) -> bool:
    """Checks if a local PDF is structurally valid and readable."""
    try:
        file_size = os.path.getsize(file_path)
        if file_size < 100:
            return False

        with open(file_path, "rb") as f:
            header = f.read(8)
            f.seek(-min(1024, file_size), 2)
            trailer = f.read()

        if not header.startswith(b"%PDF-"):
            return False
        if b"%%EOF" not in trailer:
            return False

        reader = PdfReader(file_path)
        if len(reader.pages) == 0:
            return False

        _ = reader.pages[0]

        return True

    except OSError as e:
        raise RuntimeError(f"System error when accessing file {file_path}: {e}") from e

    except (PdfReadError, Exception):
        return False
