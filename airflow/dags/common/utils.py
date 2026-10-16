import ast
from urllib.parse import urlparse


def parse_inventory(value):
    """
    Parses the input to identify if it's a literal list,
    a range of IDs (1..10), or a single string/ID.
    """
    if isinstance(value, int) or str(value).isdigit():
        return [int(value)]

    str_value = str(value)

    if "[" in str_value or "]" in str_value:
        if not (str_value.startswith("[") and str_value.endswith("]")):
            raise ValueError("Invalid list format. Use '[1, 2, 3]'")
        try:
            parsed = ast.literal_eval(str_value)
        except (ValueError, SyntaxError):
            raise ValueError("Invalid list format. Use '[1, 2, 3]'")
        if not isinstance(parsed, list) or not all(
            isinstance(item, int) for item in parsed
        ):
            raise ValueError(
                "Invalid list format. Use '[1, 2, 3]' (a flat list of integers)"
            )
        return parsed

    if ".." in value:
        try:
            start, end = map(int, value.split(".."))
            return list(range(start, end + 1))
        except ValueError:
            raise ValueError("Invalid range format. Use 'start..end' (e.g., 1..10)")

    return value


def parse_cernbox_url(url: str) -> dict:
    parsed = urlparse(url)
    path = parsed.path

    if "/s/" in path:
        hash_code = path.split("/s/")[-1].split("/")[0]
        return {"public_link_hash": hash_code, "eos_path": None}
    elif "public-files" in path:
        hash_code = path.split("public-files/")[-1].split("/")[0]
        return {"public_link_hash": hash_code, "eos_path": None}
    elif "eos" in path:
        eos_path = "eos" + path.split("eos")[-1]
        return {"public_link_hash": None, "eos_path": eos_path}
    else:
        raise ValueError(f"Invalid CERNBox URL format: {url}")
