import ast


def parse_inventory(value):
    """
    Parses the input to identify if it's a literal list,
    a range of IDs (1..10), or a single string/ID.
    """
    if isinstance(value, int) or str(value).isdigit():
        return [int(value)]

    if str(value).startswith("[") and str(value).endswith("]"):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            raise ValueError("Invalid list format. Use '[1, 2, 3]'")

    if ".." in value:
        try:
            start, end = map(int, value.split(".."))
            return list(range(start, end + 1))
        except ValueError:
            raise ValueError("Invalid range format. Use 'start..end' (e.g., 1..10)")

    return value
