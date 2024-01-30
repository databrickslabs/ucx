import string

_allowed_object_chars = set(string.ascii_letters + string.digits + '_')


def escape_sql_identifier(path: str, optional: bool | None = True) -> str:
    """
    Escapes the path components to make them SQL safe.

    Args:
        path (str): The dot-separated path of a catalog object.
        optional (bool): If `True` then do not escape if no special characters are present.

    Returns:
         str: The path with all parts escaped in backticks.
    """
    parts = path.split(".", maxsplit=2)
    escaped = []
    for part in parts:
        if not part.startswith("`") and not part.endswith("`"):
            part = part.strip("`")
            if not optional or not set(part) <= _allowed_object_chars:
                part = f"`{part}`"
        escaped.append(part)
    return ".".join(escaped)
