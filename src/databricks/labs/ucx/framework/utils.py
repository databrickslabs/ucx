import logging
import string
import subprocess

logger = logging.getLogger(__name__)

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


def run_command(command: str | list[str]) -> tuple[int, str, str]:
    args = command.split() if isinstance(command, str) else command
    logger.info(f"Invoking command: {args!r}")
    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        output, error = process.communicate()
        return process.returncode, output.decode("utf-8"), error.decode("utf-8")
