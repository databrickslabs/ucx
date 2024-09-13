import logging
import subprocess

logger = logging.getLogger(__name__)


def escape_sql_identifier(path: str, *, maxsplit: int = 2) -> str:
    """
    Escapes the path components to make them SQL safe.

    Args:
        path (str): The dot-separated path of a catalog object.
        maxsplit (int): The maximum number of splits to perform.

    Returns:
         str: The path with all parts escaped in backticks.
    """
    if not path:
        return path
    parts = path.split(".", maxsplit=maxsplit)
    escaped = [f"`{part.strip('`').replace('`', '``')}`" for part in parts]
    return ".".join(escaped)


def run_command(command: str | list[str]) -> tuple[int, str, str]:
    args = command.split() if isinstance(command, str) else command
    logger.info(f"Invoking command: {args!r}")
    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        output, error = process.communicate()
        return process.returncode, output.decode("utf-8"), error.decode("utf-8")
