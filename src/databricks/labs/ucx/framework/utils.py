import logging
import subprocess
from collections.abc import Callable, Iterator

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


def paginated_fetch_offset(
    fetch_page: Callable[[int, int], dict],
    items_key: str,
    page_size: int,
    start_index: int = 1,
) -> Iterator[dict]:
    """Paginate a SCIM-style offset API (startIndex / count).

    Args:
        fetch_page: Callable that takes (start_index, count) and returns a response dict.
        items_key: Key in the response containing the list of items (e.g. "Resources").
        page_size: Number of items to request per page.
        start_index: 1-based index to start from (SCIM default is 1).

    Yields:
        Individual raw item dicts from each page.
    """
    while True:
        response = fetch_page(start_index, page_size)
        items = response.get(items_key, [])
        if not items:
            break
        yield from items
        if len(items) < page_size:
            break
        start_index += len(items)


def paginated_fetch_cursor(
    fetch_page: Callable[[str | None], dict],
    items_key: str,
    next_token_key: str = "next_page_token",
) -> Iterator[dict]:
    """Paginate a cursor/token-based API.

    Args:
        fetch_page: Callable that takes an optional page token and returns a response dict.
        items_key: Key in the response containing the list of items.
        next_token_key: Key in the response containing the next page token.

    Yields:
        Individual raw item dicts from each page.
    """
    token: str | None = None
    while True:
        response = fetch_page(token)
        items = response.get(items_key, [])
        yield from items
        token = response.get(next_token_key)
        if not token:
            break


def run_command(command: str | list[str]) -> tuple[int, str, str]:
    args = command.split() if isinstance(command, str) else command
    logger.info(f"Invoking command: {args!r}")
    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        output, error = process.communicate()
        return process.returncode, output.decode("utf-8"), error.decode("utf-8")
