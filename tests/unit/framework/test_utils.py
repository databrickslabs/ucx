import pytest

from databricks.labs.ucx.framework.utils import (
    escape_sql_identifier,
    paginated_fetch_offset,
    paginated_fetch_cursor,
)


@pytest.mark.parametrize(
    "path,expected",
    [
        ("a", "`a`"),
        ("a.b", "`a`.`b`"),
        ("a.b.c", "`a`.`b`.`c`"),
        ("`a`.b.c", "`a`.`b`.`c`"),
        ("a.`b`.c", "`a`.`b`.`c`"),
        ("a.b.`c`", "`a`.`b`.`c`"),
        ("`a.b`.c", "`a`.`b`.`c`"),
        ("a.`b.c`", "`a`.`b`.`c`"),
        ("`a.b`.`c`", "`a`.`b`.`c`"),
        ("`a`.`b.c`", "`a`.`b`.`c`"),
        ("`a`.`b`.`c`", "`a`.`b`.`c`"),
        ("a.b.c.d", "`a`.`b`.`c.d`"),
        ("a-b.c.d", "`a-b`.`c`.`d`"),
        ("a.b-c.d", "`a`.`b-c`.`d`"),
        ("a.b.c-d", "`a`.`b`.`c-d`"),
        ("a.b.c`d", "`a`.`b`.`c``d`"),
        ("✨.🍰.✨", "`✨`.`🍰`.`✨`"),
        ("", ""),
    ],
)
def test_escaped_path(path: str, expected: str) -> None:
    assert escape_sql_identifier(path) == expected


def test_escaped_when_column_contains_period() -> None:
    expected = "`column.with.periods`"
    path = "column.with.periods"
    assert escape_sql_identifier(path, maxsplit=0) == expected


# --- paginated_fetch_offset tests ---


def test_offset_pagination_empty_first_page() -> None:
    """No items returned on the first request."""
    pages: list[dict] = [{"Resources": []}]

    def fetch_page(query: dict) -> dict:
        return pages.pop(0)

    results = list(paginated_fetch_offset(fetch_page, items_key="Resources", page_size=10))
    assert results == []


def test_offset_pagination_single_page() -> None:
    """All items fit in a single page (fewer items than page_size)."""
    items = [{"id": "1"}, {"id": "2"}]
    pages = [{"Resources": items}]

    def fetch_page(query: dict) -> dict:
        return pages.pop(0)

    results = list(paginated_fetch_offset(fetch_page, items_key="Resources", page_size=10))
    assert results == items


def test_offset_pagination_multiple_pages() -> None:
    """Items span multiple pages; pagination advances startIndex correctly."""
    page1 = [{"id": "1"}, {"id": "2"}]
    page2 = [{"id": "3"}]
    pages = [{"Resources": page1}, {"Resources": page2}]
    captured_queries: list[dict] = []

    def fetch_page(query: dict) -> dict:
        captured_queries.append(dict(query))
        return pages.pop(0)

    results = list(paginated_fetch_offset(fetch_page, items_key="Resources", page_size=2))
    assert results == [{"id": "1"}, {"id": "2"}, {"id": "3"}]
    assert captured_queries[0]["startIndex"] == 1
    assert captured_queries[1]["startIndex"] == 3


def test_offset_pagination_terminates_on_missing_key() -> None:
    """Stops when the items key is missing from the response entirely."""
    pages = [{"other_key": "value"}]

    def fetch_page(query: dict) -> dict:
        return pages.pop(0)

    results = list(paginated_fetch_offset(fetch_page, items_key="Resources", page_size=10))
    assert results == []


def test_offset_pagination_respects_start_index() -> None:
    """Custom start_index is passed to the first request."""
    pages = [{"Resources": [{"id": "5"}]}]
    captured_queries: list[dict] = []

    def fetch_page(query: dict) -> dict:
        captured_queries.append(dict(query))
        return pages.pop(0)

    results = list(paginated_fetch_offset(fetch_page, items_key="Resources", page_size=10, start_index=5))
    assert results == [{"id": "5"}]
    assert captured_queries[0]["startIndex"] == 5


# --- paginated_fetch_cursor tests ---


def test_cursor_pagination_empty_first_page() -> None:
    """No items returned on the first request."""
    pages: list[dict] = [{"feature_tables": []}]

    def fetch_page(token: str | None) -> dict:
        return pages.pop(0)

    results = list(paginated_fetch_cursor(fetch_page, items_key="feature_tables"))
    assert results == []


def test_cursor_pagination_single_page_no_token() -> None:
    """All items in one page, no next_page_token in response."""
    items = [{"id": "t1"}, {"id": "t2"}]
    pages = [{"feature_tables": items}]

    def fetch_page(token: str | None) -> dict:
        return pages.pop(0)

    results = list(paginated_fetch_cursor(fetch_page, items_key="feature_tables"))
    assert results == items


def test_cursor_pagination_multiple_pages() -> None:
    """Items span multiple pages with cursor tokens."""
    pages: list[dict] = [
        {"feature_tables": [{"id": "t1"}], "next_page_token": "token_abc"},
        {"feature_tables": [{"id": "t2"}, {"id": "t3"}]},
    ]
    captured_tokens: list[str | None] = []

    def fetch_page(token: str | None) -> dict:
        captured_tokens.append(token)
        return pages.pop(0)

    results = list(paginated_fetch_cursor(fetch_page, items_key="feature_tables"))
    assert results == [{"id": "t1"}, {"id": "t2"}, {"id": "t3"}]
    assert captured_tokens == [None, "token_abc"]


def test_cursor_pagination_custom_token_key() -> None:
    """Supports a custom key name for the next page token."""
    pages: list[dict] = [
        {"items": [{"id": "1"}], "continuation": "xyz"},
        {"items": [{"id": "2"}]},
    ]

    def fetch_page(token: str | None) -> dict:
        return pages.pop(0)

    results = list(paginated_fetch_cursor(fetch_page, items_key="items", next_token_key="continuation"))
    assert results == [{"id": "1"}, {"id": "2"}]


def test_cursor_pagination_terminates_on_missing_items_key() -> None:
    """Stops when the items key is missing from the response."""
    pages = [{"other": "data"}]

    def fetch_page(token: str | None) -> dict:
        return pages.pop(0)

    results = list(paginated_fetch_cursor(fetch_page, items_key="feature_tables"))
    assert results == []
