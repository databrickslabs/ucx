from dataclasses import dataclass

import pytest
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.framework.crawlers import CrawlerBase

# pylint: disable=protected-access


@dataclass
class Foo:
    first: str
    second: bool


@dataclass
class Baz:
    first: str
    second: str | None = None


@dataclass
class Bar:
    first: str
    second: bool
    third: float


def test_invalid():
    with pytest.raises(ValueError):
        CrawlerBase(MockBackend(), "a.a.a", "b", "c", Bar)


def test_full_name():
    cb = CrawlerBase(MockBackend(), "a", "b", "c", Bar)
    assert cb.full_name == "a.b.c"


def test_snapshot_appends_to_existing_table():
    mock_backend = MockBackend()
    cb = CrawlerBase[Baz](mock_backend, "a", "b", "c", Baz)

    result = cb._snapshot(fetcher=lambda: [], loader=lambda: [Baz(first="first")])

    assert [Baz(first="first")] == result
    assert [Row(first="first", second=None)] == mock_backend.rows_written_for("a.b.c", "append")


def test_snapshot_appends_to_new_table():
    mock_backend = MockBackend()
    cb = CrawlerBase[Foo](mock_backend, "a", "b", "c", Foo)

    def fetcher():
        msg = ".. TABLE_OR_VIEW_NOT_FOUND .."
        raise NotFound(msg)

    result = cb._snapshot(fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)])

    assert [Foo(first="first", second=True)] == result
    assert [Row(first="first", second=True)] == mock_backend.rows_written_for("a.b.c", "append")


def test_snapshot_wrong_error():
    sql_backend = MockBackend()
    cb = CrawlerBase(sql_backend, "a", "b", "c", Bar)

    def fetcher():
        msg = "always fails"
        raise ValueError(msg)

    with pytest.raises(ValueError):
        cb._snapshot(fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)])
