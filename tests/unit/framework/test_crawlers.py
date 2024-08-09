from collections.abc import Iterable
from dataclasses import dataclass

import pytest
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.framework.crawlers import CrawlerBase, Result, ResultFn


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


class _CrawlerFixture(CrawlerBase[Result]):
    def __init__(
        self,
        backend: MockBackend,
        catalog: str,
        schema: str,
        table: str,
        klass: type[Result],
        *,
        fetcher: ResultFn = lambda: [],
        loader: ResultFn = lambda: [],
    ):
        super().__init__(backend, catalog, schema, table, klass)
        self._fetcher = fetcher
        self._loader = loader

    def snapshot(self) -> Iterable[Result]:
        return self._snapshot(self._fetcher, self._loader)


def test_invalid():
    with pytest.raises(ValueError):
        _CrawlerFixture(MockBackend(), "a.a.a", "b", "c", Bar)


def test_full_name():
    cb = _CrawlerFixture(MockBackend(), "a", "b", "c", Bar)
    assert cb.full_name == "a.b.c"


def test_snapshot_appends_to_existing_table():
    mock_backend = MockBackend()
    cb = _CrawlerFixture[Baz](mock_backend, "a", "b", "c", Baz, loader=lambda: [Baz(first="first")])

    result = cb.snapshot()

    assert [Baz(first="first")] == result
    assert [Row(first="first", second=None)] == mock_backend.rows_written_for("a.b.c", "append")


def test_snapshot_appends_to_new_table():
    mock_backend = MockBackend()

    def fetcher():
        msg = ".. TABLE_OR_VIEW_NOT_FOUND .."
        raise NotFound(msg)

    cb = _CrawlerFixture[Foo](
        mock_backend, "a", "b", "c", Foo, fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)]
    )

    result = cb.snapshot()

    assert [Foo(first="first", second=True)] == result
    assert [Row(first="first", second=True)] == mock_backend.rows_written_for("a.b.c", "append")


def test_snapshot_wrong_error():
    sql_backend = MockBackend()

    def fetcher():
        msg = "always fails"
        raise ValueError(msg)

    cb = _CrawlerFixture[Bar](sql_backend, "a", "b", "c", Bar, fetcher=fetcher)

    with pytest.raises(ValueError):
        cb.snapshot()
