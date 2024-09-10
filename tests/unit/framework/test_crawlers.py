from collections.abc import Iterable
from dataclasses import dataclass
from unittest.mock import Mock

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

    def _try_fetch(self) -> Iterable[Result]:
        return self._fetcher()

    def _crawl(self) -> Iterable[Result]:
        return self._loader()


def test_invalid():
    with pytest.raises(ValueError):
        _CrawlerFixture(MockBackend(), "a.a.a", "b", "c", Bar)


def test_full_name():
    cb = _CrawlerFixture(MockBackend(), "a", "b", "c", Bar)
    assert cb.full_name == "a.b.c"


def test_snapshot_crawls_when_no_prior_crawl() -> None:
    """Check that the crawler is invoked when the fetcher reports that the inventory doesn't exist."""
    mock_backend = MockBackend()
    mock_fetcher = Mock(side_effect=NotFound(".. TABLE_OR_VIEW_NOT_FOUND .."))
    mock_loader = Mock(return_value=[Baz(first="first")])
    cb = _CrawlerFixture[Baz](mock_backend, "a", "b", "c", Baz, fetcher=mock_fetcher, loader=mock_loader)

    result = cb.snapshot()

    mock_fetcher.assert_called_once()
    mock_loader.assert_called_once()
    assert [Baz(first="first")] == result


def test_snapshot_crawls_when_prior_crawl_yielded_no_data() -> None:
    """Check that the crawler is invoked when the fetcher reports that the inventory exists but doesn't contain data."""
    mock_backend = MockBackend()
    mock_fetcher = Mock(return_value=[])
    mock_loader = Mock(return_value=[Baz(first="first")])
    cb = _CrawlerFixture[Baz](mock_backend, "a", "b", "c", Baz, fetcher=mock_fetcher, loader=mock_loader)

    result = cb.snapshot()

    mock_fetcher.assert_called_once()
    mock_loader.assert_called_once()
    assert [Baz(first="first")] == result


def test_snapshot_doesnt_crawl_if_previous_crawl_yielded_data() -> None:
    """Check that existing data is used (with no crawl) if the fetcher can load the snapshot data."""
    mock_backend = MockBackend()
    mock_fetcher = Mock(return_value=[Baz(first="first")])
    mock_loader = Mock(return_value=[Baz(first="second")])
    cb = _CrawlerFixture[Baz](mock_backend, "a", "b", "c", Baz, fetcher=mock_fetcher, loader=mock_loader)

    result = cb.snapshot()

    mock_fetcher.assert_called_once()
    mock_loader.assert_not_called()
    assert [Baz(first="first")] == result


def test_snapshot_crawls_if_refresh_forced() -> None:
    """Check that existing data is used (with no crawl) if the fetcher can load the snapshot data."""
    mock_backend = MockBackend()
    mock_fetcher = Mock(return_value=[Baz(first="first")])
    mock_loader = Mock(return_value=[Baz(first="second")])
    cb = _CrawlerFixture[Baz](mock_backend, "a", "b", "c", Baz, fetcher=mock_fetcher, loader=mock_loader)

    result = cb.snapshot(force_refresh=True)

    mock_fetcher.assert_not_called()
    mock_loader.assert_called_once()
    assert [Baz(first="second")] == result


def test_snapshot_force_refresh_replaces_prior_data() -> None:
    mock_backend = MockBackend()
    mock_fetcher = Mock(side_effect=RuntimeError("never called"))
    mock_loader = Mock(return_value=[Baz(first="second")])
    cb = _CrawlerFixture[Baz](mock_backend, "a", "b", "c", Baz, fetcher=mock_fetcher, loader=mock_loader)

    cb.snapshot(force_refresh=True)

    assert [Row(first="second", second=None)] == mock_backend.rows_written_for("a.b.c", mode="overwrite")


def test_snapshot_updates_existing_table() -> None:
    mock_backend = MockBackend()
    cb = _CrawlerFixture[Baz](mock_backend, "a", "b", "c", Baz, loader=lambda: [Baz(first="first")])

    result = cb.snapshot()

    assert [Baz(first="first")] == result
    assert [Row(first="first", second=None)] == mock_backend.rows_written_for("a.b.c", "overwrite")


def test_snapshot_updates_new_table() -> None:
    mock_backend = MockBackend()

    def fetcher():
        msg = ".. TABLE_OR_VIEW_NOT_FOUND .."
        raise NotFound(msg)

    cb = _CrawlerFixture[Foo](
        mock_backend, "a", "b", "c", Foo, fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)]
    )

    result = cb.snapshot()

    assert [Foo(first="first", second=True)] == result
    assert [Row(first="first", second=True)] == mock_backend.rows_written_for("a.b.c", "overwrite")


def test_snapshot_wrong_error() -> None:
    sql_backend = MockBackend()

    def fetcher():
        msg = "always fails"
        raise ValueError(msg)

    cb = _CrawlerFixture[Bar](sql_backend, "a", "b", "c", Bar, fetcher=fetcher)

    with pytest.raises(ValueError):
        cb.snapshot()
