from dataclasses import dataclass

import pytest
from unit.framework.mocks import MockBackend

from databricks.labs.ucx.framework.crawlers import CrawlerBase


@dataclass
class Foo:
    first: str
    second: bool


@dataclass
class Bar:
    first: str
    second: bool
    third: float


def test_invalid():
    with pytest.raises(ValueError):
        CrawlerBase(MockBackend(), "a.a.a", "b", "c")


def test_full_name():
    cb = CrawlerBase(MockBackend(), "a", "b", "c")
    assert "a.b.c" == cb._full_name


def test_snapshot_appends_to_existing_table():
    b = MockBackend()
    cb = CrawlerBase(b, "a", "b", "c")
    runs = []

    def fetcher():
        if len(runs) == 0:
            runs.append(1)
            msg = "TABLE_OR_VIEW_NOT_FOUND"
            raise RuntimeError(msg)
        return []

    cb._snapshot(Foo, fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)])

    insert = "INSERT INTO a.b.c (first, second) VALUES ('first', TRUE)"
    assert len(b.queries) == 1
    assert insert == b.queries[0]


def test_snapshot_appends_incorrect_type():
    b = MockBackend()
    cb = CrawlerBase(b, "a", "b", "c")
    runs = []

    def fetcher():
        if len(runs) == 0:
            runs.append(1)
            msg = "TABLE_OR_VIEW_NOT_FOUND"
            raise RuntimeError(msg)
        return []

    with pytest.raises(ValueError):
        cb._snapshot(Bar, fetcher=fetcher, loader=lambda: [Bar(first="first", second=True, third=3.14)])


def test_snapshot_appends_to_new_table():
    b = MockBackend(fails_on_first={"INSERT INTO a.b.c": "TABLE_OR_VIEW_NOT_FOUND ..."})
    cb = CrawlerBase(b, "a", "b", "c")
    runs = []

    def fetcher():
        if len(runs) == 0:
            runs.append(1)
            msg = "TABLE_OR_VIEW_NOT_FOUND"
            raise RuntimeError(msg)
        return []

    cb._snapshot(Foo, fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)])

    insert = "INSERT INTO a.b.c (first, second) VALUES ('first', TRUE)"
    create = "CREATE TABLE a.b.c (first STRING, second BOOLEAN) USING DELTA"
    assert len(b.queries) == 3
    assert insert == b.queries[0]
    assert create == b.queries[1]
    assert insert == b.queries[2]


def test_snapshot_wrong_error():
    b = MockBackend(fails_on_first={"INSERT INTO a.b.c": "TABLE_NOT_FOUND ..."})
    cb = CrawlerBase(b, "a", "b", "c")
    runs = []

    def fetcher():
        if len(runs) == 0:
            runs.append(1)
            msg = "TABLE_OR_VIEW_NOT_FOUND"
            raise RuntimeError(msg)
        return []

    with pytest.raises(RuntimeError):
        cb._snapshot(Foo, fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)])
