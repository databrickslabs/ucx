from dataclasses import dataclass

import pytest

from databricks.labs.ucx.tacl._internal import CrawlerBase


@dataclass
class Foo:
    first: str
    second: bool


def test_invalid(mock_backend):
    with pytest.raises(ValueError):
        CrawlerBase(mock_backend, "a.a.a", "b", "c")


def test_full_name(mock_backend):
    cb = CrawlerBase(mock_backend, "a", "b", "c")
    assert "a.b.c" == cb._full_name


def test_snapshot_appends_to_existing_table(mock_backend):
    cb = CrawlerBase(mock_backend, "a", "b", "c")
    runs = []

    def fetcher():
        if len(runs) == 0:
            runs.append(1)
            msg = "TABLE_OR_VIEW_NOT_FOUND"
            raise RuntimeError(msg)
        return []

    cb._snapshot(Foo, fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)])

    insert = "INSERT INTO a.b.c (first, second) VALUES ('first', TRUE)"
    assert len(mock_backend.queries) == 1
    assert insert == mock_backend.queries[0]


def test_snapshot_appends_to_new_table(mock_backend):
    mock_backend._fails_on_first = {"INSERT INTO a.b.c": "TABLE_OR_VIEW_NOT_FOUND ..."}
    cb = CrawlerBase(mock_backend, "a", "b", "c")
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
    assert len(mock_backend.queries) == 3
    assert insert == mock_backend.queries[0]
    assert create == mock_backend.queries[1]
    assert insert == mock_backend.queries[2]
