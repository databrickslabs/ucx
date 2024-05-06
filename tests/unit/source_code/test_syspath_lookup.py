from pathlib import Path

from databricks.labs.ucx.source_code.syspath_lookup import SysPathLookup


def test_lookup_is_initialized_with_syspath():
    lookup = SysPathLookup.from_sys_path(Path.cwd())
    assert lookup is not None
    paths = list(lookup.paths)[1:]
    filtered = list(filter(lambda path: "ucx" in path.as_posix(), paths))
    assert len(filtered) > 0


def test_lookup_is_initialized_with_handmade_string():
    lookup = SysPathLookup.from_pathlike_string(Path.cwd(), "what:on:earth")
    assert lookup is not None
    paths = list(lookup.paths)[1:]
    assert ["what", "on", "earth"] == [path.as_posix() for path in paths]


def test_lookup_prepends_path():
    lookup = SysPathLookup.from_pathlike_string(Path.cwd(), "what:on:earth")
    lookup.prepend_path(Path("is"))
    lookup.prepend_path(Path("this"))
    paths = list(lookup.paths)[1:]
    assert [path.as_posix() for path in paths] == ["this", "is", "what", "on", "earth"]


def test_lookup_appends_path():
    lookup = SysPathLookup.from_pathlike_string(Path.cwd(), "what:on:earth")
    lookup.append_path(Path("is"))
    lookup.append_path(Path("this"))
    paths = list(lookup.paths)[1:]
    assert [path.as_posix() for path in paths] == ["what", "on", "earth", "is", "this"]


def test_lookup_inserts_path():
    lookup = SysPathLookup.from_pathlike_string(Path.cwd(), "what:on:earth")
    lookup.insert_path(1, Path("is"))
    paths = list(lookup.paths)[1:]
    assert [path.as_posix() for path in paths] == ["what", "is", "on", "earth"]


def test_lookup_removes_path():
    lookup = SysPathLookup.from_pathlike_string(Path.cwd(), "what:is:on:earth")
    lookup.remove_path(1)
    paths = list(lookup.paths)[1:]
    assert [path.as_posix() for path in paths] == ["what", "on", "earth"]


def test_lookup_sets_cwd():
    lookup = SysPathLookup.from_sys_path(Path.cwd())
    location = Path("some-location")
    lookup.cwd = location
    assert lookup.cwd == location
