from pathlib import Path

from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_lookup_is_initialized_with_syspath():
    provider = PathLookup.from_sys_path()
    assert provider is not None
    paths = list(provider.paths)
    filtered = list(filter(lambda path: "ucx" in path.as_posix(), paths))
    assert len(filtered) > 0


def test_lookup_is_initialized_with_handmade_string():
    provider = PathLookup.from_pathlike_string("what:on:earth")
    assert provider is not None
    paths = list(provider.paths)
    assert ["what", "on", "earth"] == [path.as_posix() for path in paths]


def test_lookup_pushes_path():
    provider = PathLookup.from_pathlike_string("what:on:earth")
    provider.push_path(Path("is"))
    provider.push_path(Path("this"))
    paths = list(provider.paths)
    assert [path.as_posix() for path in paths] == ["this", "is", "what", "on", "earth"]


def test_lookup_inserts_path():
    provider = PathLookup.from_pathlike_string("what:on:earth")
    provider.insert_path(1, Path("is"))
    paths = list(provider.paths)
    assert [path.as_posix() for path in paths] == ["what", "is", "on", "earth"]


def test_lookup_removes_path():
    provider = PathLookup.from_pathlike_string("what:is:on:earth")
    provider.remove_path(1)
    paths = list(provider.paths)
    assert [path.as_posix() for path in paths] == ["what", "on", "earth"]


def test_lookup_pops_path():
    provider = PathLookup.from_pathlike_string("what:on:earth")
    popped = provider.pop_path()
    assert popped.as_posix() == "what"
    paths = list(provider.paths)
    assert [path.as_posix() for path in paths] == ["on", "earth"]


def test_lookup_pushes_cwd():
    provider = PathLookup.from_sys_path()
    location = Path("some-location")
    provider.push_cwd(location)
    assert provider.cwd == location
