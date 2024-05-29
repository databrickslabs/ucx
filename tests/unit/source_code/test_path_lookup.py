from pathlib import Path

from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_lookup_is_initialized_with_syspath():
    provider = PathLookup.from_sys_path(Path.cwd())
    assert provider is not None
    paths = list(provider.library_roots)[1:]
    filtered = list(filter(lambda path: "ucx" in path.as_posix(), paths))
    assert len(filtered) > 0


def test_lookup_is_initialized_with_handmade_string():
    provider = PathLookup.from_pathlike_string(Path.cwd(), "what:on:earth")
    assert provider is not None
    cwd, *paths = provider.library_roots
    assert paths == [cwd / "what", cwd / "on", cwd / "earth"]


def test_lookup_inserts_path():
    provider = PathLookup.from_pathlike_string(Path.cwd(), "what:on:earth")
    provider.insert_path(1, Path("is"))
    cwd, *paths = provider.library_roots
    assert paths == [cwd / "what", cwd / "is", cwd / "on", cwd / "earth"]


def test_lookup_removes_path():
    provider = PathLookup.from_pathlike_string(Path.cwd(), "what:is:on:earth")
    provider.remove_path(1)
    cwd, *paths = provider.library_roots
    assert paths == [cwd / "what", cwd / "on", cwd / "earth"]
