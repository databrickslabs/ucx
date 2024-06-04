from pathlib import Path

from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_lookup_is_initialized_with_syspath():
    provider = PathLookup.from_sys_path(Path.cwd())
    assert provider is not None
    paths = list(provider.library_roots)[1:]
    filtered = list(filter(lambda path: "ucx" in path.as_posix(), paths))
    assert len(filtered) > 0


def test_lookup_is_initialized_with_handmade_string(tmp_path):
    directories, sys_paths = ("what", "on", "earth"), []
    for directory in directories:
        path = tmp_path / directory
        path.mkdir()
        sys_paths.append(path)

    provider = PathLookup.from_pathlike_string(Path.cwd(), ":".join([p.as_posix() for p in sys_paths]))

    assert provider is not None
    assert provider.library_roots[1:] == sys_paths


def test_lookup_inserts_path(tmp_path):
    directories, sys_paths = ("what", "on", "earth"), []
    for directory in directories:
        path = tmp_path / directory
        path.mkdir()
        sys_paths.append(path)

    provider = PathLookup.from_pathlike_string(Path.cwd(), ":".join([p.as_posix() for p in sys_paths]))

    new_sys_path = tmp_path / Path("is")
    new_sys_path.mkdir()
    provider.insert_path(1, new_sys_path)

    assert provider.library_roots[1:] == [sys_paths[0]] + [new_sys_path] + sys_paths[1:]


def test_lookup_removes_path(tmp_path):
    directories, sys_paths = ("what", "is", "on", "earth"), []
    for directory in directories:
        path = tmp_path / directory
        path.mkdir()
        sys_paths.append(path)

    provider = PathLookup.from_pathlike_string(Path.cwd(), ":".join([p.as_posix() for p in sys_paths]))
    provider.remove_path(1)
    sys_paths.pop(1)
    assert provider.library_roots[1:] == sys_paths
