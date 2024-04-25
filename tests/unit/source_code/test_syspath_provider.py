from pathlib import Path

from databricks.labs.ucx.source_code.syspath_provider import SysPathProvider


def test_provider_is_initialized():
    provider = SysPathProvider.initialize("what:on:earth")
    assert provider is not None
    paths = list(provider.paths)
    assert ["what", "on", "earth"] == [path.as_posix() for path in paths]


def test_provider_pushes():
    provider = SysPathProvider.initialize("what:on:earth")
    provider.push(Path("is"))
    provider.push(Path("this"))
    paths = list(provider.paths)
    assert [path.as_posix() for path in paths] == ["this", "is", "what", "on", "earth"]


def test_provider_pops():
    provider = SysPathProvider.initialize("what:on:earth")
    popped = provider.pop()
    assert popped.as_posix() == "what"
    paths = list(provider.paths)
    assert [path.as_posix() for path in paths] == ["on", "earth"]
