from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.files import LocalFile


def test_local_file_path_is_accessible() -> None:
    local_file = LocalFile(Path("test.py"), "print(1)", Language.PYTHON)
    assert local_file.path == Path("test.py")
