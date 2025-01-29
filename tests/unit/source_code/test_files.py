from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.files import LocalFile


def test_local_file_language() -> None:
    local_file = LocalFile(Path(), "code", Language.PYTHON)
    assert local_file.language == Language.PYTHON
