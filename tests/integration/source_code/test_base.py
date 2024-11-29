from pathlib import Path
from unittest.mock import create_autospec

from databricks.labs.ucx.source_code.base import safe_read_text


def test_safe_read_text_handles_errors() -> None:
    path = create_autospec(Path)
    path.open.side_effect = FileNotFoundError()

    text = safe_read_text(path)

    assert not text
