from pathlib import Path
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import safe_read_text


@pytest.mark.parametrize(
    "exception",
    [
        FileNotFoundError(),
        UnicodeDecodeError("utf-8", b"\x80\x81\x82", 0, 1, "invalid start byte"),
        PermissionError(),
    ],
)
def test_safe_read_text_handles_errors(exception: Exception) -> None:
    path = create_autospec(Path)
    path.open.side_effect = exception

    text = safe_read_text(path)

    assert not text
