import codecs
import logging
from pathlib import Path
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import read_text, safe_read_text


@pytest.mark.parametrize(
    "exception",
    [
        FileNotFoundError(),
        UnicodeDecodeError("utf-8", b"\x80\x81\x82", 0, 1, "invalid start byte"),
        PermissionError(),
    ],
)
def test_safe_read_text_handles_and_warns_read_errors(caplog, exception: Exception) -> None:
    path = create_autospec(Path)
    path.open.side_effect = exception

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.aggregate"):
        text = safe_read_text(path)

    assert not text
    assert f"Could not read file: {path}" in caplog.messages


@pytest.mark.parametrize(
    "bom, encoding",
    [
        (codecs.BOM_UTF8, "utf-8"),
        (codecs.BOM_UTF16_LE, "utf-16-le"),
        (codecs.BOM_UTF16_BE, "utf-16-be"),
        (codecs.BOM_UTF32_LE, "utf-32-le"),
        (codecs.BOM_UTF32_BE, "utf-32-be"),
    ],
)
def test_read__encoded_file_with_bom(tmp_path, bom, encoding) -> None:
    path = tmp_path / "file.py"
    path.write_bytes(bom + "a = 12".encode(encoding))

    text = read_text(path)

    assert text == "a = 12"
