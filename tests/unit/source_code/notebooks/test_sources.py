import locale
import tempfile
from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter


@pytest.mark.parametrize("path, content", [("xyz.py", "a = 3"), ("xyz.sql", "select * from dual")])
def test_file_linter_lints_supported_language(path, content, migration_index):
    linter = FileLinter(LinterContext(migration_index), Path(path), content)
    advices = list(linter.lint())
    assert not advices


@pytest.mark.parametrize("path", ["xyz.scala", "xyz.r", "xyz.sh"])
def test_file_linter_lints_not_yet_supported_language(path, migration_index):
    linter = FileLinter(LinterContext(migration_index), Path(path), "")
    advices = list(linter.lint())
    assert [advice.code for advice in advices] == ["unsupported-language"]


class FriendFileLinter(FileLinter):

    def source_code(self):
        return self._source_code


def test_checks_encoding_of_pseudo_file(migration_index):
    linter = FriendFileLinter(LinterContext(migration_index), Path("whatever"), "a=b")
    assert linter.source_code() == "a=b"


def test_checks_encoding_of_file_with_utf8_bom(migration_index, mock_path_lookup):
    path = mock_path_lookup.resolve(Path("file_with_bom.py"))
    linter = FriendFileLinter(LinterContext(migration_index), path)
    assert linter.source_code() is not None


def test_checks_encoding_of_file_with_utf16_le_bom(migration_index):
    with (tempfile.NamedTemporaryFile() as tf):
        data = bytearray()
        data.append(0xFF)
        data.append(0xFE)
        for b in "a = 12".encode('utf-16-le'):
            data.append(b)
        tf.write(data)
        tf.flush()
        linter = FriendFileLinter(LinterContext(migration_index), Path(tf.name))
        assert linter.source_code() == "a = 12"


def test_checks_encoding_of_file_with_utf16_be_bom(migration_index):
    with (tempfile.NamedTemporaryFile() as tf):
        data = bytearray()
        data.append(0xFE)
        data.append(0xFF)
        for b in "a = 12".encode('utf-16-be'):
            data.append(b)
        tf.write(data)
        tf.flush()
        linter = FriendFileLinter(LinterContext(migration_index), Path(tf.name))
        assert linter.source_code() == "a = 12"


@pytest.mark.parametrize(
    "path",
    [
        "xyz.json",
        "xyz.xml",
        "xyz.yml",
        "xyz.cfg",
        "xyz.md",
        "xyz.txt",
        "xyz.gif",
        "xyz.png",
        "xyz.jpg",
        "xyz.jpeg",
        "xyz.tif",
        "xyz.bmp",
        "xyz.toml",
        ".DS_Store",  # on MacOS
    ],
)
def test_file_linter_lints_ignorable_language(path, migration_index):
    linter = FileLinter(LinterContext(migration_index), Path(path), "")
    advices = list(linter.lint())
    assert not advices


def test_file_linter_lints_non_ascii_encoded_file(migration_index):
    preferred_encoding = locale.getpreferredencoding(False)
    non_ascii_encoded_file = Path(__file__).parent.parent / "samples" / "nonascii.py"
    linter = FileLinter(LinterContext(migration_index), non_ascii_encoded_file)

    advices = list(linter.lint())

    assert len(advices) == 1
    assert advices[0].code == "unsupported-file-encoding"
    assert advices[0].message == f"File without {preferred_encoding} encoding is not supported {non_ascii_encoded_file}"
