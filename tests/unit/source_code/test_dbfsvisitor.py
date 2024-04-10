import pytest

from databricks.labs.ucx.source_code.base import Advice
from databricks.labs.ucx.source_code.dbfsvisitor import DBFSFinderLinter


class TestDetectFSVisitor:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ('"/dbfs/mnt"', 1),
            ('"not a file system path"', 0),
            ('"/dbfs/mnt", "dbfs:/", "/mnt/"', 3),
            ('# "/dbfs/mnt"', 0),
        ],
    )
    def test_detects_fs_paths(self, code, expected):
        finder = DBFSFinderLinter()
        advices = finder.lint(code)
        count = 0
        for advice in advices:
            assert isinstance(advice, Advice)
            count += 1
        assert count == expected
