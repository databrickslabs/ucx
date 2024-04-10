import pytest

from databricks.labs.ucx.source_code.base import Deprecation, Advisory
from databricks.labs.ucx.source_code.dbfs import DBFSUsageLinter


class TestDetectDBFS:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ('"/dbfs/mnt"', 1),
            ('"not a file system path"', 0),
            ('"/dbfs/mnt", "dbfs:/", "/mnt/"', 3),
            ('# "/dbfs/mnt"', 0),
            ('SOME_CONSTANT = "/dbfs/mnt"', 1),
        ],
    )
    def test_detects_dbfs_str_paths(self, code, expected):
        finder = DBFSUsageLinter()
        advices = finder.lint(code)
        count = 0
        for advice in advices:
            assert isinstance(advice, Advisory)
            count += 1
        assert count == expected

    @pytest.mark.parametrize(
        "code, expected",
        [
            ("load_data('/dbfs/mnt/data')", 1),
            ("load_data('/data')", 0),
            ("load_data('/dbfs/mnt/data', '/data')", 1),
            ("# load_data('/dbfs/mnt/data', '/data')", 0),
        ],
    )
    def test_dbfs_usage_linter(self, code, expected):
        linter = DBFSUsageLinter()
        advices = linter.lint(code)
        count = 0
        for advice in advices:
            if isinstance(advice, Deprecation):
                count += 1
        assert count == expected
