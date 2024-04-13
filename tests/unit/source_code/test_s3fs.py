import pytest

from databricks.labs.ucx.source_code.base import Advice, Deprecation
from databricks.labs.ucx.source_code.s3fs import S3FSUsageLinter


class TestDetectS3FS:

    class TestDetectS3FS:
        @pytest.mark.parametrize(
            "code, expected",
            [
                (
                    "import s3fs",
                    [
                        Deprecation(
                            code='s3fs-usage',
                            message='The use of s3fs is deprecated',
                            start_line=1,
                            start_col=7,
                            end_line=1,
                            end_col=11,
                        )
                    ],
                ),
                (
                    "from s3fs import something",
                    [
                        Deprecation(
                            code='s3fs-usage',
                            message='The use of s3fs is deprecated',
                            start_line=1,
                            start_col=0,
                            end_line=1,
                            end_col=26,
                        )
                    ],
                ),
                ("import os", []),
                ("from os import path", []),
                (
                    "import s3fs, os",
                    [
                        Deprecation(
                            code='s3fs-usage',
                            message='The use of s3fs is deprecated',
                            start_line=1,
                            start_col=7,
                            end_line=1,
                            end_col=11,
                        )
                    ],
                ),
                ("from os import path, s3fs", []),
                (
                    "def func():\n    import s3fs",
                    [
                        Deprecation(
                            code='s3fs-usage',
                            message='The use of s3fs is deprecated',
                            start_line=2,
                            start_col=11,
                            end_line=2,
                            end_col=15,
                        )
                    ],
                ),
                (
                    "import s3fs as s",
                    [
                        Deprecation(
                            code='s3fs-usage',
                            message='The use of s3fs is deprecated',
                            start_line=1,
                            start_col=7,
                            end_line=1,
                            end_col=11,
                        )
                    ],
                ),
                (
                    "import os, \\\n    s3fs",
                    [
                        Deprecation(
                            code='s3fs-usage',
                            message='The use of s3fs is deprecated',
                            start_line=2,
                            start_col=4,
                            end_line=2,
                            end_col=8,
                        )
                    ],
                ),
                (
                    "from s3fs.subpackage import something",
                    [
                        Deprecation(
                            code='s3fs-usage',
                            message='The use of s3fs is deprecated',
                            start_line=1,
                            start_col=0,
                            end_line=1,
                            end_col=37,
                        )
                    ],
                ),
                ("", []),
            ],
        )
        def test_detect_s3fs_import(self, code: str, expected: list[Advice]):
            linter = S3FSUsageLinter()
            advices = list(linter.lint(code))
            assert advices == expected

    def test_s3fs_name(self):
        linter = S3FSUsageLinter()
        assert linter.name() == 's3fs-usage'
