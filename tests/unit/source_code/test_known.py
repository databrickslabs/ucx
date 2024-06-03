from pathlib import Path
from unittest import mock

import pytest

from databricks.labs.ucx.source_code.known import Whitelist


def test_checks_compatibility():
    known = Whitelist()
    spark_sql = known.module_compatibility("spark.sql")
    assert not spark_sql.known

    sdk_compute = known.module_compatibility("databricks.sdk.service.compute")
    assert sdk_compute.known
    assert not sdk_compute.problems

    assert known.module_compatibility("sys").known

    os_path = known.module_compatibility("os.path")
    assert os_path.known
    assert not os_path.problems


def test_checks_library_compatibility():
    known = Whitelist()

    sklearn = known.distribution_compatibility("scikit-learn")
    assert sklearn.known
    assert not sklearn.problems

    s3fs = known.distribution_compatibility("s3fs")
    assert s3fs.known
    assert len(s3fs.problems) == 1


def test_loads_known_json():
    known_json = Whitelist._get_known()  # pylint: disable=protected-access
    assert known_json is not None and len(known_json) > 0


def test_error_on_missing_known_json():
    with (
        mock.patch("pkgutil.get_data", side_effect=FileNotFoundError("simulate missing file")),
        pytest.raises(FileNotFoundError),
    ):
        Whitelist._get_known()  # pylint: disable=protected-access


def test_rebuild_trivial():
    # No-op: the known.json file is already up-to-date
    cwd = Path.cwd()
    Whitelist.rebuild(cwd)
