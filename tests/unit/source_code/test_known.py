from pathlib import Path
from typing import cast
from unittest import mock

import pytest

from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_checks_compatibility():
    known = KnownList()
    spark_sql = known.module_compatibility("spark.sql")
    assert not spark_sql.known

    sdk_compute = known.module_compatibility("databricks.sdk.service.compute")
    assert sdk_compute.known
    assert not sdk_compute.problems

    assert known.module_compatibility("sys").known

    os_path = known.module_compatibility("os.path")
    assert os_path.known
    assert not os_path.problems

    other = known.module_compatibility(cast(str, None))
    assert not other.known
    assert not other.problems


def test_checks_library_compatibility():
    known = KnownList()

    sklearn = known.distribution_compatibility("scikit-learn")
    assert sklearn.known
    assert not sklearn.problems

    s3fs = known.distribution_compatibility("s3fs")
    assert s3fs.known
    assert len(s3fs.problems) == 1

    other = known.distribution_compatibility(cast(str, None))
    assert not other.known
    assert not other.problems


@pytest.mark.parametrize(
    "library", ["pytest", "pytest_cov", "pytest_mock", "_pytest", "_pytest.mark", "_pytest.mark.expression"]
)
def test_known_compatibility(library) -> None:
    known = KnownList()
    compatibility = known.module_compatibility(library)
    assert compatibility.known


@pytest.mark.parametrize("library", ["pytest_asyncio"])
def test_unknown_compatibility(library: str) -> None:
    known = KnownList()
    compatibility = known.module_compatibility(library)
    assert not compatibility.known


def test_loads_known_json():
    known_json = KnownList._get_known()  # pylint: disable=protected-access
    assert known_json is not None and len(known_json) > 0


def test_error_on_missing_known_json():
    with (
        mock.patch("pkgutil.get_data", side_effect=FileNotFoundError("simulate missing file")),
        pytest.raises(FileNotFoundError),
    ):
        KnownList._get_known()  # pylint: disable=protected-access


def test_rebuild_trivial():
    # No-op: the known.json file is already up-to-date
    cwd = Path.cwd()
    KnownList.rebuild(cwd)


def test_analyze_dist_info():

    class TestKnownList(KnownList):

        @classmethod
        def analyze_cachetools_dist_info(cls):
            path_lookup = PathLookup.from_sys_path(Path.cwd())
            for library_root in path_lookup.library_roots:
                for dist_info_folder in library_root.glob("*.dist-info"):
                    if "cachetools" in dist_info_folder.as_posix():
                        cls._analyze_dist_info(dist_info_folder, {}, library_root)
                        return

    TestKnownList.analyze_cachetools_dist_info()
