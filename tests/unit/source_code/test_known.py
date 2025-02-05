import logging
from pathlib import Path
from typing import cast
from unittest import mock
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import DependencyProblem, DependencyGraph

from databricks.labs.ucx.source_code.known import KnownList, KnownDependency, KnownContainer, KnownLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_checks_compatibility() -> None:
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


def test_checks_library_compatibility() -> None:
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


def test_loads_known_json() -> None:
    known_json = KnownList._get_known()  # pylint: disable=protected-access
    assert known_json is not None and len(known_json) > 0


def test_error_on_missing_known_json() -> None:
    with (
        mock.patch("pkgutil.get_data", side_effect=FileNotFoundError("simulate missing file")),
        pytest.raises(FileNotFoundError),
    ):
        KnownList._get_known()  # pylint: disable=protected-access


def test_known_list_rebuild_finds_no_new_distributions(caplog) -> None:
    """Should be a no-op, otherwise know.json should be updated"""
    with caplog.at_level(logging.INFO, logger="databricks.labs.ucx.source_code.known"):
        KnownList.rebuild(Path.cwd(), dry_run=True)
    assert "No new distributions found." in caplog.messages


def test_analyze_dist_info() -> None:

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


@pytest.mark.parametrize("problems", [[], [DependencyProblem("test", "test")]])
def test_known_container_loads_problems_during_dependency_graph_building(
    simple_dependency_resolver, problems: list[DependencyProblem]
) -> None:
    path_lookup = create_autospec(PathLookup)
    dependency = KnownDependency("test", problems)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, path_lookup, CurrentSessionState())
    container = KnownContainer(Path("test.py"), problems)
    assert container.build_dependency_graph(graph) == problems
    path_lookup.assert_not_called()


@pytest.mark.parametrize("problems", [[], [DependencyProblem("test", "test")]])
def test_known_loader_loads_known_container_with_problems(
    simple_dependency_resolver, problems: list[DependencyProblem]
) -> None:
    path_lookup = create_autospec(PathLookup)
    loader = KnownLoader()
    dependency = KnownDependency("test", problems)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, path_lookup, CurrentSessionState())
    container = loader.load_dependency(path_lookup, dependency)
    assert container.build_dependency_graph(graph) == problems
    path_lookup.resolve.assert_not_called()


@pytest.mark.parametrize("problems", [[], [DependencyProblem("test", "test")]])
def test_known_dependency_has_problems(problems: list[DependencyProblem]) -> None:
    dependency = KnownDependency("test", problems)
    assert dependency.problems == problems
