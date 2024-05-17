import dataclasses

from databricks.labs.ucx.runtime import Workflows
from databricks.labs.ucx.source_code.base import LocatedAdvice
from databricks.labs.ucx.source_code.jobs import JobProblem


def test_tasks_detected():
    workflows = Workflows.all()

    tasks = workflows.tasks()

    assert len(tasks) > 1


@dataclasses.dataclass
class Foo:
    first: str
    second: bool


@dataclasses.dataclass
class Derived(Foo):
    third: str


@dataclasses.dataclass
class DerivedProblem(LocatedAdvice):
    job_id: str


def test_job_problem_is_correctly_reflected():
    for f in dataclasses.fields(JobProblem):
        assert isinstance(f.type, type)


def test_derived_problem_is_correctly_reflected():
    for f in dataclasses.fields(DerivedProblem):
        assert isinstance(f.type, type)


def test_foo_is_correctly_reflected():
    for f in dataclasses.fields(Foo):
        assert isinstance(f.type, type)


def test_derived_is_correctly_reflected():
    for f in dataclasses.fields(Derived):
        assert isinstance(f.type, type)
