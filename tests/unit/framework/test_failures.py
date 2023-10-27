from databricks.sdk.core import DatabricksError

from databricks.labs.ucx.framework.failures import FailureReporter, ObjectFailure
from databricks.labs.ucx.framework.parallel import Threads

from .mocks import MockBackend


def test_failure_reporter():
    b = MockBackend()
    fh = FailureReporter(b, "a", "b", "c")

    errors = [ObjectFailure(object_type="step", object_id="object_id", error_info="error_info")]
    fh.report(errors)

    # check that the failure record has been written to the table
    assert errors == b.rows_written_for("a.b.c", "append")


def test_failure_reporter_with_threads():
    def works():
        return True

    def fails():
        msg = "failed"
        raise DatabricksError(msg)

    b = MockBackend()
    fh = FailureReporter(b, "a", "b", "c")
    tasks = [works, fails, works, fails, works, fails, works, fails]
    results, errors = Threads.gather("testing", tasks)
    if len(errors) > 0:
        fh.report(errors)

    assert len(b.rows_written_for("a.b.c", "append")) == 4
