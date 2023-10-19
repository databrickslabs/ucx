import logging

from databricks.sdk.core import DatabricksError
from unit.framework.mocks import MockBackend

from databricks.labs.ucx.framework.failures import FailureReporter, ObjectFailure
from databricks.labs.ucx.framework.parallel import Threads


def test_failure_reporter():
    b = MockBackend()
    fh = FailureReporter(b, "a", "b", "c")

    result = ObjectFailure(object_type="step", object_id="object_id", error_info="error_info")
    fh.report(result)
    fh.flush()

    assert [result] == b.rows_written_for("a.b.c", "append")


def _predictable_messages(caplog):
    res = []
    for msg in caplog.messages:
        if "rps" in msg:
            continue
        msg = msg.split(". Took ")[0]  # noqa: PLW2901
        res.append(msg)
    return sorted(res)


def test_failure_reporter_with_threads(caplog):
    caplog.set_level(logging.INFO)

    def works():
        return True

    def fails():
        msg = "failed"
        raise DatabricksError(msg)

    tasks = [works, fails, works, fails, works, fails, works, fails]
    results, errors = Threads.gather("testing", tasks)

    assert [True, True, True, True] == results
    assert 4 == len(errors)
    assert [
        "More than half 'testing' tasks failed: 50% results available (4/8)",
        "testing task failed: failed",
        "testing task failed: failed",
        "testing task failed: failed",
        "testing task failed: failed",
    ] == _predictable_messages(caplog)
