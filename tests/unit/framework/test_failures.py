from unit.framework.mocks import MockBackend

from databricks.labs.ucx.framework.failures import FailureReporter, ObjectFailure


def test_failure_reporter():
    b = MockBackend()
    fh = FailureReporter(b, "a", "b", "c")

    result = ObjectFailure(object_type="step", object_id="object_id", error_info="error_info")
    fh.report(result)
    fh.flush()

    assert [result] == b.rows_written_for("a.b.c", "append")