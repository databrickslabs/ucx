import logging

from unit.framework.mocks import MockBackend

from databricks.labs.ucx.framework.failures import FailureReporter, ObjectFailure


def test_logger():
    logger = logging.getLogger(__name__)
    logger.debug("This is a debug message")
    logger.info("This is an table message")
    logger.warning("This is a warning message")
    logger.error("This is an error message", exc_info=KeyError(123))
    logger.critical("This is a critical message")


def test_failure_reporter():
    b = MockBackend()
    fh = FailureReporter(b, "a", "b", "c")

    result = ObjectFailure(object_type="step", object_id="object_id", error_info="error_info")
    fh.report(result)
    fh.flush()

    assert [result] == b.rows_written_for("a.b.c", "append")
