from databricks.sdk.core import DatabricksError

from databricks.labs.ucx.framework.parallel import ThreadedExecution


def test_threaded_execution():
    def works():
        return True

    def fails():
        msg = "failed"
        raise DatabricksError(msg)

    tasks = [works, works, fails, works, works]
    futures = ThreadedExecution.gather("testing", tasks)
    assert len(futures) == 4
