import functools
import logging
import sys
import time

from uc_migration_toolkit.utils import get_dbutils


class CustomFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self._notebook_path = self._get_notebook_path()

    @staticmethod
    def _get_notebook_path() -> str:
        notebook_path = (
            get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().lower()
        )
        return notebook_path

    def format(self, record: logging.LogRecord):  # noqa: A003
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record.created))
        log_level = record.levelname
        function_name = record.funcName
        return (
            f"[{timestamp}][{log_level}][{record.threadName}]"
            f"[{self._notebook_path}][{function_name}] {record.getMessage()}"
        )


class LoggerProvider:
    @staticmethod
    @functools.lru_cache(maxsize=10_000)
    def _get_logger() -> logging.Logger:
        # Create a logger and set the custom formatter
        base_logger = logging.getLogger("uc-migration-toolkit")
        base_logger.setLevel(logging.DEBUG)

        if not base_logger.handlers:
            # Create a stream handler to output log messages to the console
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setLevel(logging.DEBUG)

            # Set the custom formatter on the stream handler
            formatter = CustomFormatter()
            stream_handler.setFormatter(formatter)

            # Add the stream handler to the logger
            base_logger.addHandler(stream_handler)

        return base_logger

    def __init__(self):
        self.logger = self._get_logger()
