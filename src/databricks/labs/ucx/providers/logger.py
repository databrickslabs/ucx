import logging
from logging import LogRecord

from loguru import logger as _loguru_logger

# reassigning the logger to the loguru logger
# for flexibility and simple dependency injection
logger = _loguru_logger

_sdk_logger = logging.getLogger("databricks.sdk")


class _LoguruHandler(logging.Handler):
    def emit(self, record: LogRecord) -> None:
        logger.debug(record.msg)


_sdk_logger.addHandler(_LoguruHandler())
