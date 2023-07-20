from loguru import logger
from loguru._logger import Logger


class LoggerMixin:
    @staticmethod
    def _get_logger() -> Logger:
        _l = logger
        return logger

    def __init__(self):
        self._logger = self._get_logger()

    @property
    def logger(self) -> Logger:
        return self._logger
