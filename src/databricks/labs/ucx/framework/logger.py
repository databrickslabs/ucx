import logging
import sys
from dataclasses import dataclass
from datetime import datetime

from databricks.labs.ucx.framework.crawlers import SqlBackend

logger = logging.getLogger(__name__)


class NiceFormatter(logging.Formatter):
    BOLD = "\033[1m"
    RESET = "\033[0m"
    GREEN = "\033[32m"
    BLACK = "\033[30m"
    CYAN = "\033[36m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"
    GRAY = "\033[90m"

    def __init__(self, *, probe_tty: bool = False) -> None:
        super().__init__(fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s", datefmt="%H:%M")
        self._levels = {
            logging.NOTSET: self._bold("TRACE"),
            logging.DEBUG: self._bold(f"{self.CYAN}DEBUG"),
            logging.INFO: self._bold(f"{self.GREEN} INFO"),
            logging.WARNING: self._bold(f"{self.YELLOW} WARN"),
            logging.ERROR: self._bold(f"{self.RED}ERROR"),
            logging.CRITICAL: self._bold(f"{self.MAGENTA}FATAL"),
        }
        # show colors in runtime, github actions, and while debugging
        self.colors = sys.stdout.isatty() if probe_tty else True

    def _bold(self, text):
        return f"{self.BOLD}{text}{self.RESET}"

    def format(self, record: logging.LogRecord):  # noqa: A003
        if not self.colors:
            return super().format(record)
        ts = self.formatTime(record, datefmt="%H:%M")
        level = self._levels[record.levelno]
        # databricks.labs.ucx.foo.bar -> d.l.u.foo.bar
        module_split = record.name.split(".")
        last_two_modules = len(module_split) - 2
        name = ".".join(part if i >= last_two_modules else part[0] for i, part in enumerate(module_split))
        msg = record.msg
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            msg += ": " + record.exc_text
        if record.stack_info:
            if msg[-1:] != "\n":
                msg += "\n"
            msg += self.formatStack(record.stack_info)

        color_marker = self.GRAY
        if record.levelno in (logging.INFO, logging.WARNING):
            color_marker = self.BOLD
        elif record.levelno in (logging.ERROR, logging.FATAL):
            color_marker = self.RED + self.BOLD
        return f"{self.GRAY}{ts}{self.RESET} {level} {color_marker}[{name}] {msg}{self.RESET}"


@dataclass
class ObjectFailure:
    step_name: str
    object_type: str
    object_id: str
    event_time: datetime.now()


class FailureReporter:
    _buffer: list[ObjectFailure]

    def __init__(self, backend: SqlBackend, catalog: str, schema: str, table: str = "failures"):
        self._backend = backend
        self._catalog = catalog
        self._schema = schema
        self._table = table

    def report(self, failure: ObjectFailure):
        self._buffer.append(failure)

    def flush(self):
        full_name = f"{self._catalog}.{self._schema}.{self._table}"
        logger.debug(f"Persisting {len(self._buffer)} new records in {full_name}")
        self._backend.save_table(full_name, self._buffer, ObjectFailure, mode="append")
        self._buffer.clear()


def _install(level="DEBUG"):
    for h in logging.root.handlers:
        logging.root.removeHandler(h)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(NiceFormatter())
    console_handler.setLevel(level)
    logging.root.addHandler(console_handler)
    return console_handler
