import enum
from abc import abstractmethod
from dataclasses import dataclass


@dataclass
class Position:
    line: int
    character: int


@dataclass
class Range:
    start: Position
    end: Position


class Severity(enum.IntEnum):
    ERROR = 1
    WARN = 2
    INFO = 3
    HINT = 4


@dataclass
class Diagnostic:
    # the range at which the message applies.
    range: Range

    # The diagnostic's code, which might appear in the user interface.
    code: str

    # An optional property to describe the error code.
    source: str

    # The diagnostic's message.
    message: str

    # The diagnostic's severity. Can be omitted. If omitted it is up to the
    # client to interpret diagnostics as error, warning, info or hint.
    severity: Severity


class Fixer:
    # TODO: fit Diagnostic from LSP
    # See https://microsoft.github.io/language-server-protocol/specifications/specification-3-16/#diagnostic
    @abstractmethod
    def match(self, code: str) -> bool: ...

    @abstractmethod
    def apply(self, code: str) -> str: ...


class SequentialFixer(Fixer):
    def __init__(self, fixes: list[Fixer]):
        self._fixes = fixes

    def match(self, code: str) -> bool:
        for fix in self._fixes:
            if fix.match(code):
                return True
        return False

    def apply(self, code: str) -> str:
        for fix in self._fixes:
            if fix.match(code):
                code = fix.apply(code)
        return code
