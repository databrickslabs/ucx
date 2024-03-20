import enum
from abc import abstractmethod
from dataclasses import dataclass
from typing import Iterable


@dataclass
class Position:
    line: int
    character: int

    def as_dict(self) -> dict:
        return {"line": self.line, "character": self.character}

    @classmethod
    def from_dict(cls, d: dict) -> 'Position':
        return cls(d['line'], d['character'])


@dataclass
class Range:
    start: Position
    end: Position

    @classmethod
    def from_dict(cls, d: dict) -> 'Range':
        return cls(Position.from_dict(d['start']), Position.from_dict(d['end']))

    @classmethod
    def make(cls, start_line: int, start_character: int, end_line: int, end_character: int) -> 'Range':
        return cls(start=Position(start_line - 1, start_character), end=Position(end_line - 1, end_character))

    def as_dict(self) -> dict:
        return {"start": self.start.as_dict(), "end": self.end.as_dict()}

    def fragment(self, code: str) -> str:
        out = []
        splitlines = code.splitlines()
        for line, part in enumerate(splitlines):
            if line == self.start.line and line == self.end.line:
                out.append(part[self.start.character : self.end.character])
            elif line == self.start.line:
                out.append(part[self.start.character :])
            elif line == self.end.line:
                out.append(part[: self.end.character])
            elif self.start.line < line < self.end.line:
                out.append(part)
        return "".join(out)


class Severity(enum.IntEnum):
    ERROR = 1
    WARN = 2
    INFO = 3
    HINT = 4


class DiagnosticTag(enum.IntEnum):
    UNNECESSARY = 1
    DEPRECATED = 2


@dataclass
class Message:
    code: str
    message: str
    start_line: int
    start_col: int
    end_line: int
    end_col: int


class Deprecation(Message):
    pass


def x():
    Deprecation("a", "b", 1, 2, 3, 4)


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

    tags: list[DiagnosticTag] | None = None

    def as_dict(self) -> dict:
        return {
            "range": self.range.as_dict(),
            "code": self.code,
            "source": self.source,
            "message": self.message,
            "severity": self.severity.value if self.severity else Severity.WARN,
            "tags": [t.value for t in self.tags] if self.tags else [],
        }


class Linter:
    @abstractmethod
    def lint(self, code: str) -> Iterable[Diagnostic]: ...


class Fixer:
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def apply(self, code: str) -> str: ...


class SequentialLinter(Linter):
    def __init__(self, analysers: list[Linter]):
        self._analysers = analysers

    def lint(self, code: str) -> Iterable[Diagnostic]:
        for analyser in self._analysers:
            yield from analyser.lint(code)


class SequentialFixer(Fixer):
    def __init__(self, fixes: list[Fixer]):
        self._fixes = fixes

    def apply(self, code: str) -> str:
        for fix in self._fixes:
            if fix.analyse(code):
                code = fix.apply(code)
        return code
