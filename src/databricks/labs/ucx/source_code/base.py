from abc import abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass

# Code mapping between LSP, PyLint, and our own diagnostics:
# | LSP                       | PyLint     | Our            |
# |---------------------------|------------|----------------|
# | Severity.ERROR            | Error      | Failure()      |
# | Severity.WARN             | Warning    | Advisory()     |
# | DiagnosticTag.DEPRECATED  | Warning    | Deprecation()  |
# | Severity.INFO             | Info       | Advice()       |
# | Severity.HINT             | Convention | Convention()   |
# | DiagnosticTag.UNNECESSARY | Refactor   | Convention()   |


@dataclass
class Advice:

    MISSING_TYPE = "<MISSING_TYPE>"
    MISSING_PATH = "<MISSING_PATH>"

    code: str
    message: str
    source_type: str
    source_path: str
    start_line: int
    start_col: int
    end_line: int
    end_col: int

    def replace(
        self,
        code: str | None = None,
        message: str | None = None,
        location_type: str | None = None,
        location_path: str | None = None,
        start_line: int | None = None,
        start_col: int | None = None,
        end_line: int | None = None,
        end_col: int | None = None,
    ) -> 'Advice':
        return self.__class__(
            code=code if code is not None else self.code,
            message=message if message is not None else self.message,
            location_type=location_type if location_type is not None else self.source_type,
            location_path=location_type if location_path is not None else self.source_path,
            start_line=start_line if start_line is not None else self.start_line,
            start_col=start_col if start_col is not None else self.start_col,
            end_line=end_line if end_line is not None else self.end_line,
            end_col=end_col if end_col is not None else self.end_col,
        )

    def as_advisory(self) -> 'Advisory':
        return Advisory(**self.__dict__)

    def as_failure(self) -> 'Failure':
        return Failure(**self.__dict__)

    def as_deprecation(self) -> 'Deprecation':
        return Deprecation(**self.__dict__)

    def as_convention(self) -> 'Convention':
        return Convention(**self.__dict__)


class Advisory(Advice):
    """A warning that does not prevent the code from running."""


class Failure(Advisory):
    """An error that prevents the code from running."""


class Deprecation(Advisory):
    """An advisory that suggests to replace the code with a newer version."""


class Convention(Advice):
    """A suggestion for a better way to write the code."""


class Linter:
    @abstractmethod
    def lint(self, code: str) -> Iterable[Advice]: ...


class Fixer:
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def apply(self, code: str) -> str: ...


class SequentialLinter(Linter):
    def __init__(self, linters: list[Linter]):
        self._linters = linters

    def lint(self, code: str) -> Iterable[Advice]:
        for linter in self._linters:
            yield from linter.lint(code)
