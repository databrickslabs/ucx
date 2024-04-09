from abc import ABC, abstractmethod
from collections.abc import Iterable
from databricks.labs.ucx.source_code.base import Advice, Linter


class FSFinderLinter(Linter, ABC):
    def __init__(self):
        pass

    def name(self) -> str:
        return 'fs-finder-linter'

    @abstractmethod
    def lint(self, code: str) -> Iterable[Advice]:
        # TODO: Implement the linting logic here
        pass
