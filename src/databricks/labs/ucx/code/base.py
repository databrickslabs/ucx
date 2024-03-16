from abc import abstractmethod



class Fixer:
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
