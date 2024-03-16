from abc import abstractmethod


class Fixer:
    @abstractmethod
    def match(self, code: str) -> bool: ...

    @abstractmethod
    def apply(self, code: str) -> str: ...
