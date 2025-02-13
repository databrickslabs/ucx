from databricks.labs.ucx.source_code.linters.base import PythonFixer
from databricks.labs.ucx.source_code.python.python_ast import Tree


def test_python_fixer_has_dummy_code() -> None:
    class _PythonFixer(PythonFixer):

        @property
        def diagnostic_code(self) -> str:
            """Dummy diagnostic code"""
            return "dummy"

        def apply_tree(self, tree: Tree) -> str:
            """Dummy fixer"""
            _ = tree
            return ""

    fixer = _PythonFixer()
    assert fixer.diagnostic_code == "dummy"


def test_python_fixer_applies_valid_python() -> None:
    class _PythonFixer(PythonFixer):

        @property
        def diagnostic_code(self) -> str:
            """Dummy diagnostic code"""
            return "dummy"

        def apply_tree(self, tree: Tree) -> str:
            """Dummy fixer"""
            _ = tree
            return "fixed"

    fixer = _PythonFixer()
    assert fixer.diagnostic_code == "dummy"
    assert fixer.apply("print(1)") == "fixed"


def test_python_fixer_applies_invalid_python() -> None:
    """Cannot fix invalid Python, thus nothing should happen"""

    class _PythonFixer(PythonFixer):

        @property
        def diagnostic_code(self) -> str:
            """Dummy diagnostic code"""
            return "dummy"

        def apply_tree(self, tree: Tree) -> str:
            """Dummy fixer"""
            _ = tree
            return "fixed"

    fixer = _PythonFixer()
    assert fixer.apply("print(1") == "print(1"  # closing parenthesis is missing on purpose
