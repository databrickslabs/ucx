from collections.abc import Iterable
from pathlib import Path

import astroid  # type: ignore[import-untyped]
from databricks.labs.blueprint.wheels import ProductInfo

from databricks.labs.ucx.source_code.base import Advice
from databricks.labs.ucx.source_code.python.python_ast import MaybeTree


def main():
    """Walk the UCX code base to find all diagnostic linting codes."""
    codes = set[str]()
    product_info = ProductInfo.from_class(Advice)
    source_code = product_info.version_file().parent
    for path in source_code.glob("**/*.py"):
        codes.update(_find_diagnostic_codes(path))
    for code in sorted(codes):
        print(code)


def _find_diagnostic_codes(file: Path) -> Iterable[str]:
    """Walk the Python ast tree to find the diagnostic codes."""
    maybe_tree = MaybeTree.from_source_code(file.read_text())
    if not maybe_tree.tree:
        return
    for node in maybe_tree.tree.walk():
        diagnostic_code = None
        if isinstance(node, astroid.ClassDef):
            diagnostic_code = _find_diagnostic_code_in_class_def(node)
        elif isinstance(node, astroid.Call):
            diagnostic_code = _find_diagnostic_code_in_call(node)
        if diagnostic_code:
            yield diagnostic_code


def _find_diagnostic_code_in_call(node: astroid.Call) -> str | None:
    """Find the diagnostic code in a call node."""
    for keyword in node.keywords:
        if keyword.arg == "code" and isinstance(keyword.value, astroid.Const):
            problem_code = keyword.value.value
            return problem_code
    return None


def _find_diagnostic_code_in_class_def(node: astroid.ClassDef) -> str | None:
    """Find the diagnostic code in a class definition node."""
    diagnostic_code_methods = []
    for child_node in node.body:
        if isinstance(child_node, astroid.FunctionDef) and child_node.name == "diagnostic_code":
            diagnostic_code_methods.append(child_node)
    if diagnostic_code_methods and diagnostic_code_methods[0].body:
        problem_code = diagnostic_code_methods[0].body[0].value.value
        return problem_code
    return None


if __name__ == "__main__":
    main()


def test_main():
    main()
