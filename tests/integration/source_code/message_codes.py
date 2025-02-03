import astroid  # type: ignore[import-untyped]
from databricks.labs.blueprint.wheels import ProductInfo

from databricks.labs.ucx.source_code.base import Advice
from databricks.labs.ucx.source_code.python.python_ast import MaybeTree


def main():
    # pylint: disable=too-many-nested-blocks
    codes = set()
    product_info = ProductInfo.from_class(Advice)
    source_code = product_info.version_file().parent
    for file in source_code.glob("**/*.py"):
        maybe_tree = MaybeTree.from_source_code(file.read_text())
        if not maybe_tree.tree:
            continue
        tree = maybe_tree.tree
        # recursively detect values of "code" kwarg in calls
        for node in tree.walk():
            if not isinstance(node, (astroid.Call, astroid.ClassDef)):
                continue
            # Filter coders from Fixer classes
            if isinstance(node, astroid.ClassDef):
                diagnostic_code_methods = []
                for child_node in node.body:
                    if isinstance(child_node, astroid.FunctionDef) and child_node.name == "diagnostic_code":
                        diagnostic_code_methods.append(child_node)
                if diagnostic_code_methods and diagnostic_code_methods[0].body:
                    problem_code = diagnostic_code_methods[0].body[0].value.value
                    print("found code", problem_code)
                    codes.add(problem_code)
                continue
            # Filter codes from Advice calls
            for keyword in node.keywords:
                name = keyword.arg
                if name != "code":
                    continue
                if not isinstance(keyword.value, astroid.Const):
                    continue
                problem_code = keyword.value.value
                codes.add(problem_code)
    for code in sorted(codes):
        print(code)


if __name__ == "__main__":
    main()


def test_main():
    main()
