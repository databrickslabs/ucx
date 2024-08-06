import astroid  # type: ignore[import-untyped]
from databricks.labs.blueprint.wheels import ProductInfo

from databricks.labs.ucx.source_code.base import Advice
from databricks.labs.ucx.source_code.python.python_ast import Tree


def main():
    # pylint: disable=too-many-nested-blocks
    codes = set()
    product_info = ProductInfo.from_class(Advice)
    source_code = product_info.version_file().parent
    for file in source_code.glob("**/*.py"):
        tree = Tree.parse(file.read_text())
        # recursively detect values of "code" kwarg in calls
        for node in tree.walk():
            if not isinstance(node, astroid.Call):
                continue
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
