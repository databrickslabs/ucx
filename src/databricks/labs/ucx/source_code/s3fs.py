import ast
from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Advice, Linter, Deprecation


class DetectS3fsVisitor(ast.NodeVisitor):
    def __init__(self):
        self._advices: list[Advice] = []

    def visit_Import(self, node):  # pylint: disable=invalid-name
        for alias in node.names:
            if alias.name == 's3fs':
                self._advices.append(
                    Deprecation(
                        code='s3fs-usage',
                        message="The use of s3fs is deprecated",
                        start_line=alias.lineno,
                        start_col=alias.col_offset,
                        end_line=alias.lineno,
                        end_col=alias.col_offset + len(alias.name),
                    )
                )
        self.generic_visit(node)

    def visit_ImportFrom(self, node):  # pylint: disable=invalid-name
        if node.module == 's3fs' or node.module.startswith('s3fs.'):
            self._advices.append(
                Deprecation(
                    code='s3fs-usage',
                    message="The use of s3fs is deprecated",
                    start_line=node.lineno,
                    start_col=node.col_offset,
                    end_line=node.lineno,
                    end_col=node.end_col_offset,
                )
            )
        self.generic_visit(node)

    def get_advices(self) -> Iterable[Advice]:
        yield from self._advices


class S3FSUsageLinter(Linter):
    def __init__(self):
        pass

    @staticmethod
    def name() -> str:
        """
        Returns the name of the linter, for reporting etc
        """
        return 's3fs-usage'

    def lint(self, code: str) -> Iterable[Advice]:
        """
        Lints the code looking for file system paths that are deprecated
        """
        tree = ast.parse(code)
        visitor = DetectS3fsVisitor()
        visitor.visit(tree)
        return visitor.get_advices()
