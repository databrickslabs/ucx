from abc import ABC, abstractmethod
from collections.abc import Iterable
from databricks.labs.ucx.source_code.base import Advice, Linter

import ast

class DetectFSVisitor(ast.NodeVisitor):
    """
    Visitor that detects file system paths in Python code and checks them
    against a list of known deprecated paths.
    """

    def __init__(self):
        self.advices: List[Advice] = []
        self.fs_prefixes = ["/dbfs/mnt", "dbfs:/", "/mnt/"]

    def visit_Call(self, node):
        print(f"Function call: {ast.unparse(node)}")
        self.generic_visit(node)

    def visit_Str(self, node):
        print(f"String literal: {ast.unparse(node)}")
        if any(node.s.startswith(prefix) for prefix in self.fs_prefixes):
            self.advices.append(Advice(
                code='fs-finder',
                message=f"Deprecated file system path: {node.s}",
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.lineno,
                end_col=node.col_offset + len(node.s)
            ))
        self.generic_visit(node)

    def get_advices(self) -> Iterable[Advice]:
        for advice in self.advices:
            yield advice

class FSFinderLinter(Linter):
    def __init__(self):
        pass

    def name(self) -> str:
        """
        Returns the name of the linter, for reporting etc
        """
        return 'fs-finder-linter'

    def lint(self, code: str) -> Iterable[Advice]:
        """
        Lints the code looking for file system paths that are deprecated
        """
        tree = ast.parse(code)
        visitor = DetectFSVisitor()
        visitor.visit(tree)
        return visitor.get_advices()

