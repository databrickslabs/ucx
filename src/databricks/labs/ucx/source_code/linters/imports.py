from __future__ import annotations

import logging
from collections.abc import Iterable, Callable
from typing import TypeVar, cast

from astroid import (  # type: ignore
    parse,
    Attribute,
    Call,
    Const,
    Import,
    ImportFrom,
    Module,
    Name,
    NodeNG,
)

from databricks.labs.ucx.source_code.base import Linter, Advice, Advisory
from databricks.labs.ucx.source_code.linters.python_ast import ASTLinter, NodeBase, SysPathChange

logger = logging.getLogger(__name__)


class ImportSource(NodeBase):

    def __init__(self, node: NodeNG, name: str):
        super().__init__(node)
        self.name = name


class NotebookRunCall(NodeBase):

    def __init__(self, node: Call):
        super().__init__(node)

    def get_notebook_path(self) -> str | None:
        node = DbutilsLinter.get_dbutils_notebook_run_path_arg(cast(Call, self.node))
        inferred = next(node.infer(), None)
        if isinstance(inferred, Const):
            return inferred.value.strip().strip("'").strip('"')
        return None


P = TypeVar("P", bound=Callable)


class DbutilsLinter(Linter):

    def lint(self, code: str) -> Iterable[Advice]:
        linter = ASTLinter.parse(code)
        nodes = self.list_dbutils_notebook_run_calls(linter)
        return [self._convert_dbutils_notebook_run_to_advice(node.node) for node in nodes]

    @classmethod
    def _convert_dbutils_notebook_run_to_advice(cls, node: NodeNG) -> Advisory:
        assert isinstance(node, Call)
        path = cls.get_dbutils_notebook_run_path_arg(node)
        if isinstance(path, Const):
            return Advisory(
                'dbutils-notebook-run-literal',
                "Call to 'dbutils.notebook.run' will be migrated automatically",
                node.lineno,
                node.col_offset,
                node.end_lineno or 0,
                node.end_col_offset or 0,
            )
        return Advisory(
            'dbutils-notebook-run-dynamic',
            "Path for 'dbutils.notebook.run' is not a constant and requires adjusting the notebook path",
            node.lineno,
            node.col_offset,
            node.end_lineno or 0,
            node.end_col_offset or 0,
        )

    @staticmethod
    def get_dbutils_notebook_run_path_arg(node: Call):
        if len(node.args) > 0:
            return node.args[0]
        arg = next(kw for kw in node.keywords if kw.arg == "path")
        return arg.value if arg is not None else None

    @staticmethod
    def list_dbutils_notebook_run_calls(linter: ASTLinter) -> list[NotebookRunCall]:
        calls = linter.locate(Call, [("run", Attribute), ("notebook", Attribute), ("dbutils", Name)])
        return [NotebookRunCall(call) for call in calls]

    @classmethod
    def list_import_sources(cls, linter: ASTLinter, problem_type: P) -> tuple[list[ImportSource], list[P]]:
        problems: list[P] = []
        sources: list[ImportSource] = []
        try:  # pylint: disable=too-many-try-statements
            nodes = linter.locate(Import, [])
            for source in cls._make_sources_for_import_nodes(nodes):
                sources.append(source)
            nodes = linter.locate(ImportFrom, [])
            for source in cls._make_sources_for_import_from_nodes(nodes):
                sources.append(source)
            nodes = linter.locate(Call, [("import_module", Attribute), ("importlib", Name)])
            nodes.extend(linter.locate(Call, [("__import__", Attribute), ("importlib", Name)]))
            for source in cls._make_sources_for_import_call_nodes(nodes, problem_type, problems):
                sources.append(source)
            return sources, problems
        except Exception as e:  # pylint: disable=broad-except
            problem = problem_type('internal-error', f"While linter {linter} was checking imports: {e}")
            problems.append(problem)
            return [], problems

    @staticmethod
    def list_sys_path_changes(linter: ASTLinter) -> list[SysPathChange]:
        return linter.collect_sys_paths_changes()

    @classmethod
    def _make_sources_for_import_nodes(cls, nodes: list[Import]) -> Iterable[ImportSource]:
        for node in nodes:
            for name, _ in node.names:
                if name is not None:
                    yield ImportSource(node, name)

    @classmethod
    def _make_sources_for_import_from_nodes(cls, nodes: list[ImportFrom]) -> Iterable[ImportSource]:
        for node in nodes:
            yield ImportSource(node, node.modname)

    @classmethod
    def _make_sources_for_import_call_nodes(cls, nodes: list[Call], problem_type: P, problems: list[P]):
        for node in nodes:
            arg = node.args[0]
            if isinstance(arg, Const):
                yield ImportSource(node, arg.value)
                continue
            problem = problem_type(
                'dependency-not-constant',
                "Can't check dependency not provided as a constant",
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )
            problems.append(problem)
