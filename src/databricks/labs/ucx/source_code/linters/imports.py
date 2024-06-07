from __future__ import annotations

import abc
import logging
from collections.abc import Iterable, Callable
from typing import TypeVar, cast

from astroid import (  # type: ignore
    Attribute,
    Call,
    Const,
    Import,
    ImportFrom,
    Name,
    NodeNG,
)

from databricks.labs.ucx.source_code.base import Linter, Advice, Advisory
from databricks.labs.ucx.source_code.linters.python_ast import Tree, NodeBase, TreeVisitor

logger = logging.getLogger(__name__)


class ImportSource(NodeBase):

    @classmethod
    def extract_from_tree(cls, tree: Tree, problem_type: T) -> tuple[list[ImportSource], list[T]]:
        problems: list[T] = []
        sources: list[ImportSource] = []
        try:  # pylint: disable=too-many-try-statements
            nodes = tree.locate(Import, [])
            for source in cls._make_sources_for_import_nodes(nodes):
                sources.append(source)
            nodes = tree.locate(ImportFrom, [])
            for source in cls._make_sources_for_import_from_nodes(nodes):
                sources.append(source)
            nodes = tree.locate(Call, [("import_module", Attribute), ("importlib", Name)])
            nodes.extend(tree.locate(Call, [("__import__", Attribute), ("importlib", Name)]))
            for source in cls._make_sources_for_import_call_nodes(nodes, problem_type, problems):
                sources.append(source)
            return sources, problems
        except Exception as e:  # pylint: disable=broad-except
            problem = problem_type('internal-error', f"While checking imports: {e}")
            problems.append(problem)
            return [], problems

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
    def _make_sources_for_import_call_nodes(cls, nodes: list[Call], problem_type: T, problems: list[T]):
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


T = TypeVar("T", bound=Callable)


class DbutilsLinter(Linter):

    def lint(self, code: str) -> Iterable[Advice]:
        tree = Tree.parse(code)
        nodes = self.list_dbutils_notebook_run_calls(tree)
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
    def list_dbutils_notebook_run_calls(tree: Tree) -> list[NotebookRunCall]:
        calls = tree.locate(Call, [("run", Attribute), ("notebook", Attribute), ("dbutils", Name)])
        return [NotebookRunCall(call) for call in calls]


class SysPathChange(NodeBase, abc.ABC):

    @staticmethod
    def extract_from_tree(tree: Tree) -> list[SysPathChange]:
        visitor = SysPathChangesVisitor()
        visitor.visit(tree.root)
        return visitor.sys_path_changes

    def __init__(self, node: NodeNG, path: str, is_append: bool):
        super().__init__(node)
        self._path = path
        self._is_append = is_append

    @property
    def node(self):
        return self._node

    @property
    def path(self):
        return self._path

    @property
    def is_append(self):
        return self._is_append


class AbsolutePath(SysPathChange):
    # path directly added to sys.path
    pass


class RelativePath(SysPathChange):
    # path added to sys.path using os.path.abspath
    pass


class SysPathChangesVisitor(TreeVisitor):

    def __init__(self) -> None:
        super()
        self._aliases: dict[str, str] = {}
        self.sys_path_changes: list[SysPathChange] = []

    def visit_import(self, node: Import):
        for name, alias in node.names:
            if alias is None or name not in {"sys", "os"}:
                continue
            self._aliases[name] = alias

    def visit_importfrom(self, node: ImportFrom):
        interesting_aliases = [("sys", "path"), ("os", "path"), ("os.path", "abspath")]
        interesting_alias = next((t for t in interesting_aliases if t[0] == node.modname), None)
        if interesting_alias is None:
            return
        for name, alias in node.names:
            if name == interesting_alias[1]:
                self._aliases[f"{node.modname}.{interesting_alias[1]}"] = alias or name
                break

    def visit_call(self, node: Call):
        func = cast(Attribute, node.func)
        # check for 'sys.path.append'
        if not (
            self._match_aliases(func, ["sys", "path", "append"]) or self._match_aliases(func, ["sys", "path", "insert"])
        ):
            return
        is_append = func.attrname == "append"
        changed = node.args[0] if is_append else node.args[1]
        if isinstance(changed, Const):
            self.sys_path_changes.append(AbsolutePath(node, changed.value, is_append))
        elif isinstance(changed, Call):
            self._visit_relative_path(changed, is_append)

    def _match_aliases(self, node: NodeNG, names: list[str]):
        if isinstance(node, Attribute):
            if node.attrname != names[-1]:
                return False
            if len(names) == 1:
                return True
            return self._match_aliases(node.expr, names[0 : len(names) - 1])
        if isinstance(node, Name):
            full_name = ".".join(names)
            alias = self._aliases.get(full_name, full_name)
            return node.name == alias
        return False

    def _visit_relative_path(self, node: Call, is_append: bool):
        # check for 'os.path.abspath'
        if not self._match_aliases(node.func, ["os", "path", "abspath"]):
            return
        changed = node.args[0]
        if isinstance(changed, Const):
            self.sys_path_changes.append(RelativePath(changed, changed.value, is_append))
