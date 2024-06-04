from __future__ import annotations

import abc
import ast
import logging
from collections.abc import Iterable, Callable
from typing import TypeVar, Generic, cast

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

logger = logging.getLogger(__name__)


class Visitor:

    def visit(self, node: NodeNG):
        self._visit_specific(node)
        for child in node.get_children():
            self.visit(child)

    def _visit_specific(self, node: NodeNG):
        method_name = "visit_" + type(node).__name__.lower()
        method_slot = getattr(self, method_name, None)
        if callable(method_slot):
            method_slot(node)
        else:
            self.visit_nodeng(node)

    def visit_nodeng(self, node: NodeNG):
        pass


class TreeWalker:

    @classmethod
    def walk(cls, node: NodeNG) -> Iterable[NodeNG]:
        yield node
        for child in node.get_children():
            yield from TreeWalker.walk(child)


class MatchingVisitor(Visitor):

    def __init__(self, node_type: type, match_nodes: list[tuple[str, type]]):
        super()
        self._matched_nodes: list[NodeNG] = []
        self._node_type = node_type
        self._match_nodes = match_nodes

    @property
    def matched_nodes(self):
        return self._matched_nodes

    def visit_Call(self, node: ast.Call):
        if self._node_type is not ast.Call:
            return
        try:
            if self._matches(node.func, 0):
                self._matched_nodes.append(node)
        except NotImplementedError as e:
            logger.warning(f"Missing implementation: {e.args[0]}")

    def visit_import(self, node: Import):
        if self._node_type is not Import:
            return
        self._matched_nodes.append(node)

    def visit_importfrom(self, node: ImportFrom):
        if self._node_type is not ImportFrom:
            return
        self._matched_nodes.append(node)

    def _matches(self, node: NodeNG, depth: int):
        if depth >= len(self._match_nodes):
            return False
        name, match_node = self._match_nodes[depth]
        if not isinstance(node, match_node):
            return False
        next_node: NodeNG | None = None
        if isinstance(node, Attribute):
            if node.attrname != name:
                return False
            next_node = node.expr
        elif isinstance(node, Name):
            if node.name != name:
                return False
        else:
            raise NotImplementedError(str(type(node)))
        if next_node is None:
            # is this the last node to match ?
            return len(self._match_nodes) - 1 == depth
        return self._matches(next_node, depth + 1)


class NodeBase(abc.ABC):

    def __init__(self, node: NodeNG):
        self._node = node

    @property
    def node(self):
        return self._node

    def __repr__(self):
        return f"<{self.__class__.__name__}: {repr(self._node)}>"


class SysPathChange(NodeBase, abc.ABC):

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


# path directly added to sys.path
class AbsolutePath(SysPathChange):
    pass


# path added to sys.path using os.path.abspath
class RelativePath(SysPathChange):
    pass


class SysPathVisitor(Visitor):

    def __init__(self):
        super()
        self._aliases: dict[str, str] = {}
        self._syspath_changes: list[SysPathChange] = []

    @property
    def syspath_changes(self):
        return self._syspath_changes

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
            self._syspath_changes.append(AbsolutePath(node, changed.value, is_append))
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

    def _visit_relative_path(self, node: NodeNG, is_append: bool):
        # check for 'os.path.abspath'
        if not self._match_aliases(node.func, ["os", "path", "abspath"]):
            return
        changed = node.args[0]
        if isinstance(changed, Const):
            self._syspath_changes.append(RelativePath(changed, changed.value, is_append))


T = TypeVar("T", bound=NodeNG)


# disclaimer this class is NOT thread-safe
class ASTLinter(Generic[T]):

    @staticmethod
    def parse(code: str):
        root = parse(code)
        return ASTLinter(root)

    def __init__(self, root: Module):
        self._root: Module = root

    @property
    def root(self):
        return self._root

    def locate(self, node_type: type[T], match_nodes: list[tuple[str, type]]) -> list[T]:
        visitor = MatchingVisitor(node_type, match_nodes)
        visitor.visit(self._root)
        return visitor.matched_nodes

    def collect_sys_paths_changes(self):
        visitor = SysPathVisitor()
        visitor.visit(self._root)
        return visitor.syspath_changes

    def first_statement(self):
        return self._root.body[0]

    @classmethod
    def extract_call_by_name(cls, call: Call, name: str) -> Call | None:
        """Given a call-chain, extract its sub-call by method name (if it has one)"""
        assert isinstance(call, Call)
        node = call
        while True:
            func = node.func
            if not isinstance(func, Attribute):
                return None
            if func.attrname == name:
                return node
            if not isinstance(func.expr, Call):
                return None
            node = func.expr

    @classmethod
    def args_count(cls, node: Call) -> int:
        """Count the number of arguments (positionals + keywords)"""
        assert isinstance(node, Call)
        return len(node.args) + len(node.keywords)

    @classmethod
    def get_arg(
        cls,
        node: Call,
        arg_index: int | None,
        arg_name: str | None,
    ) -> NodeNG | None:
        """Extract the call argument identified by an optional position or name (if it has one)"""
        assert isinstance(node, Call)
        if arg_index is not None and len(node.args) > arg_index:
            return node.args[arg_index]
        if arg_name is not None:
            arg = [kw.value for kw in node.keywords if kw.arg == arg_name]
            if len(arg) == 1:
                return arg[0]
        return None

    @classmethod
    def is_none(cls, node: NodeNG) -> bool:
        """Check if the given AST expression is the None constant"""
        if not isinstance(node, Const):
            return False
        return node.value is None

    def __repr__(self):
        truncate_after = 32
        code = repr(self._root)
        if len(code) > truncate_after:
            code = code[0:truncate_after] + "..."
        return f"<ASTLinter: {code}>"


class ImportSource(NodeBase):

    def __init__(self, node: NodeNG, name: str):
        super().__init__(node)
        self.name = name


class NotebookRunCall(NodeBase):

    def __init__(self, node: Call):
        super().__init__(node)

    def get_notebook_path(self) -> str | None:
        node = PythonLinter.get_dbutils_notebook_run_path_arg(cast(Call, self.node))
        inferred = next(node.infer(), None)
        if isinstance(inferred, Const):
            return inferred.value.strip().strip("'").strip('"')
        return None


P = TypeVar("P", bound=Callable)


class PythonLinter(Linter):

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
            sources.extend(list(cls._make_sources_for_import_nodes(nodes)))
            nodes = linter.locate(ImportFrom, [])
            sources.extend(list(cls._make_sources_for_import_from_nodes(nodes)))
            nodes = linter.locate(Call, [("import_module", Attribute), ("importlib", Name)])
            nodes.extend(linter.locate(Call, [("__import__", Attribute), ("importlib", Name)]))
            sources.extend(list(cls._make_sources_for_import_call_nodes(nodes, problem_type, problems)))
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
            return [], problems

