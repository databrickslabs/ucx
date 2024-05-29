from __future__ import annotations

import abc
import astroid
import logging
from collections.abc import Iterable, Callable
from typing import TypeVar, Generic, cast

from databricks.labs.ucx.source_code.base import Linter, Advice, Advisory

logger = logging.getLogger(__name__)


class MatchingVisitor(astroid.NodeVisitor):

    def __init__(self, node_type: type, match_nodes: list[tuple[str, type]]):
        self._matched_nodes: list[astroid.NodeNG] = []
        self._node_type = node_type
        self._match_nodes = match_nodes

    @property
    def matched_nodes(self):
        return self._matched_nodes

    def visit_Call(self, node: astroid.Call):
        if self._node_type is not astroid.Call:
            return
        try:
            if self._matches(node.func, 0):
                self._matched_nodes.append(node)
        except NotImplementedError as e:
            logger.warning(f"Missing implementation: {e.args[0]}")

    def visit_Import(self, node: astroid.Import):
        if self._node_type is not astroid.Import:
            return
        self._matched_nodes.append(node)

    def visit_ImportFrom(self, node: astroid.ImportFrom):
        if self._node_type is not astroid.ImportFrom:
            return
        self._matched_nodes.append(node)

    def _matches(self, node: astroid.NodeNG, depth: int):
        if depth >= len(self._match_nodes):
            return False
        pair = self._match_nodes[depth]
        if not isinstance(node, pair[1]):
            return False
        next_node: astroid.NodeNG | None = None
        if isinstance(node, astroid.Attribute):
            if node.attr != pair[0]:
                return False
            next_node = node.value
        elif isinstance(node, astroid.Name):
            if node.id != pair[0]:
                return False
        else:
            raise NotImplementedError(str(type(node)))
        if next_node is None:
            # is this the last node to match ?
            return len(self._match_nodes) - 1 == depth
        return self._matches(next_node, depth + 1)


class NodeBase(abc.ABC):

    def __init__(self, node: astroid.NodeNG):
        self._node = node

    @property
    def node(self):
        return self._node

    def __repr__(self):
        return f"<{self.__class__.__name__}: {repr(self._node)}>"


class SysPathChange(NodeBase, abc.ABC):

    def __init__(self, node: astroid.NodeNG, path: str, is_append: bool):
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


class SysPathVisitor(astroid.NodeVisitor):

    def __init__(self):
        self._aliases: dict[str, str] = {}
        self._syspath_changes: list[SysPathChange] = []

    @property
    def syspath_changes(self):
        return self._syspath_changes

    def visit_Import(self, node: astroid.Import):
        for alias in node.names:
            if alias.name in {"sys", "os"}:
                self._aliases[alias.name] = alias.asname or alias.name

    def visit_ImportFrom(self, node: astroid.ImportFrom):
        interesting_aliases = [("sys", "path"), ("os", "path"), ("os.path", "abspath")]
        interesting_alias = next((t for t in interesting_aliases if t[0] == node.module), None)
        if interesting_alias is None:
            return
        for alias in node.names:
            if alias.name == interesting_alias[1]:
                self._aliases[f"{node.module}.{interesting_alias[1]}"] = alias.asname or alias.name
                break

    def visit_Call(self, node: astroid.Call):
        func = cast(astroid.Attribute, node.func)
        # check for 'sys.path.append'
        if not (
            self._match_aliases(func, ["sys", "path", "append"]) or self._match_aliases(func, ["sys", "path", "insert"])
        ):
            return
        is_append = func.attr == "append"
        changed = node.args[0] if is_append else node.args[1]
        if isinstance(changed, astroid.Constant):
            self._syspath_changes.append(AbsolutePath(node, changed.value, is_append))
        elif isinstance(changed, astroid.Call):
            self._visit_relative_path(changed, is_append)

    def _match_aliases(self, node: astroid.NodeNG, names: list[str]):
        if isinstance(node, astroid.Attribute):
            if node.attr != names[-1]:
                return False
            if len(names) == 1:
                return True
            return self._match_aliases(node.value, names[0 : len(names) - 1])
        if isinstance(node, astroid.Name):
            full_name = ".".join(names)
            alias = self._aliases.get(full_name, full_name)
            return node.id == alias
        return False

    def _visit_relative_path(self, node: astroid.NodeNG, is_append: bool):
        # check for 'os.path.abspath'
        if not self._match_aliases(node.func, ["os", "path", "abspath"]):
            return
        changed = node.args[0]
        if isinstance(changed, astroid.Constant):
            self._syspath_changes.append(RelativePath(changed, changed.value, is_append))


T = TypeVar("T", bound=astroid.NodeNG)


# disclaimer this class is NOT thread-safe
class ASTLinter(Generic[T]):

    @staticmethod
    def parse(code: str):
        root = astroid.parse(code)
        return ASTLinter(root)

    def __init__(self, root: astroid.Module):
        self._root: astroid.Module = root

    def locate(self, node_type: type[T], match_nodes: list[tuple[str, type]]) -> list[T]:
        visitor = MatchingVisitor(node_type, match_nodes)
        visitor.visit(self._root)
        return visitor.matched_nodes

    def collect_sys_paths_changes(self):
        visitor = SysPathVisitor()
        visitor.visit(self._root)
        return visitor.syspath_changes

    def extract_callchain(self) -> astroid.Call | None:
        """If 'node' is an assignment or expression, extract its full call-chain (if it has one)"""
        call = None
        if isinstance(self._root, astroid.Assign):
            call = self._root.value
        elif isinstance(self._root, astroid.Expr):
            call = self._root.value
        if not isinstance(call, astroid.Call):
            call = None
        return call

    def extract_call_by_name(self, name: str) -> astroid.Call | None:
        """Given a call-chain, extract its sub-call by method name (if it has one)"""
        assert isinstance(self._root, astroid.Call)
        node = self._root
        while True:
            func = node.func
            if not isinstance(func, astroid.Attribute):
                return None
            if func.attr == name:
                return node
            if not isinstance(func.value, astroid.Call):
                return None
            node = func.value

    def args_count(self) -> int:
        """Count the number of arguments (positionals + keywords)"""
        assert isinstance(self._root, astroid.Call)
        return len(self._root.args) + len(self._root.keywords)

    def get_arg(
        self,
        arg_index: int | None,
        arg_name: str | None,
    ) -> astroid.expr | None:
        """Extract the call argument identified by an optional position or name (if it has one)"""
        assert isinstance(self._root, astroid.Call)
        if arg_index is not None and len(self._root.args) > arg_index:
            return self._root.args[arg_index]
        if arg_name is not None:
            arg = [kw.value for kw in self._root.keywords if kw.arg == arg_name]
            if len(arg) == 1:
                return arg[0]
        return None

    def is_none(self) -> bool:
        """Check if the given AST expression is the None constant"""
        assert isinstance(self._root, astroid.expr)
        if not isinstance(self._root, astroid.Constant):
            return False
        return self._root.value is None

    def __repr__(self):
        truncate_after = 32
        code = repr(self._root)
        if len(code) > truncate_after:
            code = code[0:truncate_after] + "..."
        return f"<ASTLinter: {code}>"


class ImportSource(NodeBase):

    def __init__(self, node: astroid.NodeNG, name: str):
        super().__init__(node)
        self.name = name


class NotebookRunCall(NodeBase):

    def __init__(self, node: astroid.Call):
        super().__init__(node)

    def get_constant_path(self) -> str | None:
        path = PythonLinter.get_dbutils_notebook_run_path_arg(cast(astroid.Call, self.node))
        if isinstance(path, astroid.Constant):
            return path.value.strip().strip("'").strip('"')
        return None


P = TypeVar("P", bound=Callable)


class PythonLinter(Linter):

    def lint(self, code: str) -> Iterable[Advice]:
        linter = ASTLinter.parse(code)
        nodes = self.list_dbutils_notebook_run_calls(linter)
        return [self._convert_dbutils_notebook_run_to_advice(node.node) for node in nodes]

    @classmethod
    def _convert_dbutils_notebook_run_to_advice(cls, node: astroid.NodeNG) -> Advisory:
        assert isinstance(node, astroid.Call)
        path = cls.get_dbutils_notebook_run_path_arg(node)
        if isinstance(path, astroid.Constant):
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
    def get_dbutils_notebook_run_path_arg(node: astroid.Call):
        if len(node.args) > 0:
            return node.args[0]
        arg = next(kw for kw in node.keywords if kw.arg == "path")
        return arg.value if arg is not None else None

    @staticmethod
    def list_dbutils_notebook_run_calls(linter: ASTLinter) -> list[NotebookRunCall]:
        calls = linter.locate(astroid.Call, [("run", astroid.Attribute), ("notebook", astroid.Attribute), ("dbutils", astroid.Name)])
        return [NotebookRunCall(call) for call in calls]

    @staticmethod
    def list_import_sources(linter: ASTLinter, problem_type: P) -> tuple[list[ImportSource], list[P]]:
        problems: list[P] = []
        try:  # pylint: disable=too-many-try-statements
            nodes = linter.locate(astroid.Import, [])
            sources = [ImportSource(node, alias.name) for node in nodes for alias in node.names]
            nodes = linter.locate(astroid.ImportFrom, [])
            sources.extend(ImportSource(node, node.module) for node in nodes)
            nodes = linter.locate(astroid.Call, [("import_module", astroid.Attribute), ("importlib", astroid.Name)])
            nodes.extend(linter.locate(astroid.Call, [("__import__", astroid.Attribute), ("importlib", astroid.Name)]))
            for node in nodes:
                if isinstance(node.args[0], astroid.Constant):
                    sources.append(ImportSource(node, node.args[0].value))
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
            return sources, problems
        except Exception as e:  # pylint: disable=broad-except
            problem = problem_type('internal-error', f"While linter {linter} was checking imports: {e}")
            problems.append(problem)
            return [], problems

    @staticmethod
    def list_sys_path_changes(linter: ASTLinter) -> list[SysPathChange]:
        return linter.collect_sys_paths_changes()
