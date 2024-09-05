from __future__ import annotations

import abc
import logging
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TypeVar, cast

from astroid import (  # type: ignore
    Attribute,
    Call,
    Const,
    InferenceError,
    Import,
    ImportFrom,
    Name,
    NodeNG,
)

from databricks.labs.ucx.source_code.base import Advice, Advisory, CurrentSessionState, PythonLinter
from databricks.labs.ucx.source_code.python.python_ast import Tree, NodeBase, TreeVisitor
from databricks.labs.ucx.source_code.python.python_infer import InferredValue
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.getLogger(__name__)

T = TypeVar("T")
ProblemFactory = Callable[[str, str, NodeNG], T]


class ImportSource(NodeBase):

    @classmethod
    def extract_from_tree(cls, tree: Tree, problem_factory: ProblemFactory) -> tuple[list[ImportSource], list[T]]:
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
            for source in cls._make_sources_for_import_call_nodes(nodes, problem_factory, problems):
                sources.append(source)
            return sources, problems
        except Exception as e:  # pylint: disable=broad-except
            problem = problem_factory('internal-error', f"While checking imports: {e}", tree.node)
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
    def _make_sources_for_import_call_nodes(cls, nodes: list[Call], problem_factory: ProblemFactory, problems: list[T]):
        for node in nodes:
            arg = node.args[0]
            if isinstance(arg, Const):
                yield ImportSource(node, arg.value)
                continue
            problem = problem_factory(
                'dependency-not-constant', "Can't check dependency not provided as a constant", node
            )
            problems.append(problem)

    def __init__(self, node: NodeNG, name: str):
        super().__init__(node)
        self.name = name


class NotebookRunCall(NodeBase):

    def __init__(self, node: Call):
        super().__init__(node)

    def get_notebook_paths(self, session_state: CurrentSessionState) -> tuple[bool, list[str]]:
        """we return multiple paths because astroid can infer them in scenarios such as:
        paths = ["p1", "p2"]
        for path in paths:
            dbutils.notebook.run(path)
        """
        arg = DbutilsPyLinter.get_dbutils_notebook_run_path_arg(self.node)
        try:
            all_inferred = InferredValue.infer_from_node(arg, session_state)
            return self._get_notebook_paths(all_inferred)
        except InferenceError:
            logger.debug(f"Can't infer value(s) of {arg.as_string()}")
            return True, []

    @classmethod
    def _get_notebook_paths(cls, all_inferred: Iterable[InferredValue]) -> tuple[bool, list[str]]:
        has_unresolved = False
        paths: list[str] = []
        for inferred in all_inferred:
            if inferred.is_inferred():
                paths.append(inferred.as_string().strip("'").strip('"'))
                continue
            typenames = [type(node).__name__ for node in inferred.nodes]
            logger.debug(f"Can't compute nodes [{','.join(typenames)}]")
            has_unresolved = True
        return has_unresolved, paths


class DbutilsPyLinter(PythonLinter):

    def __init__(self, session_state: CurrentSessionState):
        self._session_state = session_state

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        nodes = self.list_dbutils_notebook_run_calls(tree)
        for node in nodes:
            yield from self._raise_advice_if_unresolved(node.node, self._session_state)

    @classmethod
    def _raise_advice_if_unresolved(cls, node: NodeNG, session_state: CurrentSessionState) -> Iterable[Advice]:
        assert isinstance(node, Call)
        call = NotebookRunCall(cast(Call, node))
        has_unresolved, _ = call.get_notebook_paths(session_state)
        if has_unresolved:
            yield Advisory.from_node(
                code='notebook-run-cannot-compute-value',
                message="Path for 'dbutils.notebook.run' cannot be computed and requires adjusting the notebook path(s)",
                node=node,
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
    def extract_from_tree(session_state: CurrentSessionState, tree: Tree) -> list[SysPathChange]:
        visitor = SysPathChangesVisitor(session_state)
        visitor.visit(tree.node)
        return visitor.sys_path_changes

    def __init__(self, node: NodeNG, path: str, is_append: bool):
        super().__init__(node)
        self._path = path
        self._is_append = is_append

    @property
    def path(self):
        return self._path

    def apply_to(self, path_lookup: PathLookup):
        path = Path(self._path)
        if not path.is_absolute():
            path = path_lookup.cwd / path
        if self._is_append:
            path_lookup.append_path(path)
            return
        path_lookup.prepend_path(path)


class AbsolutePath(SysPathChange):
    # path directly added to sys.path
    pass


class RelativePath(SysPathChange):
    # path added to sys.path using os.path.abspath
    pass


class UnresolvedPath(SysPathChange):
    # path added to sys.path that cannot be inferred
    pass


class SysPathChangesVisitor(TreeVisitor):

    def __init__(self, session_state: CurrentSessionState) -> None:
        super()
        self._session_state = session_state
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
        relative = False
        if isinstance(changed, Call):
            if not self._match_aliases(changed.func, ["os", "path", "abspath"]):
                return
            relative = True
            changed = changed.args[0]
        try:
            for inferred in InferredValue.infer_from_node(changed, self._session_state):
                self._visit_inferred(changed, inferred, relative, is_append)
        except InferenceError:
            self.sys_path_changes.append(UnresolvedPath(changed, changed.as_string(), is_append))

    def _visit_inferred(self, changed: NodeNG, inferred: InferredValue, is_relative: bool, is_append: bool):
        if not inferred.is_inferred():
            self.sys_path_changes.append(UnresolvedPath(changed, changed.as_string(), is_append))
            return
        if is_relative:
            self.sys_path_changes.append(RelativePath(changed, inferred.as_string(), is_append))
        else:
            self.sys_path_changes.append(AbsolutePath(changed, inferred.as_string(), is_append))

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
