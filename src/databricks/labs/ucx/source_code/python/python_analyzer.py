from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import cast

from astroid import AstroidSyntaxError, ImportFrom, Try, Name  # type: ignore

from databricks.labs.ucx.source_code.graph import (
    DependencyGraphContext,
    DependencyProblem,
    InheritedContext,
)
from databricks.labs.ucx.source_code.linters.imports import (
    SysPathChange,
    DbutilsPyLinter,
    ImportSource,
    NotebookRunCall,
    UnresolvedPath,
)
from databricks.labs.ucx.source_code.notebooks.magic import MagicLine
from databricks.labs.ucx.source_code.python.python_ast import Tree, NodeBase

logger = logging.getLogger(__name__)


class PythonCodeAnalyzer:

    def __init__(self, context: DependencyGraphContext, python_code: str):
        self._context = context
        self._python_code = python_code

    def build_graph(self) -> list[DependencyProblem]:
        """Check python code for dependency-related problems.

        Returns:
            A list of dependency problems; position information is relative to the python code itself.
        """
        problems: list[DependencyProblem] = []
        try:
            _, nodes, parse_problems = self._parse_and_extract_nodes()
            problems.extend(parse_problems)
        except AstroidSyntaxError as e:
            logger.debug(f"Could not parse Python code: {self._python_code}", exc_info=True)
            problems.append(DependencyProblem('parse-error', f"Could not parse Python code: {e}"))
            return problems
        for base_node in nodes:
            for problem in self._build_graph_from_node(base_node):
                # Astroid line numbers are 1-based.
                problem = dataclasses.replace(
                    problem,
                    start_line=base_node.node.lineno - 1,
                    start_col=base_node.node.col_offset,
                    end_line=(base_node.node.end_lineno or 1) - 1,
                    end_col=base_node.node.end_col_offset or 0,
                )
                problems.append(problem)
        return problems

    def build_inherited_context(self, child_path: Path) -> InheritedContext:
        try:
            tree, nodes, _ = self._parse_and_extract_nodes()
        except AstroidSyntaxError:
            logger.debug(f"Could not parse Python code: {self._python_code}", exc_info=True)
            return InheritedContext(None, False)
        if len(nodes) == 0:
            return InheritedContext(tree, False)
        context = InheritedContext(Tree.new_module(), False)
        last_line = -1
        for base_node in nodes:
            # append nodes
            node_line = base_node.node.lineno
            nodes = tree.nodes_between(last_line + 1, node_line - 1)
            context.tree.append_nodes(nodes)
            globs = tree.globals_between(last_line + 1, node_line - 1)
            context.tree.append_globals(globs)
            last_line = node_line
            # process node
            child_context = self._build_inherited_context_from_node(base_node, child_path)
            context = context.append(child_context, True)
            if context.found:
                return context
        line_count = tree.line_count()
        if last_line < line_count:
            nodes = tree.nodes_between(last_line + 1, line_count)
            context.tree.append_nodes(nodes)
            globs = tree.globals_between(last_line + 1, line_count)
            context.tree.append_globals(globs)
        return context

    def _parse_and_extract_nodes(self) -> tuple[Tree, list[NodeBase], Iterable[DependencyProblem]]:
        problems: list[DependencyProblem] = []
        tree = Tree.normalize_and_parse(self._python_code)
        syspath_changes = SysPathChange.extract_from_tree(self._context.session_state, tree)
        run_calls = DbutilsPyLinter.list_dbutils_notebook_run_calls(tree)
        import_sources: list[ImportSource]
        import_problems: list[DependencyProblem]
        import_sources, import_problems = ImportSource.extract_from_tree(tree, DependencyProblem.from_node)
        problems.extend(import_problems)
        magic_lines, command_problems = MagicLine.extract_from_tree(tree, DependencyProblem.from_node)
        problems.extend(command_problems)
        # need to evaluate things in intertwined sequence so concat and sort them
        nodes: list[NodeBase] = cast(list[NodeBase], syspath_changes + run_calls + import_sources + magic_lines)
        nodes = sorted(nodes, key=lambda node: (node.node.lineno, node.node.col_offset))
        return tree, nodes, problems

    def _build_graph_from_node(self, base_node: NodeBase) -> Iterable[DependencyProblem]:
        if isinstance(base_node, SysPathChange):
            yield from self._mutate_path_lookup(base_node)
        elif isinstance(base_node, NotebookRunCall):
            yield from self._register_notebook(base_node)
        elif isinstance(base_node, ImportSource):
            yield from self._register_import(base_node)
        elif isinstance(base_node, MagicLine):
            yield from base_node.build_dependency_graph(self._context.parent)
        else:
            logger.warning(f"Can't build graph for node {NodeBase.__name__} of type {type(base_node).__name__}")

    def _build_inherited_context_from_node(self, base_node: NodeBase, child_path: Path) -> InheritedContext:
        if isinstance(base_node, SysPathChange):
            self._mutate_path_lookup(base_node)
            return InheritedContext(None, False)
        if isinstance(base_node, ImportSource):
            # nothing to do, Astroid takes care of imports
            return InheritedContext(None, False)
        if isinstance(base_node, NotebookRunCall):
            # nothing to do, dbutils.notebook.run uses a dedicated context
            return InheritedContext(None, False)
        if isinstance(base_node, MagicLine):
            return base_node.build_inherited_context(self._context, child_path)
        logger.warning(f"Can't build inherited context for node {NodeBase.__name__} of type {type(base_node).__name__}")
        return InheritedContext(None, False)

    def _register_import(self, base_node: ImportSource) -> Iterable[DependencyProblem]:
        prefix = ""
        if isinstance(base_node.node, ImportFrom) and base_node.node.level is not None:
            prefix = "." * base_node.node.level
        name = base_node.name or ""
        problems = self._context.parent.register_import(prefix + name)
        for problem in problems:
            prob = self._filter_import_problem_in_try_except(problem, base_node)
            if prob is not None:
                yield prob

    @classmethod
    def _filter_import_problem_in_try_except(
        cls, problem: DependencyProblem, base_node: ImportSource
    ) -> DependencyProblem | None:
        if problem.code != 'import-not-found':
            return problem
        # is base_node in a try-except clause ?
        node = base_node.node.parent
        while node and not isinstance(node, Try):
            node = node.parent
        if cls._is_try_except_import_error(node):
            return None
        return problem

    @classmethod
    def _is_try_except_import_error(cls, node: Try | None) -> bool:
        if not isinstance(node, Try):
            return False
        for handler in node.handlers:
            if isinstance(handler.type, Name):
                if handler.type.name == "ImportError":
                    return True
        return False

    def _register_notebook(self, base_node: NotebookRunCall) -> Iterable[DependencyProblem]:
        has_unresolved, paths = base_node.get_notebook_paths(self._context.session_state)
        if has_unresolved:
            yield DependencyProblem(
                'dependency-cannot-compute-value',
                f"Can't check dependency from {base_node.node.as_string()} because the expression cannot be computed",
            )
        for path in paths:
            # notebooks ran via dbutils.notebook.run do not inherit or propagate context
            yield from self._context.parent.register_notebook(Path(path), False)

    def _mutate_path_lookup(self, change: SysPathChange) -> Iterable[DependencyProblem]:
        if isinstance(change, UnresolvedPath):
            yield DependencyProblem(
                'sys-path-cannot-compute-value',
                f"Can't update sys.path from {change.node.as_string()} because the expression cannot be computed",
            )
            return
        change.apply_to(self._context.path_lookup)
