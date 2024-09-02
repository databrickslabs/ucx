from __future__ import annotations

import logging
import re
import shlex
from abc import ABC, abstractmethod
from ast import parse as parse_python
from collections.abc import Callable, Iterable
from enum import Enum
from pathlib import Path
from typing import TypeVar, cast

from astroid import Call, Const, ImportFrom, Name, NodeNG, Try  # type: ignore
from astroid.exceptions import AstroidSyntaxError  # type: ignore
from sqlglot import parse as parse_sql, ParseError as SQLParseError

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import NOTEBOOK_HEADER
from databricks.labs.ucx.source_code.graph import (
    DependencyGraph,
    DependencyProblem,
    DependencyGraphContext,
    InheritedContext,
)
from databricks.labs.ucx.source_code.linters.imports import (
    SysPathChange,
    DbutilsPyLinter,
    ImportSource,
    NotebookRunCall,
    UnresolvedPath,
)
from databricks.labs.ucx.source_code.linters.python_ast import Tree, NodeBase

# use a specific logger for sqlglot warnings so we can disable them selectively
sqlglot_logger = logging.getLogger(f"{__name__}.sqlglot")
logger = logging.getLogger(__name__)

CELL_SEPARATOR = "COMMAND ----------"
MAGIC_PREFIX = 'MAGIC'
LANGUAGE_PREFIX = '%'
LANGUAGE_PI = 'LANGUAGE'
COMMENT_PI = 'COMMENT'


class Cell(ABC):

    def __init__(self, source: str, original_offset: int):
        # The header line prevents a cell from having a zero offset.
        if original_offset < 1:
            raise ValueError("Cells must have a positive offset within the original file.")
        self._original_offset = original_offset
        self._original_code = source
        self._migrated_code = source

    @property
    def original_offset(self) -> int:
        """Line offset of this cell within the original notebook file.

        Example: if the line offset is 5, then line 4 within this cell corresponds to line 9 of the original file.
        """
        return self._original_offset

    @property
    def original_code(self):
        return self._original_code

    @property
    def migrated_code(self):
        return self._migrated_code

    @migrated_code.setter
    def migrated_code(self, value: str):
        self._migrated_code = value

    @property
    @abstractmethod
    def language(self) -> CellLanguage: ...

    @abstractmethod
    def is_runnable(self) -> bool: ...

    def build_dependency_graph(self, _: DependencyGraph) -> list[DependencyProblem]:
        """Check for any problems with dependencies of this cell.

        Returns:
            A list of found dependency problems; position information for problems is relative to the enclosing notebook.
        """
        return []

    def __repr__(self):
        return f"{self.language.name}: {self._original_code[:20]}"

    def build_inherited_context(self, _graph: DependencyGraph, _child_path: Path) -> InheritedContext:
        return InheritedContext(None, False)


class PythonCell(Cell):

    @property
    def language(self):
        return CellLanguage.PYTHON

    def is_runnable(self) -> bool:
        try:
            tree = parse_python(self._original_code)
            return tree is not None
        except SyntaxError:
            return True

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        context = parent.new_dependency_graph_context()
        analyzer = PythonCodeAnalyzer(context, self._original_code)
        python_dependency_problems = analyzer.build_graph()
        # Position information for the Python code is within the code and needs to be mapped to the location within the parent nodebook.
        return [
            problem.replace(
                start_line=self.original_offset + problem.start_line, end_line=self.original_offset + problem.end_line
            )
            for problem in python_dependency_problems
        ]

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        context = graph.new_dependency_graph_context()
        analyzer = PythonCodeAnalyzer(context, self._original_code)
        return analyzer.build_inherited_context(child_path)


class RCell(Cell):

    @property
    def language(self):
        return CellLanguage.R

    def is_runnable(self) -> bool:
        return True


class ScalaCell(Cell):

    @property
    def language(self):
        return CellLanguage.SCALA

    def is_runnable(self) -> bool:
        return True


class SQLCell(Cell):

    @property
    def language(self):
        return CellLanguage.SQL

    def is_runnable(self) -> bool:
        try:
            statements = parse_sql(self._original_code)
            return len(statements) > 0
        except SQLParseError as e:
            sqlglot_logger.warning(f"Failed to parse SQL using 'sqlglot': {self._original_code}", exc_info=e)
            return True


class MarkdownCell(Cell):

    @property
    def language(self):
        return CellLanguage.MARKDOWN

    def is_runnable(self) -> bool:
        return True  # TODO


class RunCell(Cell):

    @property
    def language(self):
        return CellLanguage.RUN

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        path, idx, line = self._read_notebook_path()
        if path is not None:
            start_line = self._original_offset + idx
            problems = parent.register_notebook(path, True)
            return [
                problem.replace(start_line=start_line, start_col=0, end_line=start_line, end_col=len(line))
                for problem in problems
            ]
        start_line = self._original_offset
        problem = DependencyProblem(
            'invalid-run-cell',
            "Missing notebook path in %run command",
            start_line=start_line,
            start_col=0,
            end_line=start_line,
            end_col=len(self._original_code),
        )
        return [problem]

    def maybe_notebook_path(self) -> Path | None:
        path, _, _ = self._read_notebook_path()
        return path

    def _read_notebook_path(self):
        command = f'{LANGUAGE_PREFIX}{self.language.magic_name}'
        lines = self._original_code.split('\n')
        for idx, line in enumerate(lines):
            start = line.find(command)
            if start >= 0:
                path = line[start + len(command) :]
                path = path.strip().strip("'").strip('"')
                if len(path) == 0:
                    continue
                return Path(path), idx, line
        return None, 0, ""

    def migrate_notebook_path(self):
        pass


class ShellCell(Cell):

    @property
    def language(self):
        return CellLanguage.SHELL

    def is_runnable(self) -> bool:
        return True  # TODO


class PipCell(Cell):

    @property
    def language(self):
        return CellLanguage.PIP

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, graph: DependencyGraph) -> list[DependencyProblem]:
        node = MagicNode(0, 1, None, end_lineno=0, end_col_offset=len(self.original_code))
        return PipCommand(node, self.original_code).build_dependency_graph(graph)


class CellLanguage(Enum):
    # long magic_names must come first to avoid shorter ones being matched
    PYTHON = Language.PYTHON, 'python', '#', True, PythonCell
    SCALA = Language.SCALA, 'scala', '//', True, ScalaCell
    SQL = Language.SQL, 'sql', '--', True, SQLCell
    RUN = None, 'run', '', False, RunCell
    PIP = None, 'pip', '', False, PipCell
    SHELL = None, 'sh', '', False, ShellCell
    # see https://spec.commonmark.org/0.31.2/#html-comment
    MARKDOWN = None, 'md', "<!--->", False, MarkdownCell
    R = Language.R, 'r', '#', True, RCell

    def __init__(self, *args):
        super().__init__()
        self._language = args[0]
        self._magic_name = args[1]
        self._comment_prefix = args[2]
        # PI stands for Processing Instruction
        self._requires_isolated_pi = args[3]
        self._new_cell = args[4]

    @property
    def file_magic_header(self):
        return f"{self._comment_prefix} {NOTEBOOK_HEADER}"

    @property
    def language(self) -> Language:
        return self._language

    @property
    def magic_name(self) -> str:
        return self._magic_name

    @property
    def comment_prefix(self) -> str:
        return self._comment_prefix

    @property
    def requires_isolated_pi(self) -> str:
        return self._requires_isolated_pi

    @classmethod
    def of_language(cls, language: Language) -> CellLanguage:
        # TODO: Should this not raise a ValueError if the language is not found?
        #  It also  causes a GeneratorExit exception to be raised. Maybe an explicit loop is better.
        return next((cl for cl in CellLanguage if cl.language == language))

    @classmethod
    def of_magic_name(cls, magic_name: str) -> CellLanguage | None:
        return next((cl for cl in CellLanguage if magic_name.startswith(cl.magic_name)), None)

    def read_cell_language(self, lines: list[str]) -> CellLanguage | None:
        magic_prefix = f'{self.comment_prefix} {MAGIC_PREFIX} '
        magic_language_prefix = f'{magic_prefix}{LANGUAGE_PREFIX}'
        for line in lines:
            # if we find a non-comment then we're done
            if not line.startswith(self.comment_prefix):
                return None
            # if it's not a magic comment, skip it
            if not line.startswith(magic_prefix):
                continue
            if line.startswith(magic_language_prefix):
                line = line[len(magic_language_prefix) :]
                return CellLanguage.of_magic_name(line.strip())
            return None
        return None

    def new_cell(self, source: str, original_offset: int) -> Cell:
        return self._new_cell(source, original_offset)

    def extract_cells(self, source: str) -> list[Cell]:
        lines = source.split('\n')
        if not lines[0].startswith(self.file_magic_header):
            raise ValueError("Not a Databricks notebook source!")

        def make_cell(cell_lines: list[str], start: int) -> Cell:
            # trim leading blank lines
            while len(cell_lines) > 0 and len(cell_lines[0]) == 0:
                cell_lines.pop(0)
                start += 1

            # trim trailing blank lines
            while len(cell_lines) > 0 and len(cell_lines[-1]) == 0:
                cell_lines.pop(-1)
            cell_language = self.read_cell_language(cell_lines)
            if cell_language is None:
                cell_language = self
            else:
                cell_lines = self._remove_magic_wrapper(cell_lines, cell_language)

            cell_source = '\n'.join(cell_lines)
            cell = cell_language.new_cell(cell_source, start)
            return cell

        cells = []
        cell_lines: list[str] = []
        separator = f"{self.comment_prefix} {CELL_SEPARATOR}"

        # Start iterating from the 2nd line (after the header) to split the cells.
        next_cell_pos = 1
        for i in range(1, len(lines)):
            line = lines[i].strip()
            if line.startswith(separator):
                cell = make_cell(cell_lines, next_cell_pos)
                cells.append(cell)
                cell_lines = []
                # i points to the cell separator, the next cell begins 1 line later.
                next_cell_pos = i + 1
            else:
                cell_lines.append(lines[i])

        if len(cell_lines) > 0:
            cell = make_cell(cell_lines, next_cell_pos)
            cells.append(cell)

        return cells

    def _process_line(self, line: str, prefix: str, lang_prefix: str, cell_language: CellLanguage) -> list[str]:
        if line.startswith(prefix):
            line = line[len(prefix) :]
            if cell_language.requires_isolated_pi and line.startswith(lang_prefix):
                new_line = line[len(lang_prefix) :].strip()
                if new_line:
                    return [f"{cell_language.comment_prefix} {LANGUAGE_PI}", new_line.strip()]
                return [f"{cell_language.comment_prefix} {LANGUAGE_PI}"]
            return [line]
        if line.startswith(self.comment_prefix):
            return [f"{cell_language.comment_prefix} {COMMENT_PI}{line}"]
        return [line]

    def _remove_magic_wrapper(self, lines: list[str], cell_language: CellLanguage) -> list[str]:
        prefix = f"{self.comment_prefix} {MAGIC_PREFIX} "
        lang_prefix = f"{LANGUAGE_PREFIX}{cell_language.magic_name}"
        new_lines = []
        for line in lines:
            new_lines.extend(self._process_line(line, prefix, lang_prefix, cell_language))
        return new_lines

    def wrap_with_magic(self, code: str, cell_language: CellLanguage) -> str:
        language_pi_prefix = f"{cell_language.comment_prefix} {LANGUAGE_PI}"
        comment_pi_prefix = f"{cell_language.comment_prefix} {COMMENT_PI}"
        comment_pi_prefix_len = len(comment_pi_prefix)
        lines = code.split('\n')
        for i, line in enumerate(lines):
            if line.startswith(language_pi_prefix):
                line = f"{self.comment_prefix} {MAGIC_PREFIX} {LANGUAGE_PREFIX}{cell_language.magic_name}"
                lines[i] = line
                continue
            if line.startswith(comment_pi_prefix):
                lines[i] = line[comment_pi_prefix_len:]
                continue
            line = f"{self.comment_prefix} {MAGIC_PREFIX} {line}"
            lines[i] = line
        if code.endswith('./'):
            lines.append('\n')
        return "\n".join(lines)


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
                problem = problem.replace(
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


T = TypeVar("T")


class MagicLine(NodeBase):

    @classmethod
    def extract_from_tree(
        cls, tree: Tree, problem_factory: Callable[[str, str, NodeNG], T]
    ) -> tuple[list[MagicLine], list[T]]:
        problems: list[T] = []
        commands: list[MagicLine] = []
        try:
            nodes = tree.locate(Call, [("magic_command", Name)])
            for command in cls._make_commands_for_magic_command_call_nodes(nodes):
                commands.append(command)
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f"Internal error while checking magic commands in tree: {tree.root}", exc_info=True)
            problem = problem_factory('internal-error', f"While checking magic commands: {e}", tree.root)
            problems.append(problem)
        return commands, problems

    @classmethod
    def _make_commands_for_magic_command_call_nodes(cls, nodes: list[Call]):
        for node in nodes:
            arg = node.args[0]
            if isinstance(arg, Const):
                yield MagicLine(node, arg.value)

    def __init__(self, node: NodeNG, command: bytes):
        super().__init__(node)
        self._command = command.decode()

    def as_magic(self) -> MagicCommand | None:
        if self._command.startswith("%pip") or self._command.startswith("!pip"):
            return PipCommand(self.node, self._command)
        if self._command.startswith("%run"):
            return RunCommand(self.node, self._command)
        return None

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        magic = self.as_magic()
        if magic is not None:
            return magic.build_dependency_graph(parent)
        problem = DependencyProblem.from_node(
            code='unsupported-magic-line', message=f"magic line '{self._command}' is not supported yet", node=self.node
        )
        return [problem]

    def build_inherited_context(self, context: DependencyGraphContext, child_path: Path) -> InheritedContext:
        magic = self.as_magic()
        if magic is not None:
            return magic.build_inherited_context(context, child_path)
        return InheritedContext(None, False)


class MagicNode(NodeNG):
    pass


class MagicCommand(ABC):

    def __init__(self, node: NodeNG, code: str):
        self._node = node
        self._code = code

    @abstractmethod
    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]: ...

    def build_inherited_context(self, _context: DependencyGraphContext, _child_path: Path) -> InheritedContext:
        return InheritedContext(None, False)


class RunCommand(MagicCommand):

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        path = self.notebook_path
        if path is not None:
            problems = parent.register_notebook(path, True)
            return [problem.from_node(problem.code, problem.message, self._node) for problem in problems]
        problem = DependencyProblem.from_node('invalid-run-cell', "Missing notebook path in %run command", self._node)
        return [problem]

    @property
    def notebook_path(self) -> Path | None:
        start = self._code.find(' ')
        if start < 0:
            return None
        path = self._code[start + 1 :].strip().strip('"').strip("'")
        return Path(path)

    def build_inherited_context(self, context: DependencyGraphContext, child_path: Path) -> InheritedContext:
        path = self.notebook_path
        if path is None:
            logger.warning("Missing notebook path in %run command")
            return InheritedContext(None, False)
        maybe = context.resolver.resolve_notebook(context.path_lookup, path, inherit_context=True)
        if not maybe.dependency:
            logger.warning(f"Could not load notebook {path}")
            return InheritedContext(None, False)
        child_path = maybe.dependency.path
        absolute_path = context.path_lookup.resolve(path)
        absolute_child = context.path_lookup.resolve(child_path)
        if absolute_path == absolute_child:
            return InheritedContext(None, True)
        container = maybe.dependency.load(context.path_lookup)
        if not container:
            logger.warning(f"Could not load notebook {path}")
            return InheritedContext(None, False)
        return container.build_inherited_context(context.parent, child_path)


class PipCommand(MagicCommand):

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        argv = self._split(self._code)
        if len(argv) == 1:
            return [DependencyProblem.from_node("library-install-failed", "Missing command after 'pip'", self._node)]
        if argv[1] != "install":
            return [
                DependencyProblem.from_node(
                    "library-install-failed", f"Unsupported 'pip' command: {argv[1]}", self._node
                )
            ]
        if len(argv) == 2:
            return [
                DependencyProblem.from_node(
                    "library-install-failed", "Missing arguments after 'pip install'", self._node
                )
            ]
        problems = parent.register_library(*argv[2:])  # Skipping %pip install
        return [problem.from_node(problem.code, problem.message, self._node) for problem in problems]

    # Cache re-used regex (and ensure issues are raised during class init instead of upon first use).
    _splitter = re.compile(r"(?<!\\)\n")

    @classmethod
    def _split(cls, code: str) -> list[str]:
        """Split pip cell code into multiple arguments

        Note:
            PipCell should be a pip command, i.e. single line possible spanning multilines escaped with backslashes.

        Sources:
            https://docs.databricks.com/en/libraries/notebooks-python-libraries.html#manage-libraries-with-pip-commands
        """
        match = cls._splitter.search(code)
        if match:
            code = code[: match.start()]  # Remove code after non-escaped newline
        code = code.replace("\\\n", " ")
        lexer = shlex.split(code, posix=True)
        return list(lexer)
