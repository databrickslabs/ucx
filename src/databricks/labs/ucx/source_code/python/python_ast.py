from __future__ import annotations

import builtins
import logging
import sys
import re
from abc import ABC
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypeVar, cast

from astroid import (  # type: ignore
    Assign,
    AssignName,
    Attribute,
    Call,
    ClassDef,
    Const,
    Import,
    ImportFrom,
    Instance,
    JoinedStr,
    Module,
    Name,
    NodeNG,
    parse,
    Uninferable,
)
from astroid.exceptions import AstroidSyntaxError  # type: ignore

from databricks.labs.ucx.github import IssueType, construct_new_issue_url
from databricks.labs.ucx.source_code.base import Failure


logger = logging.getLogger(__name__)
missing_handlers: set[str] = set()


T = TypeVar("T", bound=NodeNG)


@dataclass(frozen=True)
class MaybeTree:
    """A :class:`Tree` or a :class:`Failure`.

    The `MaybeTree` is designed to either contain a `Tree` OR a `Failure`,
    never both or neither. Typically, a `Tree` is constructed using the
    `MaybeTree` class method(s) that yields a `Failure` if the `Tree` could
    NOT be constructed, otherwise it yields the `Tree`, resulting in code that
    looks like:

    ``` python
    maybe_tree = Tree.from_source_code("print(1)")
    if maybe_tree.failure:
        # Handle failure and return early
    assert maybe_tree.tree, "Tree should be not-None when Failure is None."
    # Use tree
    ```
    """

    tree: Tree | None
    """The UCX Python abstract syntax tree object"""

    failure: Failure | None
    """The failure during constructing the tree"""

    def __post_init__(self):
        if self.tree is None and self.failure is None:
            raise ValueError(f"Tree and failure should not be both `None`: {self}")
        if self.tree is not None and self.failure is not None:
            raise ValueError(f"Tree and failure should not be both given: {self}")

    @classmethod
    def from_source_code(cls, code: str) -> MaybeTree:
        """Normalize and parse the source code to get a `Tree` or parse `Failure`."""
        code = cls._normalize(code)
        return cls._maybe_parse(code)

    @classmethod
    def _maybe_parse(cls, code: str) -> MaybeTree:
        try:
            root = parse(code)
            tree = Tree(root)
            return cls(tree, None)
        except Exception as e:  # pylint: disable=broad-exception-caught
            # see https://github.com/databrickslabs/ucx/issues/2976
            failure = cls._failure_from_exception(code, e)
            return cls(None, failure)

    @staticmethod
    def _failure_from_exception(source_code: str, e: Exception) -> Failure:
        if isinstance(e, AstroidSyntaxError) and isinstance(e.error, SyntaxError):
            return Failure(
                code="python-parse-error",
                message=f"Failed to parse code due to invalid syntax: {source_code}",
                # Lines and columns are both 0-based: the first line is line 0.
                start_line=(e.error.lineno or 1) - 1,
                start_col=(e.error.offset or 1) - 1,
                end_line=(e.error.end_lineno or 2) - 1,
                end_col=(e.error.end_offset or 2) - 1,
            )
        body = "# Desired behaviour\n\nCannot parse the follow Python code\n\n``` python\n{source_code}\n```"
        new_issue_url = construct_new_issue_url(IssueType.BUG, "Python parse error", body)
        return Failure(
            code="python-parse-error",
            message=(
                f"Please report the following error as an issue on UCX GitHub: {new_issue_url}\n"
                f"Caught error `{type(e)} : {e}` while parsing code: {source_code}"
            ),
            # Lines and columns are both 0-based: the first line is line 0.
            start_line=0,
            start_col=0,
            end_line=1,
            end_col=1,
        )

    @classmethod
    def _normalize(cls, code: str) -> str:
        code = cls._normalize_indents(code)
        code = cls._convert_magic_lines_to_magic_commands(code)
        return code

    @staticmethod
    def _normalize_indents(python_code: str) -> str:
        lines = python_code.split("\n")
        for line in lines:
            # skip leading ws and comments
            if len(line.strip()) == 0 or line.startswith('#'):
                continue
            if not line.startswith(' '):
                # first line of code is correctly indented
                return python_code
            # first line of code is indented when it shouldn't
            prefix_count = len(line) - len(line.lstrip(' '))
            prefix_str = ' ' * prefix_count
            for i, line_to_fix in enumerate(lines):
                if line_to_fix.startswith(prefix_str):
                    lines[i] = line_to_fix[prefix_count:]
            return "\n".join(lines)
        return python_code

    @staticmethod
    def _convert_magic_lines_to_magic_commands(python_code: str) -> str:
        lines = python_code.split("\n")
        magic_markers = {"%", "!"}
        in_multi_line_comment = False
        pattern = re.compile('"""')
        for i, line in enumerate(lines):
            if len(line) == 0:
                continue
            if not in_multi_line_comment and line[0] in magic_markers:
                lines[i] = f"magic_command({line.encode()!r})"
                continue
            matches = re.findall(pattern, line)
            if len(matches) & 1:
                in_multi_line_comment = not in_multi_line_comment
        return "\n".join(lines)


class Tree:
    """The UCX Python abstract syntax tree object"""

    @classmethod
    def new_module(cls) -> Tree:
        node = Module("root")
        return cls(node)

    def __init__(self, node: NodeNG):
        self._node: NodeNG = node

    @property
    def node(self) -> NodeNG:
        return self._node

    @property
    def root(self) -> NodeNG:
        node = self._node
        while node.parent:
            node = node.parent
        return node

    def walk(self) -> Iterable[NodeNG]:
        yield from self._walk(self._node)

    def _walk(self, node: NodeNG) -> Iterable[NodeNG]:
        yield node
        for child in node.get_children():
            yield from self._walk(child)

    def locate(self, node_type: type[T], match_nodes: list[tuple[str, type]]) -> list[T]:
        visitor = MatchingVisitor(node_type, match_nodes)
        visitor.visit(self._node)
        return visitor.matched_nodes

    def first_statement(self) -> NodeNG | None:
        if isinstance(self._node, Module):
            if len(self._node.body) > 0:
                return self._node.body[0]
        return None

    def __repr__(self):
        truncate_after = 32
        code = repr(self._node)
        if len(code) > truncate_after:
            code = code[0:truncate_after] + "..."
        return f"<Tree: {code}>"

    def attach_child_tree(self, tree: Tree) -> None:
        """Attach a child tree.

        1. Make parent tree of the nodes in the child tree
        2. Extend parents globals with child globals

        Attaching a child tree is a **stateful** operation for the child tree. After attaching a child
        tree, the tree can be traversed starting from the child tree as a child knows its parent. However, the tree can
        not be traversed from the parent tree as that node object does not contain a list with children trees.
        """
        if not isinstance(tree.node, Module):
            raise NotImplementedError(f"Cannot attach child tree: {type(tree.node).__name__}")
        tree_module: Module = cast(Module, tree.node)
        self.attach_child_nodes(tree_module.body)
        self.extend_globals(tree_module.globals)

    def attach_child_nodes(self, nodes: list[NodeNG]) -> None:
        """Attach child nodes.

        Attaching a child tree is a **stateful** operation for the child tree. After attaching a child
        tree, the tree can be traversed starting from the child tree as a child knows its parent. However, the tree can
        not be traversed from the parent tree as that node object does not contain a list with children trees.
        """
        if not isinstance(self.node, Module):
            raise NotImplementedError(f"Cannot attach nodes to: {type(self.node).__name__}")
        self_module: Module = cast(Module, self.node)
        for node in nodes:
            node.parent = self_module

    def extend_globals(self, globs: dict[str, list[NodeNG]]) -> None:
        """Extend globals by extending the global values for each global key.

        Extending globals is a stateful operation for this `Tree` (`self`), similarly to extending a list.
        """
        if not isinstance(self.node, Module):
            raise NotImplementedError(f"Cannot extend globals to: {type(self.node).__name__}")
        self_module: Module = cast(Module, self.node)
        for global_key, global_values in globs.items():
            self_module.globals[global_key] = self_module.globals.get(global_key, []) + global_values

    def is_instance_of(self, class_name: str) -> bool:
        for inferred in self.node.inferred():
            if inferred is Uninferable:
                continue
            if not isinstance(inferred, (Const, Instance)):
                return False
            proxied = getattr(inferred, "_proxied", None)
            return isinstance(proxied, ClassDef) and proxied.name == class_name
        return False

    def is_from_module(self, module_name: str) -> bool:
        return self._is_from_module(module_name, set())

    def _is_from_module(self, module_name: str, visited: set[NodeNG]) -> bool:
        if self._node in visited:
            logger.debug(f"Recursion encountered while traversing node {self._node.as_string()}")
            return False
        visited.add(self._node)
        return self._node_is_from_module(module_name, visited)

    def _node_is_from_module(self, module_name: str, visited: set[NodeNG]) -> bool:
        if isinstance(self._node, Name):
            return self._name_is_from_module(module_name, visited)
        if isinstance(self._node, Call):
            return self._call_is_from_module(module_name, visited)
        if isinstance(self._node, Attribute):
            return self._attribute_is_from_module(module_name, visited)
        if isinstance(self._node, Const):
            return self._const_is_from_module(module_name, visited)
        return False

    def _name_is_from_module(self, module_name: str, visited: set[NodeNG]) -> bool:
        assert isinstance(self._node, Name)
        # if this is the call's root node, check it against the required module
        if self._node.name == module_name:
            return True
        root = self.root
        if not isinstance(root, Module):
            return False
        for value in root.globals.get(self._node.name, []):
            if not isinstance(value, AssignName) or not isinstance(value.parent, Assign):
                continue
            if _LocalTree(value.parent.value).is_from_module_visited(module_name, visited):
                return True
        return False

    def _call_is_from_module(self, module_name: str, visited: set[NodeNG]) -> bool:
        assert isinstance(self._node, Call)
        # walk up intermediate calls such as spark.range(...)
        return isinstance(self._node.func, Attribute) and _LocalTree(self._node.func.expr).is_from_module_visited(
            module_name, visited
        )

    def _attribute_is_from_module(self, module_name: str, visited: set[NodeNG]) -> bool:
        assert isinstance(self._node, Attribute)
        return _LocalTree(self._node.expr).is_from_module_visited(module_name, visited)

    def _const_is_from_module(self, module_name: str, visited: set[NodeNG]) -> bool:
        assert isinstance(self._node, Const)
        return _LocalTree(self._node.parent).is_from_module_visited(module_name, visited)

    def has_global(self, name: str) -> bool:
        if not isinstance(self.node, Module):
            return False
        self_module: Module = cast(Module, self.node)
        return self_module.globals.get(name, None) is not None

    def get_global(self, name: str) -> list[NodeNG]:
        if not self.has_global(name):
            return []
        return cast(Module, self.node).globals.get(name)

    def nodes_between(self, first_line: int, last_line: int) -> list[NodeNG]:
        if not isinstance(self.node, Module):
            raise NotImplementedError(f"Can't extract nodes from {type(self.node).__name__}")
        self_module: Module = cast(Module, self.node)
        nodes: list[NodeNG] = []
        for node in self_module.body:
            if node.lineno < first_line:
                continue
            if node.lineno > last_line:
                break
            nodes.append(node)
        return nodes

    def globals_between(self, first_line: int, last_line: int) -> dict[str, list[NodeNG]]:
        if not isinstance(self.node, Module):
            raise NotImplementedError(f"Can't extract globals from {type(self.node).__name__}")
        self_module: Module = cast(Module, self.node)
        globs: dict[str, list[NodeNG]] = {}
        for key, nodes in self_module.globals.items():
            nodes_in_scope: list[NodeNG] = []
            for node in nodes:
                if node.lineno < first_line or node.lineno > last_line:
                    continue
                nodes_in_scope.append(node)
            if len(nodes_in_scope) > 0:
                globs[key] = nodes_in_scope
        return globs

    def line_count(self) -> int:
        if not isinstance(self.node, Module):
            raise NotImplementedError(f"Can't count lines from {type(self.node).__name__}")
        self_module: Module = cast(Module, self.node)
        nodes_count = len(self_module.body)
        if nodes_count == 0:
            return 0
        return 1 + self_module.body[nodes_count - 1].lineno - self_module.body[0].lineno

    def renumber(self, start: int) -> Tree:
        assert start != 0
        if not isinstance(self.node, Module):
            raise NotImplementedError(f"Can't renumber {type(self.node).__name__}")
        root: Module = cast(Module, self.node)
        # for now renumber in place to avoid the complexity of rebuilding the tree with clones

        def renumber_node(node: NodeNG, offset: int) -> None:
            for child in node.get_children():
                renumber_node(child, offset + (child.lineno or 0) - (node.lineno or 0))
            if node.end_lineno:
                node.end_lineno = node.end_lineno + offset
                node.lineno = node.lineno + offset

        nodes = root.body if start > 0 else reversed(root.body)
        for node in nodes:
            offset = start - node.lineno
            renumber_node(node, offset)
            num_lines = 1 + (node.end_lineno - node.lineno if node.end_lineno else 0)
            start = start + num_lines if start > 0 else start - num_lines
        return self

    def is_builtin(self) -> bool:
        if isinstance(self._node, Name):
            name = self._node.name
            return name in dir(builtins) or name in sys.stdlib_module_names or name in sys.builtin_module_names
        if isinstance(self._node, Call):
            return Tree(self._node.func).is_builtin()
        if isinstance(self._node, Attribute):
            return Tree(self._node.expr).is_builtin()
        return False  # not supported yet


class _LocalTree(Tree):

    def is_from_module_visited(self, name: str, visited_nodes: set[NodeNG]) -> bool:
        return self._is_from_module(name, visited_nodes)


class TreeHelper(ABC):

    @classmethod
    def get_call_name(cls, call: Call) -> str:
        if not isinstance(call, Call):
            return ""
        func = call.func
        if isinstance(func, Name):
            return func.name
        if isinstance(func, Attribute):
            return func.attrname
        return ""  # not supported yet

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

    @classmethod
    def get_full_attribute_name(cls, node: Attribute) -> str | None:
        return cls._get_attribute_value(node)

    @classmethod
    def get_function_name(cls, node: Call) -> str | None:
        if not isinstance(node, Call):
            return None
        if isinstance(node.func, Attribute):
            return node.func.attrname
        if isinstance(node.func, Name):
            return node.func.name
        return None

    @classmethod
    def get_full_function_name(cls, node: Call) -> str | None:
        if not isinstance(node, Call):
            return None
        if isinstance(node.func, Attribute):
            return cls._get_attribute_value(node.func)
        if isinstance(node.func, Name):
            return node.func.name
        return None

    @classmethod
    def _get_attribute_value(cls, node: Attribute) -> str | None:
        if isinstance(node.expr, Name):
            return node.expr.name + '.' + node.attrname
        if isinstance(node.expr, Attribute):
            parent = cls._get_attribute_value(node.expr)
            return node.attrname if parent is None else parent + '.' + node.attrname
        if isinstance(node.expr, Call):
            name = cls.get_full_function_name(node.expr)
            return node.attrname if name is None else name + '.' + node.attrname
        name = type(node.expr).__name__
        if name not in missing_handlers:
            missing_handlers.add(name)
            logger.debug(f"Missing handler for {name}")
        return None


class TreeVisitor:

    def visit(self, node: NodeNG) -> None:
        self._visit_specific(node)
        for child in node.get_children():
            self.visit(child)

    def _visit_specific(self, node: NodeNG) -> None:
        method_name = "visit_" + type(node).__name__.lower()
        method_slot = getattr(self, method_name, None)
        if callable(method_slot):
            method_slot(node)
            return
        self.visit_nodeng(node)

    def visit_nodeng(self, node: NodeNG) -> None:
        pass


class MatchingVisitor(TreeVisitor):

    def __init__(self, node_type: type, match_nodes: list[tuple[str, type]]):
        super()
        self._matched_nodes: list[NodeNG] = []
        self._node_type = node_type
        self._match_nodes = match_nodes
        self._imports: dict[str, list[NodeNG]] = {}

    @property
    def matched_nodes(self) -> list[NodeNG]:
        return self._matched_nodes

    def visit_assign(self, node: Assign) -> None:
        if self._node_type is not Assign:
            return
        self._matched_nodes.append(node)

    def visit_call(self, node: Call) -> None:
        if self._node_type is not Call:
            return
        try:
            if self._matches(node.func, 0):
                self._matched_nodes.append(node)
        except NotImplementedError as e:
            logger.warning(f"Missing implementation: {e.args[0]}")

    def visit_import(self, node: Import) -> None:
        if self._node_type is not Import:
            return
        self._matched_nodes.append(node)

    def visit_importfrom(self, node: ImportFrom) -> None:
        if self._node_type is not ImportFrom:
            return
        self._matched_nodes.append(node)

    def visit_joinedstr(self, node: JoinedStr) -> None:
        if self._node_type is not JoinedStr:
            return
        self._matched_nodes.append(node)

    def _matches(self, node: NodeNG, depth: int) -> bool:
        if depth >= len(self._match_nodes):
            return False
        if isinstance(node, Call):
            return self._matches(node.func, depth)
        name, match_node = self._match_nodes[depth]
        node = self._adjust_node_for_import_member(name, match_node, node)
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

    def _adjust_node_for_import_member(self, name: str, match_node: type, node: NodeNG) -> NodeNG:
        if isinstance(node, match_node):
            return node
        # if we're looking for an attribute, it might be a global name
        if match_node != Attribute or not isinstance(node, Name) or node.name != name:
            return node
        # in which case it could be an import member
        module = Tree(Tree(node).root)
        if not module.has_global(node.name):
            return node
        for import_from in module.get_global(node.name):
            if not isinstance(import_from, ImportFrom):
                continue
            parent = Name(
                name=import_from.modname,
                lineno=import_from.lineno,
                col_offset=import_from.col_offset,
                end_lineno=import_from.end_lineno,
                end_col_offset=import_from.end_col_offset,
                parent=import_from.parent,
            )
            resolved = Attribute(
                attrname=name,
                lineno=import_from.lineno,
                col_offset=import_from.col_offset,
                end_lineno=import_from.end_lineno,
                end_col_offset=import_from.end_col_offset,
                parent=parent,
            )
            resolved.postinit(parent)
            return resolved
        return node


class NodeBase(ABC):

    def __init__(self, node: NodeNG):
        self._node = node

    @property
    def node(self) -> NodeNG:
        return self._node

    def __repr__(self):
        return f"<{self.__class__.__name__}: {repr(self._node)}>"
