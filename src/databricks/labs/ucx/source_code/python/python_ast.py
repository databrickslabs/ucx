from __future__ import annotations

import builtins
import sys
from abc import ABC
import logging
import re
from collections.abc import Iterable
from typing import TypeVar, cast

from astroid import (  # type: ignore
    Assign,
    AssignName,
    Attribute,
    Call,
    ClassDef,
    Const,
    Expr,
    Import,
    ImportFrom,
    Instance,
    Module,
    Name,
    NodeNG,
    parse,
    Uninferable,
)

logger = logging.getLogger(__name__)
missing_handlers: set[str] = set()


T = TypeVar("T", bound=NodeNG)


class Tree:

    @staticmethod
    def parse(code: str):
        root = parse(code)
        return Tree(root)

    @classmethod
    def normalize_and_parse(cls, code: str):
        code = cls.normalize(code)
        root = parse(code)
        return Tree(root)

    @classmethod
    def normalize(cls, code: str):
        code = cls._normalize_indents(code)
        code = cls._convert_magic_lines_to_magic_commands(code)
        return code

    @classmethod
    def _normalize_indents(cls, python_code: str):
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

    @classmethod
    def _convert_magic_lines_to_magic_commands(cls, python_code: str):
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

    @classmethod
    def new_module(cls):
        node = Module("root")
        return Tree(node)

    def __init__(self, node: NodeNG):
        self._node: NodeNG = node

    @property
    def node(self):
        return self._node

    @property
    def root(self):
        node = self._node
        while node.parent:
            node = node.parent
        return node

    def walk(self) -> Iterable[NodeNG]:
        yield from self._walk(self._node)

    @classmethod
    def _walk(cls, node: NodeNG) -> Iterable[NodeNG]:
        yield node
        for child in node.get_children():
            yield from cls._walk(child)

    def locate(self, node_type: type[T], match_nodes: list[tuple[str, type]]) -> list[T]:
        visitor = MatchingVisitor(node_type, match_nodes)
        visitor.visit(self._node)
        return visitor.matched_nodes

    def first_statement(self):
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

    def append_tree(self, tree: Tree) -> Tree:
        """returns the appended tree, not the consolidated one!"""
        if not isinstance(tree.node, Module):
            raise NotImplementedError(f"Can't append tree from {type(tree.node).__name__}")
        tree_module: Module = cast(Module, tree.node)
        self.append_nodes(tree_module.body)
        self.append_globals(tree_module.globals)
        # the following may seem strange but it's actually ok to use the original module as tree root
        # because each node points to the correct parent (practically, the tree is now only a list of statements)
        return tree

    def append_globals(self, globs: dict[str, list[Expr]]) -> None:
        if not isinstance(self.node, Module):
            raise NotImplementedError(f"Can't append globals to {type(self.node).__name__}")
        self_module: Module = cast(Module, self.node)
        for name, values in globs.items():
            statements: list[Expr] = self_module.globals.get(name, None)
            if statements is None:
                self_module.globals[name] = list(values)  # clone the source list to avoid side-effects
                continue
            statements.extend(values)

    def append_nodes(self, nodes: list[NodeNG]) -> None:
        if not isinstance(self.node, Module):
            raise NotImplementedError(f"Can't append statements to {type(self.node).__name__}")
        self_module: Module = cast(Module, self.node)
        for node in nodes:
            node.parent = self_module
            self_module.body.append(node)

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
    def get_full_attribute_name(cls, node: Attribute) -> str:
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
    def _get_attribute_value(cls, node: Attribute):
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

    def visit(self, node: NodeNG):
        self._visit_specific(node)
        for child in node.get_children():
            self.visit(child)

    def _visit_specific(self, node: NodeNG):
        method_name = "visit_" + type(node).__name__.lower()
        method_slot = getattr(self, method_name, None)
        if callable(method_slot):
            method_slot(node)
            return
        self.visit_nodeng(node)

    def visit_nodeng(self, node: NodeNG):
        pass


class MatchingVisitor(TreeVisitor):

    def __init__(self, node_type: type, match_nodes: list[tuple[str, type]]):
        super()
        self._matched_nodes: list[NodeNG] = []
        self._node_type = node_type
        self._match_nodes = match_nodes

    @property
    def matched_nodes(self):
        return self._matched_nodes

    def visit_assign(self, node: Assign):
        if self._node_type is not Assign:
            return
        self._matched_nodes.append(node)

    def visit_call(self, node: Call):
        if self._node_type is not Call:
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
        if isinstance(node, Call):
            return self._matches(node.func, depth)
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


class NodeBase(ABC):

    def __init__(self, node: NodeNG):
        self._node = node

    @property
    def node(self):
        return self._node

    def __repr__(self):
        return f"<{self.__class__.__name__}: {repr(self._node)}>"
