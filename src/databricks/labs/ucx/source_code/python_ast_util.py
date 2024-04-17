from __future__ import annotations

import ast
from dataclasses import dataclass


@dataclass
class Span:
    """ Represents a (possibly multiline) source code span. """
    start_line: int
    start_col: int
    end_line: int
    end_col: int


class AstUtil:
    @staticmethod
    def extract_callchain(node: ast.AST) -> ast.Call | None:
        """ If 'node' is an assignment or expression, extract its full call-chain (if it has one) """
        call = None
        if isinstance(node, ast.Assign):
            call = node.value
        elif isinstance(node, ast.Expr):
            call = node.value
        if not isinstance(call, ast.Call):
            call = None
        return call

    @staticmethod
    def extract_call_by_name(node: ast.Call, name: str) -> ast.Call | None:
        """ Given a call-chain, extract its sub-call by method name (if it has one) """
        while True:
            if not isinstance(node, ast.Call):
                return None

            func = node.func
            if not isinstance(func, ast.Attribute):
                return None
            if func.attr == name:
                return node
            node = func.value

    @staticmethod
    def args_count(node: ast.Call) -> int:
        """ Count the number of arguments (positionals + keywords) """
        return len(node.args) + len(node.keywords)

    @staticmethod
    def get_arg(
            node: ast.Call,
            arg_index: int | None,
            arg_name: str | None,
    ) -> ast.expr | None:
        """ Extract the call argument identified by an optional position or name (if it has one) """
        if arg_index is not None and len(node.args) > arg_index:
            return node.args[arg_index]
        if arg_name is not None:
            arg = [kw.value for kw in node.keywords if kw.arg == arg_name]
            if len(arg) == 1:
                return arg[0]
        return None

    @staticmethod
    def is_none(node: ast.expr) -> bool:
        """ Check if the given AST expression is the None constant """
        if not isinstance(node, ast.Constant):
            return False
        return node.value is None

