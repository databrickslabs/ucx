from __future__ import annotations

import ast
import pytest

from databricks.labs.ucx.source_code.python_ast_util import AstUtil


def get_statement_node(stmt: str) -> ast.stmt:
    node = ast.parse(stmt)
    return node.body[0]


@pytest.mark.parametrize("stmt", ["o.m1().m2().m3()", "a = o.m1().m2().m3()"])
def test_extract_callchain(migration_index, stmt):
    node = get_statement_node(stmt)
    act = AstUtil.extract_callchain(node)
    assert isinstance(act, ast.Call)
    assert isinstance(act.func, ast.Attribute)
    assert act.func.attr == "m3"


@pytest.mark.parametrize("stmt", ["a = 3", "[x+1 for x in xs]"])
def test_extract_callchain_none(migration_index, stmt):
    node = get_statement_node(stmt)
    act = AstUtil.extract_callchain(node)
    assert act is None


def test_extract_call_by_name(migration_index):
    callchain = get_statement_node("o.m1().m2().m3()").value
    act = AstUtil.extract_call_by_name(callchain, "m2")
    assert isinstance(act, ast.Call)
    assert isinstance(act.func, ast.Attribute)
    assert act.func.attr == "m2"


def test_extract_call_by_name_none(migration_index):
    callchain = get_statement_node("o.m1().m2().m3()").value
    act = AstUtil.extract_call_by_name(callchain, "m5000")
    assert act is None


@pytest.mark.parametrize(
    "param",
    [
        {"stmt": "o.m1()", "arg_index": 1, "arg_name": "second", "expected": None},
        {"stmt": "o.m1(3)", "arg_index": 1, "arg_name": "second", "expected": None},
        {"stmt": "o.m1(first=3)", "arg_index": 1, "arg_name": "second", "expected": None},
        {"stmt": "o.m1(4, 3)", "arg_index": None, "arg_name": None, "expected": None},
        {"stmt": "o.m1(4, 3)", "arg_index": None, "arg_name": "second", "expected": None},
        {"stmt": "o.m1(4, 3)", "arg_index": 1, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(4, 3)", "arg_index": 1, "arg_name": None, "expected": 3},
        {"stmt": "o.m1(first=4, second=3)", "arg_index": 1, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(second=3, first=4)", "arg_index": 1, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(second=3, first=4)", "arg_index": None, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(second=3)", "arg_index": 1, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(4, 3, 2)", "arg_index": 1, "arg_name": "second", "expected": 3},
    ],
)
def test_get_arg(migration_index, param):
    call = get_statement_node(param["stmt"]).value
    act = AstUtil.get_arg(call, param["arg_index"], param["arg_name"])
    if param["expected"] is None:
        assert act is None
    else:
        assert isinstance(act, ast.Constant)
        assert act.value == param["expected"]


@pytest.mark.parametrize(
    "param",
    [
        {"stmt": "o.m1()", "expected": 0},
        {"stmt": "o.m1(3)", "expected": 1},
        {"stmt": "o.m1(first=3)", "expected": 1},
        {"stmt": "o.m1(3, 3)", "expected": 2},
        {"stmt": "o.m1(first=3, second=3)", "expected": 2},
        {"stmt": "o.m1(3, second=3)", "expected": 2},
        {"stmt": "o.m1(3, *b, **c, second=3)", "expected": 4},
    ],
)
def test_args_count(migration_index, param):
    call = get_statement_node(param["stmt"]).value
    act = AstUtil.args_count(call)
    assert param["expected"] == act


@pytest.mark.parametrize(
    "param",
    [
        {"stmt": "a = x", "expected": False},
        {"stmt": "a = 3", "expected": False},
        {"stmt": "a = 'None'", "expected": False},
        {"stmt": "a = None", "expected": True},
    ],
)
def test_is_none(migration_index, param):
    val = get_statement_node(param["stmt"]).value
    act = AstUtil.is_none(val)
    assert param["expected"] == act
