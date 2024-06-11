from collections.abc import Iterator, Iterable
from typing import cast

import pytest
from astroid import Assign, Attribute, Call, Const, Expr, FormattedValue, JoinedStr, Name, NodeNG  # type: ignore

from databricks.labs.ucx.source_code.linters.python_ast import Tree


def test_extract_call_by_name():
    tree = Tree.parse("o.m1().m2().m3()")
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = Tree.extract_call_by_name(stmt.value, "m2")
    assert isinstance(act, Call)
    assert isinstance(act.func, Attribute)
    assert act.func.attrname == "m2"


def test_extract_call_by_name_none():
    tree = Tree.parse("o.m1().m2().m3()")
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = Tree.extract_call_by_name(stmt.value, "m5000")
    assert act is None


@pytest.mark.parametrize(
    "code, arg_index, arg_name, expected",
    [
        ("o.m1()", 1, "second", None),
        ("o.m1(3)", 1, "second", None),
        ("o.m1(first=3)", 1, "second", None),
        ("o.m1(4, 3)", None, None, None),
        ("o.m1(4, 3)", None, "second", None),
        ("o.m1(4, 3)", 1, "second", 3),
        ("o.m1(4, 3)", 1, None, 3),
        ("o.m1(first=4, second=3)", 1, "second", 3),
        ("o.m1(second=3, first=4)", 1, "second", 3),
        ("o.m1(second=3, first=4)", None, "second", 3),
        ("o.m1(second=3)", 1, "second", 3),
        ("o.m1(4, 3, 2)", 1, "second", 3),
    ],
)
def test_linter_gets_arg(code, arg_index, arg_name, expected):
    tree = Tree.parse(code)
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = Tree.get_arg(stmt.value, arg_index, arg_name)
    if expected is None:
        assert act is None
    else:
        assert isinstance(act, Const)
        assert act.value == expected


@pytest.mark.parametrize(
    "code, expected",
    [
        ("o.m1()", 0),
        ("o.m1(3)", 1),
        ("o.m1(first=3)", 1),
        ("o.m1(3, 3)", 2),
        ("o.m1(first=3, second=3)", 2),
        ("o.m1(3, second=3)", 2),
        ("o.m1(3, *b, **c, second=3)", 4),
    ],
)
def test_args_count(code, expected):
    tree = Tree.parse(code)
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = Tree.args_count(stmt.value)
    assert act == expected


def test_tree_walks_nodes_once():
    nodes = set()
    count = 0
    tree = Tree.parse("o.m1().m2().m3()")
    for node in tree.walk():
        nodes.add(node)
        count += 1
    assert len(nodes) == count


def test_infers_fstring_value():
    source = """
value = "abc"
fstring = f"Hello {value}!"
"""
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value) # value of fstring = ...
    unresolved, values = tree.infer_values()
    assert not unresolved
    joined = Tree.string_from_consts(values[0])
    assert joined == "Hello abc!"


def test_infers_fstring_values():
    source = """
values_1 = ["abc", "def"]
for value1 in values_1:
    values_2 = ["ghi", "jkl"]
    for value2 in values_2:
        fstring = f"Hello {value1}, {value2}!"
"""
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[2].value)  # value of fstring = ...
    unresolved, values = tree.infer_values()
    assert not unresolved
    strings: list[str] = []
    for value in values:
        joined = Tree.string_from_consts(value)
        strings.append(joined)
    assert strings == ["Hello abc, ghi!", "Hello abc, jkl!", "Hello def, ghi!", "Hello def, jkl!"]


def test_fails_to_infer_cascading_fstring_values():
    source = """
    value1 = "John"
    value2 = f"Hello {value1}"
    value3 = f"{value2}, how are you today?"
    """
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[2].value)  # value of value3 = ...
    unresolved, values = tree.infer_values()
    assert unresolved
    joined = Tree.string_from_consts(values[0])
    # assert joined == "Hello John, how are you today?"
