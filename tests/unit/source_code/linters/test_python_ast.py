import pytest
from astroid import Assign, AstroidSyntaxError, Attribute, Call, Const, Expr  # type: ignore

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.python_ast import Tree


def test_extracts_root():
    tree = Tree.parse("o.m1().m2().m3()")
    stmt = tree.first_statement()
    root = Tree(stmt).root
    assert root == tree.node
    assert repr(tree)  # for test coverage


def test_no_first_statement():
    tree = Tree.parse("")
    assert not tree.first_statement()


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


def test_infers_empty_list():
    tree = Tree.parse("a=[]")
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[0].value)
    values = list(tree.infer_values())
    assert not values


def test_infers_empty_tuple():
    tree = Tree.parse("a=tuple()")
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[0].value)
    values = list(tree.infer_values())
    assert not values


def test_infers_empty_set():
    tree = Tree.parse("a={}")
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[0].value)
    values = list(tree.infer_values())
    assert not values


def test_infers_fstring_value():
    source = """
value = "abc"
fstring = f"Hello {value}!"
"""
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of fstring = ...
    values = list(tree.infer_values())
    assert all(value.is_inferred() for value in values)
    strings = list(value.as_string() for value in values)
    assert strings == ["Hello abc!"]


def test_infers_string_format_value():
    source = """
value = "abc"
fstring = "Hello {0}!".format(value)
"""
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of fstring = ...
    values = list(tree.infer_values())
    assert all(value.is_inferred() for value in values)
    strings = list(value.as_string() for value in values)
    assert strings == ["Hello abc!"]


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
    values = list(tree.infer_values())
    assert all(value.is_inferred() for value in values)
    strings = list(value.as_string() for value in values)
    assert strings == ["Hello abc, ghi!", "Hello abc, jkl!", "Hello def, ghi!", "Hello def, jkl!"]


def test_fails_to_infer_cascading_fstring_values():
    # The purpose of this test is to detect a change in astroid support for f-strings
    source = """
value1 = "John"
value2 = f"Hello {value1}"
value3 = f"{value2}, how are you today?"
"""
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[2].value)  # value of value3 = ...
    values = list(tree.infer_values())
    # for now, we simply check failure to infer!
    assert any(not value.is_inferred() for value in values)
    # the expected value would be ["Hello John, how are you today?"]


def test_infers_externally_defined_value():
    state = CurrentSessionState()
    state.named_parameters = {"my-widget": "my-value"}
    source = """
name = "my-widget"
value = dbutils.widgets.get(name)
"""
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of value = ...
    values = list(tree.infer_values(state))
    strings = list(value.as_string() for value in values)
    assert strings == ["my-value"]


def test_infers_externally_defined_values():
    state = CurrentSessionState()
    state.named_parameters = {"my-widget-1": "my-value-1", "my-widget-2": "my-value-2"}
    source = """
for name in ["my-widget-1", "my-widget-2"]:
    value = dbutils.widgets.get(name)
"""
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[0].value)  # value of value = ...
    values = list(tree.infer_values(state))
    strings = list(value.as_string() for value in values)
    assert strings == ["my-value-1", "my-value-2"]


def test_fails_to_infer_missing_externally_defined_value():
    state = CurrentSessionState()
    state.named_parameters = {"my-widget-1": "my-value-1", "my-widget-2": "my-value-2"}
    source = """
name = "my-widget"
value = dbutils.widgets.get(name)
"""
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of value = ...
    values = tree.infer_values(state)
    assert all(not value.is_inferred() for value in values)


def test_survives_absence_of_externally_defined_values():
    source = """
    name = "my-widget"
    value = dbutils.widgets.get(name)
    """
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of value = ...
    values = tree.infer_values(CurrentSessionState())
    assert all(not value.is_inferred() for value in values)


def test_infers_externally_defined_value_set():
    state = CurrentSessionState()
    state.named_parameters = {"my-widget": "my-value"}
    source = """
values = dbutils.widgets.getAll()
name = "my-widget"
value = values[name]
"""
    tree = Tree.parse(source)
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[2].value)  # value of value = ...
    values = list(tree.infer_values(state))
    strings = list(value.as_string() for value in values)
    assert strings == ["my-value"]


def test_parses_incorrectly_indented_code():
    source = """# DBTITLE 1,Get Sales Data for Analysis
 sales = (
   spark
      .table('retail_sales')
      .join( # limit data to CY 2021 and 2022
        spark.table('date').select('dateKey','date','year').filter('year between 2021 and 2022'),
        on='dateKey'
        )
      .join( # get product fields needed for analysis
        spark.table('product').select('productKey','brandValue','packSizeValueUS'),
        on='productKey'
        )
      .join( # get brand fields needed for analysis
        spark.table('brand_name_mapping').select('brandValue','brandName'),
        on='brandValue'
        )
  )
"""
    # ensure it would fail if not normalized
    with pytest.raises(AstroidSyntaxError):
        Tree.parse(source)
    Tree.normalize_and_parse(source)
    assert True


def test_ignores_magic_marker_in_multiline_comment():
    source = """message_unformatted = u\"""
%s is only supported in Python %s and above.\"""
name="name"
version="version"
formatted=message_unformatted % (name, version)
"""
    Tree.normalize_and_parse(source)
    assert True


def test_appends_statements():
    source_1 = "a = 'John'"
    tree_1 = Tree.normalize_and_parse(source_1)
    source_2 = 'b = f"Hello {a}!"'
    tree_2 = Tree.normalize_and_parse(source_2)
    tree_3 = tree_1.append_statements(tree_2)
    nodes = tree_3.locate(Assign, [])
    tree = Tree(nodes[0].value)  # tree_3 only contains tree_2 statements
    values = list(tree.infer_values(CurrentSessionState()))
    strings = list(value.as_string() for value in values)
    assert strings == ["Hello John!"]
