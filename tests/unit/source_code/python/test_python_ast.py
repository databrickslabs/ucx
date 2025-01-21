import logging
from collections.abc import Iterable

import pytest
import astroid  # type: ignore
from astroid import Assign, AssignName, Attribute, Call, Const, Expr, JoinedStr, Module, Name, NodeNG  # type: ignore

from databricks.labs.ucx.source_code.base import (
    Advice,
    DirectFsAccess,
    DirectFsAccessNode,
    Failure,
    UsedTable,
    UsedTableNode,
)
from databricks.labs.ucx.source_code.python.python_ast import (
    DfsaPyCollector,
    PythonLinter,
    PythonSequentialLinter,
    TablePyCollector,
    Tree,
    TreeHelper,
)
from databricks.labs.ucx.source_code.python.python_infer import InferredValue


def test_extracts_root() -> None:
    maybe_tree = Tree.maybe_parse("o.m1().m2().m3()")
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    stmt = tree.first_statement()
    root = Tree(stmt).root
    assert root == tree.node
    assert repr(tree)  # for test coverage


def test_no_first_statement() -> None:
    maybe_tree = Tree.maybe_parse("")
    assert maybe_tree.tree is not None
    assert maybe_tree.tree.first_statement() is None


def test_extract_call_by_name() -> None:
    tree = Tree.maybe_parse("o.m1().m2().m3()")
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = TreeHelper.extract_call_by_name(stmt.value, "m2")
    assert isinstance(act, Call)
    assert isinstance(act.func, Attribute)
    assert act.func.attrname == "m2"


def test_extract_call_by_name_none() -> None:
    tree = Tree.maybe_parse("o.m1().m2().m3()")
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = TreeHelper.extract_call_by_name(stmt.value, "m5000")
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
def test_linter_gets_arg(code, arg_index, arg_name, expected) -> None:
    tree = Tree.maybe_parse(code)
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = TreeHelper.get_arg(stmt.value, arg_index, arg_name)
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
def test_args_count(code, expected) -> None:
    tree = Tree.maybe_parse(code)
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = TreeHelper.args_count(stmt.value)
    assert act == expected


def test_tree_walks_nodes_once() -> None:
    nodes = set()
    count = 0
    tree = Tree.maybe_parse("o.m1().m2().m3()")
    for node in tree.walk():
        nodes.add(node)
        count += 1
    assert len(nodes) == count


def test_parses_incorrectly_indented_code() -> None:
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
    maybe_tree = Tree.maybe_parse(source)
    assert maybe_tree.failure is not None
    maybe_tree = Tree.maybe_normalized_parse(source)
    assert maybe_tree.failure is None


def test_ignores_magic_marker_in_multiline_comment() -> None:
    source = """message_unformatted = u\"""
%s is only supported in Python %s and above.\"""
name="name"
version="version"
formatted=message_unformatted % (name, version)
"""
    Tree.maybe_normalized_parse(source)
    assert True


def test_tree_attach_child_tree_infers_value() -> None:
    inferred_string = "Hello John!"
    parent_source, child_source = "a = 'John'", 'b = f"Hello {a}!"'
    parent_maybe_tree = Tree.maybe_normalized_parse(parent_source)
    child_maybe_tree = Tree.maybe_normalized_parse(child_source)

    assert parent_maybe_tree.tree is not None, parent_maybe_tree.failure
    assert child_maybe_tree.tree is not None, child_maybe_tree.failure

    parent_maybe_tree.tree.attach_child_tree(child_maybe_tree.tree)

    nodes = child_maybe_tree.tree.locate(Assign, [])
    tree = Tree(nodes[0].value)  # Starting from child, we are looking for the first assign
    strings = [value.as_string() for value in InferredValue.infer_from_node(tree.node)]
    assert strings == [inferred_string]


def test_tree_attach_child_tree_infers_value_from_grand_parent() -> None:
    inferred_string = "Hello John!"
    grand_parent_source, parent_source, child_source = "name = 'John'", "greeting = 'Hello'", 'say = f"Hello {name}!"'
    grand_parent_maybe_tree = Tree.maybe_normalized_parse(grand_parent_source)
    parent_maybe_tree = Tree.maybe_normalized_parse(parent_source)
    child_maybe_tree = Tree.maybe_normalized_parse(child_source)

    assert grand_parent_maybe_tree.tree is not None, grand_parent_maybe_tree.failure
    assert parent_maybe_tree.tree is not None, parent_maybe_tree.failure
    assert child_maybe_tree.tree is not None, child_maybe_tree.failure

    grand_parent_maybe_tree.tree.attach_child_tree(parent_maybe_tree.tree)
    parent_maybe_tree.tree.attach_child_tree(child_maybe_tree.tree)

    nodes = child_maybe_tree.tree.locate(Assign, [])
    tree = Tree(nodes[0].value)  # Starting from child, we are looking for the first assign
    strings = [value.as_string() for value in InferredValue.infer_from_node(tree.node)]
    assert strings == [inferred_string]


def test_tree_attach_parent_with_child_tree_infers_value() -> None:
    inferred_string = "Hello John!"
    parent_source, child_a_source, child_b_source = "name = 'John'", "greeting = 'Hello'", 'say = f"{greeting} {name}!"'
    parent_maybe_tree = Tree.maybe_normalized_parse(parent_source)
    child_a_maybe_tree = Tree.maybe_normalized_parse(child_a_source)
    child_b_maybe_tree = Tree.maybe_normalized_parse(child_b_source)

    assert parent_maybe_tree.tree is not None, parent_maybe_tree.failure
    assert child_a_maybe_tree.tree is not None, child_a_maybe_tree.failure
    assert child_b_maybe_tree.tree is not None, child_b_maybe_tree.failure

    parent_maybe_tree.tree.attach_child_tree(child_a_maybe_tree.tree)
    parent_maybe_tree.tree.attach_child_tree(child_b_maybe_tree.tree)

    nodes = child_b_maybe_tree.tree.locate(Assign, [])
    tree = Tree(nodes[0].value)  # Starting from child, we are looking for the first assign
    strings = [value.as_string() for value in InferredValue.infer_from_node(tree.node)]
    assert strings == [inferred_string]


def test_tree_attach_child_tree_with_notebook_using_variable_from_other_notebook() -> None:
    """Simulating a notebook where it uses a variable from another notebook."""
    inferred_string = "catalog.schema.table"
    child_source = "table_name = 'schema.table'"
    parent_cell_1_source = "%run ./child"
    parent_cell_2_source = "spark.table(f'catalog.{table_name}')"
    child_maybe_tree = Tree.maybe_normalized_parse(child_source)
    parent_cell_1_maybe_tree = Tree.maybe_normalized_parse(parent_cell_1_source)
    parent_cell_2_maybe_tree = Tree.maybe_normalized_parse(parent_cell_2_source)

    assert child_maybe_tree.tree is not None, child_maybe_tree.failure
    assert parent_cell_1_maybe_tree.tree is not None, parent_cell_1_maybe_tree.failure
    assert parent_cell_2_maybe_tree.tree is not None, parent_cell_2_maybe_tree.failure

    parent_cell_1_maybe_tree.tree.attach_child_tree(child_maybe_tree.tree)
    # Subsequent notebook cell is child of previous cell
    parent_cell_1_maybe_tree.tree.attach_child_tree(parent_cell_2_maybe_tree.tree)

    nodes = parent_cell_2_maybe_tree.tree.locate(JoinedStr, [])
    strings = [value.as_string() for value in InferredValue.infer_from_node(nodes[0])]
    assert strings == [inferred_string]


def test_is_from_module() -> None:
    source = """
df = spark.read.csv("hi")
df.write.format("delta").saveAsTable("old.things")
"""
    maybe_tree = Tree.maybe_normalized_parse(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    save_call = tree.locate(
        Call, [("saveAsTable", Attribute), ("format", Attribute), ("write", Attribute), ("df", Name)]
    )[0]
    assert Tree(save_call).is_from_module("spark")


def test_locates_member_import() -> None:
    source = """
from importlib import import_module
module = import_module("xyz")
"""
    maybe_tree = Tree.maybe_normalized_parse(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    import_calls = tree.locate(Call, [("import_module", Attribute), ("importlib", Name)])
    assert import_calls


@pytest.mark.parametrize("source, name, class_name", [("a = 123", "a", "int")])
def test_is_instance_of(source, name, class_name) -> None:
    maybe_tree = Tree.maybe_normalized_parse(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    assert isinstance(tree.node, Module)
    module = tree.node
    var = module.globals.get(name, None)
    assert isinstance(var, list) and len(var) > 0
    assert Tree(var[0]).is_instance_of(class_name)


def test_tree_attach_child_tree_propagates_module_reference() -> None:
    """The spark module should propagate from the parent tree."""
    source_1 = "df = spark.read.csv('hi')"
    source_2 = "df = df.withColumn(stuff)"
    source_3 = "df = df.withColumn(stuff2)"
    first_line_maybe_tree = Tree.maybe_normalized_parse(source_1)
    second_line_maybe_tree = Tree.maybe_normalized_parse(source_2)
    third_line_maybe_tree = Tree.maybe_normalized_parse(source_3)

    assert first_line_maybe_tree.tree, first_line_maybe_tree.failure
    assert second_line_maybe_tree.tree, second_line_maybe_tree.failure
    assert third_line_maybe_tree.tree, third_line_maybe_tree.failure

    first_line_maybe_tree.tree.attach_child_tree(second_line_maybe_tree.tree)
    first_line_maybe_tree.tree.attach_child_tree(third_line_maybe_tree.tree)

    assign = third_line_maybe_tree.tree.locate(Assign, [])[0]
    assert Tree(assign.value).is_from_module("spark")


def test_renumbers_positively() -> None:
    source = """df = spark.read.csv("hi")
df.write.format("delta").saveAsTable("old.things")
"""
    maybe_tree = Tree.maybe_normalized_parse(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = list(tree.node.get_children())
    assert len(nodes) == 2
    assert nodes[0].lineno == 1
    assert nodes[1].lineno == 2
    tree = tree.renumber(5)
    nodes = list(tree.node.get_children())
    assert len(nodes) == 2
    assert nodes[0].lineno == 5
    assert nodes[1].lineno == 6


def test_renumbers_negatively() -> None:
    source = """df = spark.read.csv("hi")
df.write.format("delta").saveAsTable("old.things")
"""
    maybe_tree = Tree.maybe_normalized_parse(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = list(tree.node.get_children())
    assert len(nodes) == 2
    assert nodes[0].lineno == 1
    assert nodes[1].lineno == 2
    tree = tree.renumber(-1)
    nodes = list(tree.node.get_children())
    assert len(nodes) == 2
    assert nodes[0].lineno == -2
    assert nodes[1].lineno == -1


@pytest.mark.parametrize(
    "source, line_count",
    [
        ("""df = spark.read.csv("hi")""", 1),
        ("""# comment\ndf = spark.read.csv("hi")\n# comment""", 1),
        ("""df = spark.read.csv("hi")\ndf.write.format("delta").saveAsTable("old.things")""", 2),
        ("""df = spark.read.csv("hi")\n# comment\ndf.write.format("delta").saveAsTable("old.things")""", 3),
    ],
)
def test_counts_lines(source: str, line_count: int) -> None:
    maybe_tree = Tree.maybe_normalized_parse(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    assert tree.line_count() == line_count


@pytest.mark.parametrize(
    "source, name, is_builtin",
    [
        ("x = open()", "open", True),
        ("import datetime; x = datetime.datetime.now()", "now", True),
        ("import stuff; x = stuff()", "stuff", False),
        (
            """def stuff():
  pass
x = stuff()""",
            "stuff",
            False,
        ),
    ],
)
def test_is_builtin(source, name, is_builtin) -> None:
    maybe_tree = Tree.maybe_normalized_parse(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = list(tree.node.get_children())
    for node in nodes:
        if isinstance(node, Assign):
            call = node.value
            assert isinstance(call, Call)
            func_name = TreeHelper.get_call_name(call)
            assert func_name == name
            assert Tree(call).is_builtin() == is_builtin
            return
    assert False  # could not locate call


def test_tree_attach_child_nodes_sets_parent() -> None:
    node = astroid.extract_node("b = a + 2")
    maybe_tree = Tree.maybe_normalized_parse("a = 1")
    assert maybe_tree.tree, maybe_tree.failure

    maybe_tree.tree.attach_child_nodes([node])

    assert node.parent == maybe_tree.tree.node


def test_tree_extend_globals_adds_assign_name_to_tree() -> None:
    maybe_tree = Tree.maybe_normalized_parse("a = 1")
    assert maybe_tree.tree, maybe_tree.failure

    node = astroid.extract_node("b = a + 2")
    assign_name = next(node.get_children())
    assert isinstance(assign_name, AssignName)

    maybe_tree.tree.extend_globals({"b": [assign_name]})

    assert isinstance(maybe_tree.tree.node, Module)
    assert maybe_tree.tree.node.globals.get("b") == [assign_name]


def test_tree_attach_child_tree_appends_globals_to_parent_tree() -> None:
    parent_tree = Tree.maybe_normalized_parse("a = 1")
    child_tree = Tree.maybe_normalized_parse("b = a + 2")

    assert parent_tree.tree, parent_tree.failure
    assert child_tree.tree, child_tree.failure

    parent_tree.tree.attach_child_tree(child_tree.tree)

    assert set(parent_tree.tree.node.globals.keys()) == {"a", "b"}
    assert set(child_tree.tree.node.globals.keys()) == {"b"}


def test_first_statement_is_none() -> None:
    node = Const("xyz")
    assert not Tree(node).first_statement()


def test_repr_is_truncated() -> None:
    assert len(repr(Tree(Const("xyz")))) <= (32 + len("...") + len("<Tree: >"))


def test_tree_attach_child_tree_raises_not_implemented_error_for_constant_node() -> None:
    with pytest.raises(NotImplementedError, match="Cannot attach child tree: .*"):
        Tree(Const("xyz")).attach_child_tree(Tree(Const("xyz")))


def test_tree_attach_child_nodes_raises_not_implemented_error_for_constant_node() -> None:
    with pytest.raises(NotImplementedError, match="Cannot attach nodes to: .*"):
        Tree(Const("xyz")).attach_child_nodes([])


def test_extend_globals_raises_not_implemented_error_for_constant_node() -> None:
    with pytest.raises(NotImplementedError, match="Cannot extend globals to: .*"):
        Tree(Const("xyz")).extend_globals({})


def test_nodes_between_fails() -> None:
    with pytest.raises(NotImplementedError):
        Tree(Const("xyz")).nodes_between(0, 100)


def test_has_global_fails() -> None:
    assert not Tree.new_module().has_global("xyz")


def test_globals_between_fails() -> None:
    with pytest.raises(NotImplementedError):
        Tree(Const("xyz")).line_count()


def test_line_count_fails() -> None:
    with pytest.raises(NotImplementedError):
        Tree(Const("xyz")).globals_between(0, 100)


def test_renumber_fails() -> None:
    with pytest.raises(NotImplementedError):
        Tree(Const("xyz")).renumber(100)


def test_const_is_not_from_module() -> None:
    assert not Tree(Const("xyz")).is_from_module("spark")


class DummyPythonLinter(PythonLinter):
    """Dummy python linter yielding dummy advices for testing purpose."""

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        yield Advice("dummy", "dummy advice", 0, 0, 0, 0)
        yield Advice("dummy", "dummy advice", 1, 1, 1, 1)


def test_dummy_python_linter_lint_lints_tree() -> None:
    linter = DummyPythonLinter()
    advices = list(linter.lint("print(1)"))
    assert advices == [Advice("dummy", "dummy advice", 0, 0, 0, 0), Advice("dummy", "dummy advice", 1, 1, 1, 1)]


def test_dummy_python_linter_lint_yields_failure_due_to_parse_error() -> None:
    linter = DummyPythonLinter()
    advices = list(linter.lint("print(1"))  # Closing parenthesis is missing on purpose
    assert advices == [
        Failure(
            code="python-parse-error",
            message="Failed to parse code due to invalid syntax: print(1",
            start_line=0,
            start_col=5,
            end_line=0,
            end_col=1,
        )
    ]


def test_python_sequential_linter_lint_lints_tree() -> None:
    linter = PythonSequentialLinter([DummyPythonLinter()], [], [])
    advices = list(linter.lint("print(1)"))
    assert advices == [Advice("dummy", "dummy advice", 0, 0, 0, 0), Advice("dummy", "dummy advice", 1, 1, 1, 1)]


class BodyNodesGlobalsLinter(PythonLinter):
    """Get the node globals"""

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        globs = set()
        if isinstance(tree.node, Module):
            for node in tree.node.body:
                globs |= set(node.parent.globals.keys())
        yield Advice("globals", ",".join(sorted(globs)), 0, 0, 0, 0)


def test_python_sequential_linter_lint_has_no_globals() -> None:
    linter = PythonSequentialLinter([BodyNodesGlobalsLinter()], [], [])
    advices = list(linter.lint("print(1)"))
    assert advices == [Advice("globals", "", 0, 0, 0, 0)]


def test_python_sequential_linter_lint_has_one_global() -> None:
    linter = PythonSequentialLinter([BodyNodesGlobalsLinter()], [], [])
    advices = list(linter.lint("a = 1"))
    assert advices == [Advice("globals", "a", 0, 0, 0, 0)]


def test_python_sequential_linter_lint_has_two_globals() -> None:
    linter = PythonSequentialLinter([BodyNodesGlobalsLinter()], [], [])
    advices = list(linter.lint("a = 1;b = 2"))
    assert advices == [Advice("globals", "a,b", 0, 0, 0, 0)]


def test_python_sequential_linter_lint_is_stateless() -> None:
    """Globals from previous lint calls should not be part of later calls"""
    linter = PythonSequentialLinter([BodyNodesGlobalsLinter()], [], [])
    list(linter.lint("a = 1"))
    advices = list(linter.lint("b = 2"))
    assert advices == [Advice("globals", "b", 0, 0, 0, 0)]


class DummyDfsaPyCollector(DfsaPyCollector):
    """Dummy direct filesystem access collector yielding dummy advices for testing purpose."""

    def collect_dfsas_from_tree(self, tree: Tree) -> Iterable[DirectFsAccessNode]:
        dfsa = DirectFsAccess(path="test.py")
        node = NodeNG(0, 0, None, end_lineno=0, end_col_offset=0)
        yield DirectFsAccessNode(dfsa, node)


def test_dummy_dfsa_python_collector_collect_dfsas() -> None:
    linter = DummyDfsaPyCollector()
    dfsas = list(linter.collect_dfsas("print(1)"))
    assert dfsas == [DirectFsAccess(path="test.py")]


def test_dummy_dfsa_python_collector_collect_dfsas_warns_failure_due_to_parse_error(caplog) -> None:
    linter = DummyDfsaPyCollector()
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.python.python_ast"):
        dfsas = list(linter.collect_dfsas("print(1"))  # Closing parenthesis is missing on purpose
    assert not dfsas
    assert "Failed to parse code due to invalid syntax: print(1" in caplog.messages


def test_python_sequential_linter_collect_dfsas() -> None:
    linter = PythonSequentialLinter([], [DummyDfsaPyCollector()], [])
    dfsas = list(linter.collect_dfsas("print(1)"))
    assert dfsas == [DirectFsAccess(path="test.py")]


class DummyTablePyCollector(TablePyCollector):
    """Dummy table collector yielding dummy used tables for testing purpose."""

    def collect_tables_from_tree(self, tree: Tree) -> Iterable[UsedTableNode]:
        dfsa = UsedTable(schema_name="test", table_name="test")
        node = NodeNG(0, 0, None, end_lineno=0, end_col_offset=0)
        yield UsedTableNode(dfsa, node)


def test_dummy_table_python_collector_collect_tables() -> None:
    linter = DummyTablePyCollector()
    used_tables = list(linter.collect_tables("print(1)"))
    assert used_tables == [UsedTable(schema_name="test", table_name="test")]


def test_dummy_table_python_collector_collect_tables_warns_failure_due_to_parse_error(caplog) -> None:
    linter = DummyTablePyCollector()
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.python.python_ast"):
        used_tables = list(linter.collect_tables("print(1"))  # Closing parenthesis is missing on purpose
    assert not used_tables
    assert "Failed to parse code due to invalid syntax: print(1" in caplog.messages


def test_python_sequential_linter_collect_tables() -> None:
    linter = PythonSequentialLinter([], [], [DummyTablePyCollector()])
    used_tables = list(linter.collect_tables("print(1)"))
    assert used_tables == [UsedTable(schema_name="test", table_name="test")]
