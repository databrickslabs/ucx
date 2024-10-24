import pytest
from astroid import Assign, Attribute, Call, Const, Expr, Module, Name  # type: ignore

from databricks.labs.ucx.source_code.python.python_ast import Tree, TreeHelper
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


def test_appends_statements() -> None:
    source_1 = "a = 'John'"
    maybe_tree_1 = Tree.maybe_normalized_parse(source_1)
    assert maybe_tree_1.tree is not None, maybe_tree_1.failure
    tree_1 = maybe_tree_1.tree
    source_2 = 'b = f"Hello {a}!"'
    maybe_tree_2 = Tree.maybe_normalized_parse(source_2)
    assert maybe_tree_2.tree is not None, maybe_tree_2.failure
    tree_2 = maybe_tree_2.tree
    tree_3 = tree_1.append_tree(tree_2)
    nodes = tree_3.locate(Assign, [])
    tree = Tree(nodes[0].value)  # tree_3 only contains tree_2 statements
    values = list(InferredValue.infer_from_node(tree.node))
    strings = list(value.as_string() for value in values)
    assert strings == ["Hello John!"]


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


def test_supports_recursive_refs_when_checking_module() -> None:
    source_1 = """
    df = spark.read.csv("hi")
    """
    source_2 = """
    df = df.withColumn(stuff)
    """
    source_3 = """
    df = df.withColumn(stuff2)
    """
    maybe_tree = Tree.maybe_normalized_parse(source_1)
    assert maybe_tree.tree is not None, maybe_tree.failure
    main_tree = maybe_tree.tree
    maybe_tree_2 = Tree.maybe_normalized_parse(source_2)
    assert maybe_tree_2.tree is not None, maybe_tree_2.failure
    tree_2 = maybe_tree_2.tree
    main_tree.append_tree(tree_2)
    maybe_tree_3 = Tree.maybe_normalized_parse(source_3)
    assert maybe_tree_3.tree is not None, maybe_tree_3.failure
    tree_3 = maybe_tree_3.tree
    main_tree.append_tree(tree_3)
    assign = tree_3.locate(Assign, [])[0]
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


def test_first_statement_is_none() -> None:
    node = Const("xyz")
    assert not Tree(node).first_statement()


def test_repr_is_truncated() -> None:
    assert len(repr(Tree(Const("xyz")))) <= (32 + len("...") + len("<Tree: >"))


def test_append_tree_fails() -> None:
    with pytest.raises(NotImplementedError):
        Tree(Const("xyz")).append_tree(Tree(Const("xyz")))


def test_append_node_fails() -> None:
    with pytest.raises(NotImplementedError):
        Tree(Const("xyz")).append_nodes([])


def test_nodes_between_fails() -> None:
    with pytest.raises(NotImplementedError):
        Tree(Const("xyz")).nodes_between(0, 100)


def test_has_global_fails() -> None:
    assert not Tree.new_module().has_global("xyz")


def test_append_globals_fails() -> None:
    with pytest.raises(NotImplementedError):
        Tree(Const("xyz")).append_globals({})


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
