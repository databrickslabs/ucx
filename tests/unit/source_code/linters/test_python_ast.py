
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


@pytest.mark.parametrize(
    "code, expected",
    [
        (
            """
name = "xyz"
dbutils.notebook.run(name)
""",
            "xyz",
        )
    ],
)
def test_infers_string_variable_value(code, expected):
    tree = Tree.parse(code)
    calls = DbutilsLinter.list_dbutils_notebook_run_calls(tree)
    actual = list(call.get_notebook_path() for call in calls)
    assert [expected] == actual


def test_tree_walker_walks_nodes_once():
    nodes = set()
    count = 0
    tree = Tree.parse("o.m1().m2().m3()")
    for node in tree.walk():
        nodes.add(node)
        count += 1
    assert len(nodes) == count
