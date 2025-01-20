from astroid import Assign  # type: ignore

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.python.python_ast import MaybeTree, Tree
from databricks.labs.ucx.source_code.python.python_infer import InferredValue


def test_infers_empty_list() -> None:
    maybe_tree = MaybeTree.from_source_code("a=[]")
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[0].value)
    values = list(InferredValue.infer_from_node(tree.node))
    assert not values


def test_infers_empty_tuple() -> None:
    maybe_tree = MaybeTree.from_source_code("a=tuple()")
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[0].value)
    values = list(InferredValue.infer_from_node(tree.node))
    assert not values


def test_infers_empty_set() -> None:
    maybe_tree = MaybeTree.from_source_code("a={}")
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[0].value)
    values = list(InferredValue.infer_from_node(tree.node))
    assert not values


def test_infers_fstring_value() -> None:
    source = """
value = "abc"
fstring = f"Hello {value}!"
"""
    maybe_tree = MaybeTree.from_source_code(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of fstring = ...
    values = list(InferredValue.infer_from_node(tree.node))
    assert all(value.is_inferred() for value in values)
    strings = list(value.as_string() for value in values)
    assert strings == ["Hello abc!"]


def test_infers_fstring_dict_value() -> None:
    source = """
value = { "abc": 123 }
fstring = f"Hello {value['abc']}!"
"""
    maybe_tree = MaybeTree.from_source_code(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of fstring = ...
    values = list(InferredValue.infer_from_node(tree.node))
    assert all(value.is_inferred() for value in values)
    strings = list(value.as_string() for value in values)
    assert strings == ["Hello 123!"]


def test_infers_string_format_value() -> None:
    source = """
value = "abc"
fstring = "Hello {0}!".format(value)
"""
    maybe_tree = MaybeTree.from_source_code(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of fstring = ...
    values = list(InferredValue.infer_from_node(tree.node))
    assert all(value.is_inferred() for value in values)
    strings = list(value.as_string() for value in values)
    assert strings == ["Hello abc!"]


def test_infers_fstring_values() -> None:
    source = """
values_1 = ["abc", "def"]
for value1 in values_1:
    values_2 = ["ghi", "jkl"]
    for value2 in values_2:
        fstring = f"Hello {value1}, {value2}!"
"""
    maybe_tree = MaybeTree.from_source_code(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[2].value)  # value of fstring = ...
    values = list(InferredValue.infer_from_node(tree.node))
    assert all(value.is_inferred() for value in values)
    strings = list(value.as_string() for value in values)
    assert strings == ["Hello abc, ghi!", "Hello abc, jkl!", "Hello def, ghi!", "Hello def, jkl!"]


def test_infers_externally_defined_value() -> None:
    state = CurrentSessionState()
    state.named_parameters = {"my-widget": "my-value"}
    source = """
name = "my-widget"
value = dbutils.widgets.get(name)
"""
    maybe_tree = MaybeTree.from_source_code(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of value = ...
    values = list(InferredValue.infer_from_node(tree.node, state))
    strings = list(value.as_string() for value in values)
    assert strings == ["my-value"]


def test_infers_externally_defined_values() -> None:
    state = CurrentSessionState()
    state.named_parameters = {"my-widget-1": "my-value-1", "my-widget-2": "my-value-2"}
    source = """
for name in ["my-widget-1", "my-widget-2"]:
    value = dbutils.widgets.get(name)
"""
    maybe_tree = MaybeTree.from_source_code(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[0].value)  # value of value = ...
    values = list(InferredValue.infer_from_node(tree.node, state))
    strings = list(value.as_string() for value in values)
    assert strings == ["my-value-1", "my-value-2"]


def test_fails_to_infer_missing_externally_defined_value() -> None:
    state = CurrentSessionState()
    state.named_parameters = {"my-widget-1": "my-value-1", "my-widget-2": "my-value-2"}
    source = """
name = "my-widget"
value = dbutils.widgets.get(name)
"""
    maybe_tree = MaybeTree.from_source_code(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of value = ...
    values = InferredValue.infer_from_node(tree.node, state)
    assert all(not value.is_inferred() for value in values)


def test_survives_absence_of_externally_defined_values() -> None:
    source = """
    name = "my-widget"
    value = dbutils.widgets.get(name)
    """
    maybe_tree = MaybeTree.from_source_code(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[1].value)  # value of value = ...
    values = InferredValue.infer_from_node(tree.node, CurrentSessionState())
    assert all(not value.is_inferred() for value in values)


def test_infers_externally_defined_value_set() -> None:
    state = CurrentSessionState()
    state.named_parameters = {"my-widget": "my-value"}
    source = """
values = dbutils.widgets.getAll()
name = "my-widget"
value = values[name]
"""
    maybe_tree = MaybeTree.from_source_code(source)
    assert maybe_tree.tree is not None, maybe_tree.failure
    tree = maybe_tree.tree
    nodes = tree.locate(Assign, [])
    tree = Tree(nodes[2].value)  # value of value = ...
    values = list(InferredValue.infer_from_node(tree.node, state))
    strings = list(value.as_string() for value in values)
    assert strings == ["my-value"]
