import pytest

from databricks.labs.ucx.framework.tui import MockPrompts, Prompts


def test_choices_out_of_range(mocker):
    prompts = Prompts()
    mocker.patch("builtins.input", return_value="42")
    with pytest.raises(ValueError):
        prompts.choice("foo", ["a", "b"])


def test_choices_not_a_number(mocker):
    prompts = Prompts()
    mocker.patch("builtins.input", return_value="two")
    with pytest.raises(ValueError):
        prompts.choice("foo", ["a", "b"])


def test_choices_happy(mocker):
    prompts = Prompts()
    mocker.patch("builtins.input", return_value="1")
    res = prompts.choice("foo", ["a", "b"])
    assert "b" == res


def test_ask_for_int():
    prompts = MockPrompts({r".*": ""})
    res = prompts.question("Number of threads", default="8", valid_number=True)
    assert "8" == res
