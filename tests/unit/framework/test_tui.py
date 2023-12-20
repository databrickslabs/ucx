import pytest

from databricks.labs.ucx.framework.tui import Prompts


def test_choices_out_of_range(ws, mocker):
    install = Prompts()
    mocker.patch("builtins.input", return_value="42")
    with pytest.raises(ValueError):
        install.choice("foo", ["a", "b"])


def test_choices_not_a_number(ws, mocker):
    install = Prompts()
    mocker.patch("builtins.input", return_value="two")
    with pytest.raises(ValueError):
        install.choice("foo", ["a", "b"])


def test_choices_happy(ws, mocker):
    install = Prompts()
    mocker.patch("builtins.input", return_value="1")
    res = install.choice("foo", ["a", "b"])
    assert "b" == res
