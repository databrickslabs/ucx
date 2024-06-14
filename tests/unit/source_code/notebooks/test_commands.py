import pytest

from databricks.labs.ucx.source_code.notebooks.commands import PipMagic


@pytest.mark.parametrize(
    "code,split",
    [
        ("%pip install foo", ["%pip", "install", "foo"]),
        ("%pip install", ["%pip", "install"]),
        ("%pip installl foo", ["%pip", "installl", "foo"]),
        ("%pip install foo --index-url bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        ("%pip install foo --index-url bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        ("%pip install foo --index-url \\\n bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        ("%pip install foo --index-url bar\nmore code", ["%pip", "install", "foo", "--index-url", "bar"]),
        (
            "%pip install foo --index-url bar\\\n -t /tmp/",
            ["%pip", "install", "foo", "--index-url", "bar", "-t", "/tmp/"],
        ),
        ("%pip install foo --index-url \\\n bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        (
            "%pip install ./distribution/dist/thingy-0.0.1-py2.py3-none-any.whl",
            ["%pip", "install", "./distribution/dist/thingy-0.0.1-py2.py3-none-any.whl"],
        ),
        (
            "%pip install distribution/dist/thingy-0.0.1-py2.py3-none-any.whl",
            ["%pip", "install", "distribution/dist/thingy-0.0.1-py2.py3-none-any.whl"],
        ),
    ],
)
def test_pip_command_split(code, split):
    assert PipMagic._split(code) == split  # pylint: disable=protected-access
