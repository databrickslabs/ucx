import pytest

from databricks.labs.ucx.generic import StrEnum


def test_error():
    with pytest.raises(TypeError):

        class InvalidEnum(StrEnum):
            A = 1


def test_generate():
    class Sample(StrEnum):
        A = "a"
        B = "b"

    assert Sample._generate_next_value_("C", 3) == "C"
