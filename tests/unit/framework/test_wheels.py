import os
from unittest.mock import MagicMock

import pytest

from databricks.labs.ucx.framework.wheels import Wheels


@pytest.mark.skip  # move this test to a separate package, as it slows down the overall execution
def test_build_wheel():
    ws = MagicMock()
    wheels = Wheels(ws, "/Shared/.ucx", "0.0.1")
    with wheels:
        assert os.path.exists(wheels._local_wheel)


def test_version():
    ws = MagicMock()
    wheels = Wheels(ws, "/Shared/.ucx", "0.0.1")
    assert "+" in wheels.version()
