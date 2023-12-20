import os
from unittest.mock import MagicMock

from databricks.labs.ucx.framework.wheels import Wheels


def test_build_wheel():
    ws = MagicMock()
    wheels = Wheels(ws, "/Shared/.ucx", "0.0.1")
    with wheels:
        assert os.path.exists(wheels._local_wheel)


def test_version():
    ws = MagicMock()
    wheels = Wheels(ws, "/Shared/.ucx", "0.0.1")
    assert "+" in wheels.version()
