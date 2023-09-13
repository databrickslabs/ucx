import pytest

from databricks.labs.ucx.providers.mixins.hardening import rate_limited


def test_ratelimiting(mocker):
    _x = 0

    def _now():
        nonlocal _x
        _x += 0.2
        return _x

    mocker.patch("time.time", side_effect=_now)
    sleep = mocker.patch("time.sleep")

    @rate_limited(max_requests=5)
    def try_something():
        return 1

    for _ in range(0, 20):
        try_something()

    sleep.assert_called()
    assert 4.2 == pytest.approx(_x)
