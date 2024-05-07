import pytest


@pytest.fixture
def simple_ctx(installation_ctx, sql_backend, ws):
    return installation_ctx.replace(
        sql_backend=sql_backend,
        workspace_client=ws,
        connect=ws.config,
    )
