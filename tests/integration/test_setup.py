from functools import partial

import pytest

from databricks.labs.ucx.providers.logger import logger
from databricks.labs.ucx.utils import ThreadedExecution
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ComplexValue

Threader = partial(ThreadedExecution, num_threads=40)


def _create_user(ws: WorkspaceClient, uid: str):
    user_name = f"test-user-{uid}@example.com"
    # TODO: listing is expensive for SCIM, better swallow the exception
    potential_user = list(ws.users.list(filter=f"userName eq '{user_name}'"))
    if potential_user:
        logger.debug(f"User {user_name} already exists, skipping its creation")
    else:
        ws.users.create(
            active=True,
            user_name=user_name,
            display_name=f"test-user-{uid}",
            emails=[ComplexValue(display=None, primary=True, value=f"test-user-{uid}@example.com")],
        )


def test_create_users(ws):
    pytest.skip("run only in debug")
    executables = [partial(_create_user, ws, uid) for uid in range(200)]
    Threader(executables).run()
