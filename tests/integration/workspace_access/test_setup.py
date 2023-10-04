import logging
from functools import partial

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ComplexValue

from databricks.labs.ucx.framework.parallel import Threads

logger = logging.getLogger(__name__)


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
    Threads.gather("creating fixtures", [partial(_create_user, ws, uid) for uid in range(5)])
