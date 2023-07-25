from functools import partial
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ComplexValue
from dotenv import load_dotenv

from uc_migration_toolkit.config import RateLimitConfig
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.utils import ThreadedExecution

Threader = partial(ThreadedExecution, num_threads=40, rate_limit=RateLimitConfig())


def _create_user(_ws: WorkspaceClient, uid: str):
    user_name = f"test-user-{uid}@example.com"
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


def _create_users(_ws: WorkspaceClient):
    executables = [partial(_create_user, ws, uid) for uid in range(200)]
    Threader(executables).run()


if __name__ == "__main__":
    principal_env = Path(__file__).parent.parent / ".env.principal"
    if principal_env.exists():
        logger.info("Using credentials provided in .env.principal")
        load_dotenv(dotenv_path=principal_env)

    logger.debug("setting up the workspace client")
    ws = WorkspaceClient()
    user_info = ws.current_user.me()
    logger.debug("workspace client is set up")

    _create_users(ws)
