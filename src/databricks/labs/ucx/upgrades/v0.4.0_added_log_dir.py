# pylint: disable=invalid-name,unused-argument
import logging

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    installation.upload('logs/README.md', b'# This folder contains logs from UCX workflows')
