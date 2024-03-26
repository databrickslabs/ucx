import logging

# pylint: disable-next=unused-wildcard-import,wildcard-import
from tests.integration.conftest import *  # noqa: F403

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

logger = logging.getLogger(__name__)
