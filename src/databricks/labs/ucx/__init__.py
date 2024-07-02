from databricks.sdk.core import with_user_agent_extra
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.ucx.__about__ import __version__

install_logger()

with_user_agent_extra("ucx", __version__)
