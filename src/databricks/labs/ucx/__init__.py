from databricks.sdk.core import with_user_agent_extra, with_product
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.ucx.__about__ import __version__

install_logger()

# Add ucx/<version> for projects depending on ucx as a library
with_user_agent_extra("ucx", __version__)

# Add ucx/<version> for re-packaging of ucx, where product name is omitted
with_product("ucx", __version__)
