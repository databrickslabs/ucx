import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.useragent import with_extra

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.install import WorkspaceInstallation

logger = logging.getLogger("databricks.labs.ucx.install")

if __name__ == "__main__":
    with_extra("cmd", "uninstall")
    logger.setLevel("INFO")
    ws = WorkspaceClient(product="ucx", product_version=__version__)
    installer = WorkspaceInstallation.current(ws)
    installer.uninstall()
