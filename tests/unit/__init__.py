import logging

from databricks.labs.ucx.framework.logger import _install

_install()

logging.getLogger("tests").setLevel("DEBUG")
