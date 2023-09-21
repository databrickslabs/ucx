import logging

from databricks.labs.ucx.framework.logger import _install

logging.getLogger("tests").setLevel("DEBUG")
