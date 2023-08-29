import logging

from databricks.labs.ucx.logger import _install

_install()

logging.getLogger("tests").setLevel("DEBUG")
