import logging
from collections.abc import Iterator
from dataclasses import dataclass
from functools import partial

from databricks.labs.ucx.tacl._internal import CrawlerBase, SqlBackend
from databricks.labs.ucx.utils import ThreadedExecution

logger = logging.getLogger(__name__)


@dataclass
class JobInfo:
    job_id: int
    name: str
    success: int
    failures: list[str]
