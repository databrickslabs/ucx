import shutil
from functools import cached_property
from collections.abc import Callable

import pytest

from databricks.labs.ucx.framework.utils import run_command
from tests.integration.contexts.workspace import MockWorkspaceContext


class MockLocalAwsCli(MockWorkspaceContext):
    @cached_property
    def aws_cli_run_command(self) -> Callable[[str | list[str]], tuple[int, str, str]]:
        if not self.is_aws:
            pytest.skip("Aws only")
        if not shutil.which("aws"):
            pytest.skip("Local test only")
        return run_command

    @cached_property
    def aws_profile(self) -> str:
        return self._env_or_skip("AWS_PROFILE")
