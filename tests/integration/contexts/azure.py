from functools import cached_property

import pytest

from tests.integration.contexts.workspace import MockWorkspaceContext


class MockLocalAzureCli(MockWorkspaceContext):
    @cached_property
    def azure_cli_authenticated(self) -> bool:
        if not self.is_azure:
            pytest.skip("Azure only")
        if self.connect_config.auth_type != "azure-cli":
            pytest.skip("Local test only")
        return True

    @cached_property
    def azure_subscription_id(self) -> str:
        return self._env_or_skip("TEST_SUBSCRIPTION_ID")
