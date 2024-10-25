from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from tests.integration.contexts.common import IntegrationContext


class MockRuntimeContext(IntegrationContext, RuntimeContext):
    def __init__(self, *args):
        super().__init__(*args)
        RuntimeContext.__init__(self)
