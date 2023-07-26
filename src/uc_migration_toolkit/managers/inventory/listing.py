from collections.abc import Iterator

from databricks.sdk.service.ml import ModelDatabricks

from uc_migration_toolkit.providers.client import provider


class CustomListing:
    """
    Provides utility functions for custom listing operations
    """

    @staticmethod
    def list_models() -> Iterator[ModelDatabricks]:
        for model in provider.ws.model_registry.list_models():
            model_with_id = provider.ws.model_registry.get_model(model.name).registered_model_databricks
            yield model_with_id
