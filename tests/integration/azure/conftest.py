import pytest


def delete_ucx_created_resources(api_client):
    """Delete UCX created resources"""
    for resource in api_client.list():
        if resource.name is not None and resource.comment is not None and resource.comment == "Created by UCX":
            api_client.delete(resource.name, force=True)


@pytest.fixture
def clean_storage_credentials(az_cli_ctx):
    """Clean test generated storage credentials."""
    delete_ucx_created_resources(az_cli_ctx.workspace_client.storage_credentials)
    yield
    delete_ucx_created_resources(az_cli_ctx.workspace_client.storage_credentials)


@pytest.fixture
def clean_external_locations(az_cli_ctx):
    """Clean test generated external locations."""
    delete_ucx_created_resources(az_cli_ctx.workspace_client.external_locations)
    yield
    delete_ucx_created_resources(az_cli_ctx.workspace_client.external_locations)
