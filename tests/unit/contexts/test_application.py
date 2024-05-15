import pytest

from databricks.labs.ucx.contexts.application import GlobalContext


@pytest.mark.parametrize("attribute", ["dependency_resolver", "pip_installer", "site_packages", "site_packages_path"])
def test_global_context_attributes_not_none(attribute: str):
    """Attributes should be not None"""
    # Goal is to improve test coverage
    ctx = GlobalContext()
    assert hasattr(ctx, attribute)
    assert getattr(ctx, attribute) is not None
