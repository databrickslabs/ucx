from databricks.labs.ucx.contexts.application import GlobalContext


def test_global_context_attributes_not_none():
    """Attributes should be not None"""
    # Goal is to improve test coverage
    ctx = GlobalContext()
    assert ctx.pip_installer is not None
