from databricks.labs.ucx.contexts.application import GlobalContext


def test_cached_properties():
    # purpose of the below is merely to improve test coverage
    ctx = GlobalContext()
    assert ctx.site_packages is not None
    assert ctx.site_packages_path is not None
    assert ctx.dependency_resolver is not None
    assert ctx.dependency_resolver is not None
