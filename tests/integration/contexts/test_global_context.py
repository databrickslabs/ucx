from databricks.labs.ucx.contexts.application import GlobalContext


def test_cached_properties():
    ctx = GlobalContext()
    assert ctx.site_packages_path is not None