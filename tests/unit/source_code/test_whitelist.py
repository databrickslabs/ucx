from databricks.labs.ucx.source_code.dependencies import SourceContainer
from databricks.labs.ucx.source_code.whitelist import Whitelist, UCCompatibility
from tests.unit import _load_sources


def test_reads_whitelist():
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    assert not whitelist.compatibility("spark.sql")
    assert whitelist.compatibility("databricks.sdk.service.compute") == UCCompatibility.FULL
    assert whitelist.compatibility("sys") == UCCompatibility.FULL
    assert whitelist.compatibility("os.path") == UCCompatibility.FULL
