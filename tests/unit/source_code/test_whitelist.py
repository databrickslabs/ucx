from databricks.labs.ucx.source_code.graph import SourceContainer
from databricks.labs.ucx.source_code.whitelist import UCCompatibility, Whitelist
from tests.unit import _load_sources


def test_reads_whitelist():
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    assert whitelist.compatibility("spark.sql") == UCCompatibility.UNKNOWN
    assert whitelist.compatibility("databricks.sdk.service.compute") == UCCompatibility.FULL
    assert whitelist.compatibility("sys") == UCCompatibility.FULL
    assert whitelist.compatibility("os.path") == UCCompatibility.FULL
    assert whitelist.compatibility("databrix") == UCCompatibility.NONE
