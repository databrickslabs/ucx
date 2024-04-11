from databricks.labs.ucx.source_code.dependencies import SourceContainer
from databricks.labs.ucx.source_code.whitelist import Whitelist
from tests.unit import _load_sources


def test_reads_whitelist():
    # sample taken from https://github.com/databrickslabs/sandbox/blob/main/runtime-packages/sample-output.txt
    datas = _load_sources(SourceContainer, "sample-runtime-packages-output.txt")
    whitelist = Whitelist.parse(datas[0])
    assert whitelist.has("Apache Spark")
    assert whitelist.has("zipp")
    assert not whitelist.has("some voodoo")