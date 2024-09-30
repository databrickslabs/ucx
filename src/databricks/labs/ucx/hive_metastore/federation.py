from databricks.labs.blueprint.commands import CommandExecutor

class DbfsRootResolver:
    def __init__(self, command_executor: CommandExecutor):
        self._command_executor = command_executor

    def run(self) -> str:
        code = """
// Warning: relying on an undocumented API here. This is brittle, as it may break.
// TODO: switch to a publicly documented API when available.
URI = spark._jvm.java.net.URI
Some = spark._jvm.scala.Some
HmsFedDbfsUtils = spark._jvm.com.databricks.sql.managedcatalog.connections.HmsFedDbfsUtils
rootLocationOpt = HmsFedDbfsUtils.resolveDbfsPath(Some(URI("dbfs:/user/hive/warehouse")))
if rootLocationOpt.isDefined():
    print(rootLocationOpt.get().toString())
else:
    print("Unknown")
        """
        results = self._command_executor.run(code)
        if (results == "Unknown"):
            raise ValueError("Could not resolve the root location of the workspace")
        return results

