# ucx[session-state] {"dbr_version": [13, 3]}
# ucx[catalog-api-in-shared-clusters:+1:0:+1:13] spark.catalog functions require DBR 14.3 LTS or above on UC Shared Clusters
spark.catalog.tableExists("table")
# ucx[catalog-api-in-shared-clusters:+1:0:+1:13] spark.catalog functions require DBR 14.3 LTS or above on UC Shared Clusters
spark.catalog.listDatabases()


def catalog():
    pass


catalog()


class Fatalog:
    def tableExists(self, x): ...
class Foo:
    def catalog(self):
        Fatalog()


x = Foo()
x.catalog.tableExists("...")
