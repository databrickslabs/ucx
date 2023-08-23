from databricks.sdk import WorkspaceClient


class AssessmentToolkit:
    def __init__(self, ws: WorkspaceClient, warehouse_id=None):
        self._ws = ws
        self._warehouse_id = warehouse_id

    def generate_report(self):
        report = {}
        tables = []
        catalogs = self._ws.catalogs.list()
        report['catalogs'] = {}
        report['catalogs']['count'] = len(catalogs)
        schema_count = 0
        schema_denied_count = 0
        for c in catalogs:
            try:
                schemas = self._ws.schemas.list(catalog_name=c.name)
                schema_count += len(schemas)
                for s in schemas:
                    tables = self._ws.tables.list(catalog_name=s.catalog_name, schema_name=s.name)
            except:
                schema_denied_count += 1
        report['schemas'] = {}
        report['schemas']['count'] = schema_count
        print(f"Schemas with no access: {schema_denied_count}")
        print(report)
        print(tables[:3])