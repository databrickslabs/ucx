from databricks.labs.ucx.code.queries import FromTable
from databricks.labs.ucx.hive_metastore.table_migrate import Index, MigrationStatus


def test_migrates_from_table_no_tables():
    ftf = FromTable(Index([]))

    old_query = "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    assert not ftf.match(old_query)


def test_partially_migrated_queries_dont_match():
    ftf = FromTable(
        Index(
            [
                MigrationStatus(
                    src_schema='old', src_table='things', dst_catalog='brand', dst_schema='new', dst_table='stuff'
                ),
            ]
        )
    )

    old_query = "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    assert not ftf.match(old_query)


def test_fully_migrated_queries_match():
    ftf = FromTable(
        Index(
            [
                MigrationStatus(
                    src_schema='old', src_table='things', dst_catalog='brand', dst_schema='new', dst_table='stuff'
                ),
                MigrationStatus(
                    src_schema='other',
                    src_table='matters',
                    dst_catalog='some',
                    dst_schema='certain',
                    dst_table='issues',
                ),
            ]
        )
    )

    old_query = "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    new_query = "SELECT * FROM brand.new.stuff LEFT JOIN some.certain.issues USING (x) WHERE state > 1 LIMIT 10"
    assert ftf.apply(old_query) == new_query
