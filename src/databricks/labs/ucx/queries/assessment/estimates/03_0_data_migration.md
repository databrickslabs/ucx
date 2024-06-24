---
height: 10
width: 2
---

## 4 - Data migration to UC

Once you have defined your data model in UC and that you've created appropriate Storage Credentials and External Locations,
you can then migrate your data to UC

Assumptions for a single table migration estimates:

- UC Data model has been defined
- Storage Credentials are in place
- External Locations are in place
- Target Catalogs and schemas has been defined
- Grants has been defined

Please note that depending on the table type and their location, the migration effort will differ (see table below).
[Full guidance](https://www.databricks.com/blog/migrating-tables-hive-metastore-unity-catalog-metastore)
