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

Please note that depending on the table type and their location, the migration effort will differ.
[Full guidance](https://www.databricks.com/blog/migrating-tables-hive-metastore-unity-catalog-metastore)

- Object type: Managed \
  Table format: DELTA \
  Estimated effort: 0.5 \
  Suggestion: CTAS or recreate as external table in S3/ADLS, then SYNC
- Object type: Managed \
  Table format: not DELTA \
  Estimated effort: 2 \
  Suggestion: Can vary depending of table_format
- Object type: External \
  Table format: DELTA \
  Location: dbfs:/ \
  Estimated effort: 0.5 \
  Suggestion: CTAS or recreate as external table in S3/ADLS, then SYNC
- Object type: External \
  Table format: DELTA \
  Location: adl:/ \
  Estimated effort: 0.5 \
  Suggestion: CTAS or recreate as external table in S3/ADLS, then SYNC
- Object type: External \
  Table format: DELTA \
  Location: asbs:/ \
  Estimated effort: 0.5 \
  Suggestion: CTAS or recreate as external table in S3/ADLS, then SYNC
- Object type: External \
  Table format: DELTA \
  Location: adls:/ or S3:/ \
  Estimated effort: 0.1 \
  Suggestion: In place SYNC
- Object type: External \
  Table format: not DELTA \
  Estimated effort: 2 \
  Suggestion: Can vary depending of table_format
- Object type: External \
  Estimated effort: 2 \
  Suggestion: Recreate via Lakehouse Federation \
  Table format:
  - SQLSERVER
  - MYSQL
  - SNOWFLAKE
- Object type: View \
  Estimated effort: 2 \
  Suggestion: Recreate in target catalog on UC, can vary depending on the number of tables in the view
