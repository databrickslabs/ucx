-- widget title=Table estimates, row=3, col=0, size_x=2, size_y=8
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

| object_type | table_format                         | location       | estimated effort | suggestion                                                                               |
|-------------|--------------------------------------|----------------|------------------|------------------------------------------------------------------------------------------|
| MANAGED     | DELTA                                | -              | 0.5              | CTAS or recreate as external table in S3/ADLS, then SYNC                                 |
| MANAGED     | not DELTA                            | -              | 2                | Can vary depending of table_format                                                       |
| EXTERNAL    | DELTA                                | dbfs:/         | 0.5              | CTAS or recreate as external table in S3/ADLS, then SYNC                                 |
| EXTERNAL    | DELTA                                | adl:/          | 0.5              | CTAS or recreate as external table in S3/ADLS, then SYNC                                 |
| EXTERNAL    | DELTA                                | wasbs:/        | 0.5              | CTAS or recreate as external table in S3/ADLS, then SYNC                                 |
| EXTERNAL    | DELTA                                | adls:/ or S3:/ | 0.1              | In place SYNC                                                                            |
| EXTERNAL    | not DELTA                            | -              | 2                | Can vary depending of table_format                                                       |
| EXTERNAL    | -SQLSERVER<br/>-MYSQL<br/>-SNOWFLAKE | -              | 2                | Recreate via Lakehouse Federation                                                        |
| VIEW        | -                                    | -              | 2                | Recreate in target catalog on UC, can vary depending on the number of tables in the view |
