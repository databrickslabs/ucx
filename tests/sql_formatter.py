from pathlib import Path

import sqlglot

def test_sql():
    query_folder = Path(__file__).parent / '../src/databricks/labs/ucx/assessment/queries'
    for query_file in query_folder.glob('*.sql'):
        with query_file.open('r') as f:
            sql_query = f.read()
            sql_ast = sqlglot.parse_one(sql_query, read='spark')
            formatted = sql_ast.sql(dialect='spark', pretty=True)
            print(formatted)
            print()
