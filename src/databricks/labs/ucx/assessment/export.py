import requests
import json
import os
import csv
import zipfile
import time

# Set up your Databricks credentials and workspace information
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("TOKEN")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID")
DASHBOARD_ID = os.getenv("DASHBOARD_ID")
OUTPUT_FILE = os.getenv("OUTPUT_FILE")

# Set up the headers with your Databricks token
headers = {
    'Authorization': f'Bearer {DATABRICKS_TOKEN}',
    'Content-Type': 'application/json'
}

class Exporter():
    # Function to check and start the SQL warehouse if it's not running
    def ensure_warehouse_running(warehouse_id, max_retries=5, delay=10):
        url = f"{DATABRICKS_HOST}/api/2.0/sql/warehouses/{warehouse_id}"

        for attempt in range(max_retries):
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to check warehouse status: {response.status_code} - {response.text}")
                return False

            status = response.json().get('state')
            if status == "RUNNING":
                print("Warehouse is running.")
                return True
            elif status == "STOPPED":
                print("Warehouse is stopped. Attempting to start it...")
                start_url = f"{DATABRICKS_HOST}/api/2.0/sql/warehouses/{warehouse_id}/start"
                start_response = requests.post(start_url, headers=headers)
                if start_response.status_code != 202:
                    print(f"Failed to start warehouse: {start_response.status_code} - {start_response.text}")
                    return False

            print(f"Waiting for warehouse to start (attempt {attempt + 1}/{max_retries})...")
            time.sleep(delay)

        print("Max retries reached. Warehouse is not running.")
        return False


    # Function to get the dashboard metadata
    def get_dashboard_content(dashboard_id):
        url = f"{DATABRICKS_HOST}/api/2.0/lakeview/dashboards/{dashboard_id}"
        payload = {
            "id": dashboard_id
        }
        response = requests.get(url, headers=headers, data=json.dumps(payload))

        if response.status_code != 200:
            print(f"Failed to retrieve dashboard: {response.status_code} - {response.text}")
            return None

        return response.json()


    # Function to process the serialized dashboard content and extract datasets
    def extract_datasets(serialized_dashboard):
        dashboard_content = json.loads(serialized_dashboard)
        return dashboard_content.get('datasets', [])


    # Function to create a persistent table with data and get the schema
    def create_table_with_data_and_get_schema(sql_query, table_name, max_retries=5, poll_delay=5):
        # Create the persistent table with data
        create_table_sql = f"CREATE OR REPLACE TABLE {table_name} AS {sql_query}"
        statement_id = execute_query(create_table_sql)

        # Poll for the completion of the query
        if not poll_for_query_completion(statement_id, max_retries, poll_delay):
            print(f"Failed to create table '{table_name}' as the query did not complete successfully.")
            return None

        # Retry logic for DESCRIBE TABLE
        for attempt in range(max_retries):
            # Describe the table to get the schema
            schema_query = f"DESCRIBE TABLE {table_name}"
            schema_statement_id = execute_query(schema_query)
            schema, _ = get_query_results(schema_statement_id)

            if schema:
                # Extract the schema names from the result
                schema_columns = [row[0] for row in schema if row]
                return schema_columns
            else:
                print(f"Attempt {attempt + 1} to describe the table failed. Retrying...")
                time.sleep(poll_delay)

        print(f"Max retries reached. Failed to retrieve schema for table '{table_name}'.")
        return None


    # Function to poll for the completion of a query
    def poll_for_query_completion(statement_id, max_retries, poll_delay):
        for attempt in range(max_retries):
            url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}"
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Failed to poll query status: {response.status_code} - {response.text}")
                return False

            result = response.json()
            state = result.get('status', {}).get('state')
            if state == "SUCCEEDED":
                print("Query succeeded.")
                return True
            elif state == "FAILED":
                print(f"Query failed: {result['status']['error']['message']}")
                return False

            print(f"Query is still running... (attempt {attempt + 1}/{max_retries})")
            time.sleep(poll_delay)

        print("Max retries reached. Query did not complete.")
        return False


    # Function to execute the SQL query
    def execute_query(sql_query):
        url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
        payload = {
            "statement": sql_query,
            "warehouse_id": WAREHOUSE_ID
        }

        print("Executing SQL Query:", sql_query)
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            print(f"Failed to execute query: {response.status_code} - {response.text}")
            return None

        statement_data = response.json()
        return statement_data['statement_id']


    # Function to get the schema and query results by statement ID with enhanced error handling
    def get_query_results(statement_id):
        url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}"

        while True:
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Failed to retrieve query results: {response.status_code} - {response.text}")
                return None, None

            result = response.json()

            if result['status']['state'] == 'SUCCEEDED':
                if 'data_array' in result['result']:
                    data_array = result['result']['data_array']
                    return data_array, None  # Returning None for schema here as we extract it separately
                else:
                    print("No data array found in the response.")
                    return [], None  # Return empty list to indicate no data but still proceed
            elif result['status']['state'] == 'FAILED':
                print(f"Query failed: {result['status']['error']['message']}")
                return None, None
            else:
                print(f"Query is still running... Waiting before retrying.")
                time.sleep(5)  # Wait before checking again


    # Function to write datasets to CSV files and zip them
    def write_csvs_to_zip(zip_filename, datasets):
        with zipfile.ZipFile(zip_filename, 'w') as zipf:
            for dataset_name, rows, columns in datasets:
                csv_filename = f"{dataset_name}.csv"

                # If no data rows were returned, ensure at least an empty CSV with headers is created
                if rows is None:
                    print(f"Warning: No data returned for dataset '{dataset_name}'. Creating empty CSV...")
                    rows = []

                # Write CSV data to a string buffer
                with open(csv_filename, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)

                    # Write the header (column names)
                    csvwriter.writerow(columns)

                    # Write the data rows (which may be empty)
                    csvwriter.writerows(rows)

                # Add the CSV file to the zip archive
                zipf.write(csv_filename)

                # Clean up the CSV file after adding it to the zip (optional)
                os.remove(csv_filename)

        print(f"Created zip file: {zip_filename}")


    # Main function to get all datasets' outputs from a dashboard and save as a zip with CSVs
    def get_dashboard_data_as_csv_zip(dashboard_id, zip_filename):
        # Ensure the warehouse is running before starting
        if not ensure_warehouse_running(WAREHOUSE_ID):
            print("Warehouse is not running. Exiting.")
            return

        datasets = []
        dashboard_content = get_dashboard_content(dashboard_id)

        if not dashboard_content:
            print("Dashboard content could not be retrieved.")
            return

        serialized_dashboard = dashboard_content.get('serialized_dashboard', '{}')
        datasets_info = extract_datasets(serialized_dashboard)

        for dataset in datasets_info:
            query = dataset.get('query')
            if query:
                print(f"Executing query: {dataset['displayName']}")

                # Create a persistent table with data and get the schema
                table_name = f"temp_table_{dataset['displayName'][:20].replace(' ', '_')}"  # Limit and format name
                schema = create_table_with_data_and_get_schema(query, table_name)
                if schema:
                    print("Extracted Schema from Persistent Table:", schema)
                    datasets.append((dataset['displayName'], None, schema))  # Data to be filled in later
                else:
                    print(f"Failed to extract schema for dataset '{dataset['displayName']}'.")

        # Now fetch the data from each persistent table
        for dataset_name, _, schema in datasets:
            query = f"SELECT * FROM temp_table_{dataset_name[:20].replace(' ', '_')}"
            statement_id = execute_query(query)
            query_results, _ = get_query_results(statement_id)
            datasets[datasets.index((dataset_name, _, schema))] = (dataset_name, query_results, schema)

        # Write all datasets to a zip file with CSVs
        write_csvs_to_zip(zip_filename, datasets)

        # Optionally, drop the persistent tables to clean up
        for dataset_name, _, _ in datasets:
            table_name = f"temp_table_{dataset_name[:20].replace(' ', '_')}"
            drop_table_sql = f"DROP TABLE IF EXISTS {table_name}"
            execute_query(drop_table_sql)


    # Example usage
    get_dashboard_data_as_csv_zip(DASHBOARD_ID, "output_datasets.zip")
