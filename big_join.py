import os
import json
import sys
import pandas as pd
from clickhouse_connect import get_client

def get_clickhouse_client(database_env_var: str):
    """
    Returns a ClickHouse client connection using environment variables.
    The 'database_env_var' parameter is the name of the environment variable
    that contains the target database name.
    """
    env_vars = {
        "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST"),
        "CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_PORT", "8123"),
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        database_env_var: os.getenv(database_env_var)
    }
    missing_vars = [key for key, value in env_vars.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")
    
    return get_client(
        host=env_vars["CLICKHOUSE_HOST"],
        port=int(env_vars["CLICKHOUSE_PORT"]),
        username=env_vars["CLICKHOUSE_USER"],
        password=env_vars["CLICKHOUSE_PASSWORD"],
        database=env_vars[database_env_var]
    )

def read_common_columns_json(json_file: str):
    """
    Reads a JSON file that maps common column names to the list of tables
    containing that column.
    """
    with open(json_file, 'r') as f:
        common_columns = json.load(f)
    return common_columns

def generate_join_query_for_column(column: str, tables: list):
    """
    Generates a join query based on the common column 'column' that is found
    in the list of 'tables'. The query uses the first table as the base and then
    joins each subsequent table on the common column.
    """
    if len(tables) < 2:
        return None  # nothing to join if only one table has that column
    
    # Start from the first table (alias t0)
    base_table = tables[0]
    query = f"SELECT * FROM {base_table} AS t0"
    # Chain JOINs using the common column
    for i, table in enumerate(tables[1:], start=1):
        query += f"\nJOIN {table} AS t{i} ON t0.{column} = t{i}.{column}"
    return query

def main():
    # Get the JSON file name from an environment variable, or use a default.
    json_file = os.getenv("COMMON_COLUMNS_JSON", "common_columns.json")
    if not os.path.exists(json_file):
        print(f"Error: JSON file '{json_file}' not found.")
        sys.exit(1)
    
    common_columns = read_common_columns_json(json_file)
    join_queries = {}
    
    # Generate join queries for every common column found in 2 or more tables.
    for column, tables in common_columns.items():
        if len(tables) < 2:
            continue
        query = generate_join_query_for_column(column, tables)
        if query:
            join_queries[column] = query

    if not join_queries:
        print("No common columns with more than one table found. Nothing to join.")
        sys.exit(0)
    
    # Display the generated join queries.
    print("Generated JOIN queries based on common columns:\n")
    for column, query in join_queries.items():
        print(f"Common Column: {column}")
        print(query)
        print("\n" + "="*40 + "\n")
    
    # Optional: execute one of the join queries and export the result to CSV.
    # For demonstration purposes, here we choose the first common column found.
    first_common_column = next(iter(join_queries))
    print(f"Executing join query for common column: {first_common_column}")

    # Ensure the required database connection environment variables are set.
    database_env_var = "DATABASE_UNITED"
    required_vars = ["CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_USER", 
                     "CLICKHOUSE_PASSWORD", database_env_var]
    missing_vars = [var for var in required_vars if os.getenv(var) is None]
    if missing_vars:
        print(f"Error: Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    try:
        print("Connecting to ClickHouse...")
        client = get_clickhouse_client(database_env_var)
        
        # Execute the join query.
        query_to_execute = join_queries[first_common_column]
                # Define new table name for the join result
        joined_table_name = f"joined_{first_common_column}"

        # Create the new table and store the join result
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {joined_table_name}
        ENGINE = MergeTree()
        ORDER BY tuple()
        AS {query_to_execute}
        """

        print(f"Creating and storing joined table: {joined_table_name}")
        client.command(create_table_query)
        print(f"Table '{joined_table_name}' successfully created and data stored.")
        result = client.query(query_to_execute)
        
        # Construct a pandas DataFrame from query result.
        # Note: Depending on the client, you may need to adapt how you retrieve the column names.
        # Here we assume 'result.column_names' gives a list of (name, type) tuples.
        # If it's a list of strings, you can use it directly.
        columns = [col if isinstance(col, str) else col[0] for col in result.column_names]
        df = pd.DataFrame(result.result_rows, columns=columns)
        
        output_csv = f"joined_{first_common_column}.csv"
        df.to_csv(output_csv, index=False)
        print(f"Join result saved to '{output_csv}'.")
    
    except Exception as e:
        print(f"Error executing join query: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
