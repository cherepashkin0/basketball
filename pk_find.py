import os
from clickhouse_connect import get_client

def get_clickhouse_client(database_env_var: str):
    """
    Returns a ClickHouse client connection using environment variables.
    The 'database_env_var' parameter is the name of the environment variable
    that contains the target database name.
    """
    env_vars = {
        "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST"),
        "CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_PORT", 8123),
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

def estimate_candidate_keys(database_env_var: str, table_name: str):
    """
    Estimate candidate primary key columns for the specified table in a ClickHouse database.
    
    A candidate column is one where:
      - The number of distinct values equals the total number of rows.
      - The column contains no NULL values.
      
    This function uses the connection helper from the provided file which employs the
    clickhouse_connect library.
    
    Parameters:
      database_env_var (str): Name of the environment variable holding the database name.
      table_name (str): The name of the table to analyze.
    
    Returns:
      List[str]: List of column names that are potential primary key candidates.
    """
    # Obtain the ClickHouse client.
    client = get_clickhouse_client(database_env_var)

    # Retrieve the total number of rows in the table.
    total_rows_query = f"SELECT count() FROM {table_name}"
    total_rows_result = client.query(total_rows_query)
    total_rows = total_rows_result.result_rows[0][0]

    # Get the list of columns from system.columns.
    schema_query = f"""
        SELECT name 
        FROM system.columns 
        WHERE database = '{client.database}' AND table = '{table_name}'
    """
    schema_result = client.query(schema_query)
    columns = [row[0] for row in schema_result.result_rows]

    candidate_columns = []
    for col in columns:
        # Use uniqExact() in place of countExactDistinct() for exact distinct counts.
        query = f"SELECT uniqExact({col}), count({col}) FROM {table_name}"
        res = client.query(query)
        distinct_count, nonnull_count = res.result_rows[0]
        
        # Check if the column has all unique and non-null values.
        if total_rows == distinct_count and total_rows == nonnull_count:
            candidate_columns.append(col)

    return candidate_columns

# Example usage:
if __name__ == "__main__":
    database_env = "DATABASE_UNITED"  # Name of your environment variable for the database
    table_name = "game"  # Replace with your table name

    candidate_keys = estimate_candidate_keys(database_env, table_name)
    if candidate_keys:
        print(f"Candidate primary key columns in '{table_name}': {candidate_keys}")
    else:
        print(f"No single column candidate primary key detected in '{table_name}'.")
