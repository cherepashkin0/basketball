import os
from collections import defaultdict
import pandas as pd
from clickhouse_connect import get_client
import json
import numpy as np


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

# Function to get column information using the client
def get_column_info(client):
    """Get column information for all tables using the provided client."""
    # Get list of tables
    tables_result = client.query("SHOW TABLES")
    tables = [row[0] for row in tables_result.result_rows]
    
    print(f"Found {len(tables)} tables in the database.")
    
    # Get columns for each table with their positions and data types
    table_columns = {}
    for table in tables:
        columns_result = client.query(f"DESCRIBE TABLE {table}")
        column_info = [{'name': row[0], 'type': row[1], 'position': i} 
                      for i, row in enumerate(columns_result.result_rows)]
        table_columns[table] = column_info
    
    return table_columns

# Function to find columns with the same titles across different tables
def find_common_columns(table_columns):
    """Find columns that appear in multiple tables."""
    # Dictionary to store column name -> list of tables containing it
    column_to_tables = defaultdict(list)
    
    # Populate the dictionary with table and position information
    for table, columns in table_columns.items():
        for column in columns:
            column_to_tables[column['name']].append({
                'table': table,
                'position': column['position'],
                'type': column['type']
            })
    
    # Filter to only keep columns that appear in multiple tables
    common_columns = {column: tables for column, tables in column_to_tables.items() if len(tables) > 1}
    
    return common_columns

# Function to check if a column type is numeric
def is_numeric_type(column_type):
    """Check if a column type is numeric based on its ClickHouse type."""
    numeric_types = [
        'Int', 'UInt', 'Float', 'Decimal', 'DateTime', 'Date'
    ]
    return any(t in column_type for t in numeric_types)

def get_numeric_sample(client, table, column_name, column_type, sample_size=1000):
    """
    Get a sample of numeric values from a column.
    Handles different column types appropriately with enhanced conversion for non-numeric columns.
    """
    try:
        # For columns that are already numeric, we don't need conversion
        if is_numeric_type(column_type):
            sample_query = f"""
            SELECT {column_name} as numeric_value
            FROM {table}
            WHERE NOT isNull({column_name})
            LIMIT {sample_size}
            """
        elif 'String' in column_type or 'Nullable(String)' in column_type:
            # For string columns, try multiple conversion methods
            sample_query = f"""
            SELECT 
                CASE
                    -- Try to extract numbers from strings that may contain mixed content
                    WHEN match({column_name}, '^[\\\\s]*[-+]?[0-9]*\\\\.?[0-9]+([eE][-+]?[0-9]+)?[\\\\s]*$')
                        THEN toFloat64OrNull({column_name})
                    -- Try to extract numerical part from strings like "$123.45" or "123.45 units"
                    WHEN match({column_name}, '[-+]?[0-9]*\\\\.?[0-9]+([eE][-+]?[0-9]+)?')
                        THEN toFloat64OrNull(extractAll({column_name}, '[-+]?[0-9]*\\\\.?[0-9]+([eE][-+]?[0-9]+)?')[1])
                    ELSE NULL
                END as numeric_value
            FROM {table}
            WHERE NOT isNull({column_name}) AND length({column_name}) > 0
            LIMIT {sample_size}
            """
        elif 'Array' in column_type:
            # For array columns, try to extract the first numeric element
            sample_query = f"""
            SELECT 
                arrayElement(
                    arrayFilter(x -> isNotNull(toFloat64OrNull(x)), 
                    arrayMap(x -> toString(x), {column_name})
                ), 1) as numeric_value
            FROM {table}
            WHERE NOT isNull({column_name}) AND length({column_name}) > 0
            LIMIT {sample_size}
            """
        elif 'JSON' in column_type or 'Object' in column_type:
            # For JSON columns, try different approaches to extract numeric values
            sample_query = f"""
            SELECT 
                CASE
                    WHEN isNotNull(toFloat64OrNull(visitParamExtractString({column_name}, 'value')))
                        THEN toFloat64OrNull(visitParamExtractString({column_name}, 'value'))
                    WHEN isNotNull(toFloat64OrNull(JSONExtractString({column_name}, 'value')))
                        THEN toFloat64OrNull(JSONExtractString({column_name}, 'value'))
                    WHEN isNotNull(toFloat64OrNull(JSONExtractString({column_name}, 'amount')))
                        THEN toFloat64OrNull(JSONExtractString({column_name}, 'amount'))
                    WHEN isNotNull(toFloat64OrNull(JSONExtractString({column_name}, 'count')))
                        THEN toFloat64OrNull(JSONExtractString({column_name}, 'count'))
                    WHEN isNotNull(toFloat64OrNull({column_name}))
                        THEN toFloat64OrNull({column_name})
                    ELSE NULL
                END as numeric_value
            FROM {table}
            WHERE NOT isNull({column_name})
            LIMIT {sample_size}
            """
        else:
            # For other types, try a simple conversion
            sample_query = f"""
            SELECT toFloat64OrNull({column_name}) as numeric_value
            FROM {table}
            WHERE NOT isNull({column_name})
            LIMIT {sample_size}
            """
            
        result = client.query(sample_query)
        if not result.result_rows:
            return None  # No data in column
            
        # Convert to numpy array for easier manipulation
        values = np.array([row[0] for row in result.result_rows if row[0] is not None])
        
        # Check if values are not None and can be treated as numeric
        if values.size == 0:
            return None
            
        # Filter out non-finite values
        values = values[np.isfinite(values)]
        
        # If we have enough valid values, return them
        if values.size > 0:
            return values
            
        return None
    except Exception as e:
        print(f"Error sampling {table}.{column_name}: {str(e)}")
        return None

# Expanded function to check if a column type is numeric
def is_numeric_type(column_type):
    """Check if a column type is numeric based on its ClickHouse type."""
    numeric_types = [
        'Int', 'UInt', 'Float', 'Decimal', 'DateTime', 'Date', 
        'SimpleAggregateFunction', 'AggregateFunction'
    ]
    return any(t in column_type for t in numeric_types)

# Function to get statistics for a column
def get_column_stats(client, table, column_name, column_type):
    """
    Get min, max, mean, and std for a column, handling NaN values.
    Returns None if the column can't be converted to numeric.
    """
    try:
        # Get a sample of numeric values
        numeric_values = get_numeric_sample(client, table, column_name, column_type)
        
        if numeric_values is None or len(numeric_values) == 0:
            return None
            
        # Calculate statistics using numpy's NaN-aware functions
        stats = {
            'min': float(np.nanmin(numeric_values)),
            'max': float(np.nanmax(numeric_values)),
            'mean': float(np.nanmean(numeric_values)),
            'std': float(np.nanstd(numeric_values))
        }
        
        # Check for invalid statistics (can happen with extreme values)
        if any(not np.isfinite(v) for v in stats.values()):
            return None
            
        return stats
    except Exception as e:
        print(f"Error getting stats for {table}.{column_name}: {str(e)}")
        return None

# Function to display results in a readable format
def display_results(common_columns):
    """Display common columns and the tables they appear in."""
    print("\nColumns found in multiple tables:\n")
    
    # Sort by number of tables (descending) and then by column name
    sorted_columns = sorted(
        common_columns.items(),
        key=lambda x: (-len(x[1]), x[0])
    )
    
    for column, table_info in sorted_columns:
        print(f"Column: {column}")
        print(f"Found in {len(table_info)} tables:")
        for info in sorted(table_info, key=lambda x: x['table']):
            print(f"  - {info['table']} (position: {info['position']})")
        print()

# Function to export results to CSV
def export_to_csv(common_columns, output_file='common_columns.csv'):
    """Export results to a CSV file."""
    # Prepare data for DataFrame
    data = []
    for column, table_info in common_columns.items():
        tables = [info['table'] for info in table_info]
        positions = [info['position'] for info in table_info]
        data.append({
            'column_name': column,
            'table_count': len(table_info),
            'tables': ', '.join(sorted(tables)),
            'positions': ', '.join(str(pos) for pos in positions)
        })
    
    # Create and save DataFrame
    df = pd.DataFrame(data)
    df = df.sort_values(by=['table_count', 'column_name'], ascending=[False, True])
    df.to_csv(output_file, index=False)
    print(f"Results exported to {output_file}")

def export_to_json(common_columns, column_stats, output_file='common_columns.json'):
    """
    Export only numeric columns and their associated tables to a JSON file,
    including position information and statistics.
    """
    # Prepare a structured format for the JSON output
    output_data = {}
    
    # Only include columns that have statistical data (numeric columns)
    numeric_columns = {col: info for col, info in common_columns.items() 
                      if col in column_stats and any(column_stats[col].values())}
    
    for column_name, table_info in numeric_columns.items():
        output_data[column_name] = {
            'table_count': len(table_info),
            'tables': []
        }
        
        for info in table_info:
            table_name = info['table']
            table_entry = {
                'name': table_name,
                'position': info['position'],
                'type': info['type']
            }
            
            # Add statistics if available for this table
            if table_name in column_stats[column_name]:
                table_entry['statistics'] = column_stats[column_name][table_name]
            
            output_data[column_name]['tables'].append(table_entry)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    
    print(f"Results exported to {output_file}")

# Main function
def main():
    # Specify the environment variable that contains the database name
    database_env_var = "DATABASE_UNITED"
    
    # Ensure all required environment variables are set
    required_vars = ["CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_USER", 
                     "CLICKHOUSE_PASSWORD", database_env_var]
    missing = [var for var in required_vars if os.getenv(var) is None]
    
    if missing:
        print(f"Error: Missing environment variables: {', '.join(missing)}")
        print("Please set these environment variables before running the script.")
        exit(1)
    
    try:
        print("Connecting to ClickHouse...")
        client = get_clickhouse_client(database_env_var)
        
        print("Retrieving table information...")
        table_columns = get_column_info(client)
        
        common_columns = find_common_columns(table_columns)
        
        print(f"Found {len(common_columns)} columns that appear in multiple tables.")
        
        display_results(common_columns)
        
        # Get statistics for each column in each table
        print("Collecting statistics for numeric columns (this may take some time)...")
        column_stats = defaultdict(dict)
        for column_name, table_info in common_columns.items():
            for info in table_info:
                table_name = info['table']
                column_type = info['type']
                stats = get_column_stats(client, table_name, column_name, column_type)
                if stats:  # Only include statistics if they exist
                    column_stats[column_name][table_name] = stats
        
        export_to_csv(common_columns)
        export_to_json(common_columns, column_stats)
        
        # Report on how many columns had statistics
        numeric_columns = sum(1 for col in column_stats if any(column_stats[col].values()))
        print(f"Found {numeric_columns} numeric columns out of {len(common_columns)} common columns.")
        print(f"Only numeric columns are included in the JSON output.")
        
    except Exception as e:
        print(f"Error: {e}")
        exit(1)

if __name__ == "__main__":
    main()