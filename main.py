# main.py

import numpy as np
from db_utils import create_connection, get_all_tables, get_table_info

def get_min_max_for_column(conn, table_name, column_name, col_type=None):
    """
    Return the minimum and maximum value for a column in a given table,
    using numpy's nanmin/nanmax to skip NaN values.
    If the column type is text, cast its values to integer before calculating.
    """
    cursor = conn.cursor()
    try:
        if col_type is not None and "text" in col_type.lower():
            query = f"SELECT CAST({column_name} AS INTEGER) FROM {table_name} WHERE {column_name} IS NOT NULL;"
        else:
            query = f"SELECT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL;"

        cursor.execute(query)
        values = cursor.fetchall()

        # Flatten the list and convert to numpy array
        flat_values = [v[0] for v in values if v[0] is not None]
        np_array = np.array(flat_values, dtype=float)

        if np_array.size == 0:
            return (None, None)

        return (np.nanmin(np_array), np.nanmax(np_array))
    except Exception as e:
        print(f"Error processing {table_name}.{column_name}: {e}")
        return (None, None)

def main():
    database_path = '/home/seva/Downloads/basketball/nba.sqlite'
    
    # Create a database connection using db_utils
    conn = create_connection(database_path)
    
    if not conn:
        print("Error: Could not create the database connection")
        return

    # Get and list all tables
    tables = get_all_tables(conn)
    print(f"\nFound {len(tables)} tables in the database:")
    for table in tables:
        print(f"- {table}")

    # Analyze 'id' columns for each table
    print("\n--- COLUMNS WITH 'ID' IN THE NAME MIN/MAX ---")
    for table in tables:
        columns = get_table_info(conn, table)
        id_columns = [col for col in columns if "id" in col[1].lower()]
        
        if not id_columns:
            continue

        print(f"\nTABLE: {table}")
        for col in id_columns:
            cid, col_name, type_, notnull, dflt_value, pk = col
            min_val, max_val = get_min_max_for_column(conn, table, col_name, type_)
            print(f"Column '{col_name}' (Type: {type_}): Min = {min_val}, Max = {max_val}")

    conn.close()
    print("\nDatabase connection closed")

if __name__ == "__main__":
    main()
