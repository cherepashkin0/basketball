import pandas as pd
from csv_upload import get_clickhouse_client

# connect to ClickHouse database
client = get_clickhouse_client('DATABASE_UNITED')




def load_all_clickhouse_tables_as_dataframe(
    add_source_column: bool = True,
    limit: int | None = None
) -> pd.DataFrame:
    """
    Load all tables from a ClickHouse database into a single Pandas DataFrame.

    Parameters:
    - add_source_column (bool): If True, adds a __source_table column to track origin.
    - limit (int | None): Limit rows per table (for testing or speed).

    Returns:
    - pd.DataFrame: Combined DataFrame containing rows from all tables.
    """
    client = get_clickhouse_client('DATABASE_UNITED')
    
    # Get all table names
    query_tables = f"SELECT name FROM system.tables WHERE database = 'basketball_data'"
    print(client.query_df(query_tables))
    table_names = client.query_df(query_tables)['name'].tolist()

    dfs = []
    for table in table_names:
        query = f"SELECT * FROM basketball_data.{table}"
        if limit:
            query += f" LIMIT {limit}"
        try:
            df = client.query_df(query)
            if add_source_column:
                df['__source_table'] = table
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Skipped table '{table}' due to error: {e}")

    if not dfs:
        print("❌ No tables were successfully loaded.")
        return pd.DataFrame()

    combined_df = pd.concat(dfs, ignore_index=True, sort=False)
    return combined_df


if __name__ == "__main__":
    # Load all tables from the specified database
    df = load_all_clickhouse_tables_as_dataframe(limit=100)
    
    print("📊 Full DataFrame:")
    print(df.head())
    print("All columns:", df.columns.tolist())

    # Filter numeric columns first
    numeric_df = df.select_dtypes(include='number')

    # Then exclude 'id' columns
    numeric_non_id_columns = [col for col in numeric_df.columns if 'id' not in col.lower()]
    numeric_df_no_id = numeric_df[numeric_non_id_columns]

    print("\n🔢🚫 Numeric columns without 'id':")
    print(numeric_df_no_id.head())
    print("Numeric columns without 'id':", numeric_non_id_columns)