import os
from csv_upload import get_clickhouse_client

def get_all_column_names():
    client = get_clickhouse_client("DATABASE_UNITED")
    db_name = os.getenv("DATABASE_UNITED")

    # Query system.columns to get (table, column) pairs
    q = f"""
    SELECT
        table,
        name
    FROM system.columns
    WHERE database = '{db_name}'
    ORDER BY table, position
    """
    res = client.query(q)

    # res.result_set is a list of (table, column) tuples
    columns_by_table = {}
    for table, column in res.result_set:
        columns_by_table.setdefault(table, []).append(column)

    return columns_by_table

if __name__ == "__main__":
    cols = get_all_column_names()
    for table, cols_list in cols.items():
        print(f"Table `{table}` has {len(cols_list)} columns:")
        for c in cols_list:
            print(f"  - {c}")