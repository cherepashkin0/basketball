# pragma.py

from db_utils import create_connection, get_all_tables, get_table_info

def main():
    database_path = '/home/seva/Downloads/basketball/nba.sqlite'
    conn = create_connection(database_path)
    
    if not conn:
        print("Error: Could not create the database connection")
        return

    tables = get_all_tables(conn)
    print(f"\nFound {len(tables)} tables in the database:")
    for table in tables:
        print(f"- {table}")
    
    print("\n--- COLUMNS AND TYPES FOR EACH TABLE ---")
    for table in tables:
        print(f"\nTABLE: {table}")
        for _, col_name, type_, _, _, _ in get_table_info(conn, table):
            print(f"  Column: '{col_name}', Type: {type_}")
    
    conn.close()
    print("\nDatabase connection closed")

if __name__ == "__main__":
    main()
