import os
import pandas as pd
from sqlalchemy import create_engine

# Load credentials from environment variables
DB_NAME = 'basketball_game'
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")
DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = os.getenv("PG_PORT", "5432")

# Build the SQLAlchemy database URL
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create SQLAlchemy engine
engine = create_engine(db_url)

# Query for schema info
query = """
SELECT 
  table_schema,
  table_name, 
  column_name, 
  data_type, 
  is_nullable
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name, ordinal_position;
"""

# Load data into DataFrame
df = pd.read_sql_query(query, engine)

# Print the result
print(df)

# Optional: Save to file
df.to_csv("postgres_schema.csv", index=False)