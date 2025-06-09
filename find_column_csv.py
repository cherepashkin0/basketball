import pandas as pd
import glob

# Get the list of CSV files.
csv_files = glob.glob("/home/wsievolod/projects/datasets/basketball/csv/*.csv")

files_with_year_columns = {}

for file in csv_files:
    try:
        # Read only the header (nrows=0) to speed up the process.
        df = pd.read_csv(file, nrows=0)
        # Get list of columns that contain "year" (case insensitive).
        matching_columns = [col for col in df.columns if "draft" in col.lower()]
        if matching_columns:
            files_with_year_columns[file] = matching_columns
    except Exception as e:
        print(f"Error reading {file}: {e}")

print("Files with a column containing 'draft':")
for file, columns in files_with_year_columns.items():
    print(f"{file}: {columns}")
