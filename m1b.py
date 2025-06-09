def migrate_bigquery_to_postgresql(db_name, chunk_size=50_000):
    drop_postgres_database_if_exists(db_name)
    ensure_postgres_database_exists(db_name)

    bq_project = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    pg_engine = get_pg_engine(db_name)
    bq_client = bigquery.Client(credentials=credentials, project=bq_project)
    bq_storage_client = BigQueryReadClient(credentials=credentials)

    dataset_ref = bigquery.DatasetReference(project=bq_project, dataset_id=db_name)
    tables = list(bq_client.list_tables(dataset_ref))

    for table in tables:
        table_name = table.table_id
        full_table_id = f"{bq_project}.{db_name}.{table_name}"
        total_rows = 0

        try:
            print(f"📥 Downloading {table_name} from BigQuery...")

            # Lade gesamten DataFrame
            df = bq_client.list_rows(full_table_id).to_dataframe(bqstorage_client=bq_storage_client)

            if df.empty:
                print(f"⚠️ Skipping empty table: {table_name}")
                continue

            # Teile in Chunks auf
            chunks = np.array_split(df, int(len(df) / chunk_size) + 1)

            first_chunk = True
            for chunk in chunks:
                if chunk.empty:
                    continue

                chunk = chunk.where(pd.notnull(chunk), None)
                if_exists = "replace" if first_chunk else "append"
                chunk.head(0).to_sql(table_name, pg_engine, index=False, if_exists=if_exists)
                copy_from_stringio(chunk, table_name, pg_engine)
                total_rows += len(chunk)
                first_chunk = False

            print(f"✅ Inserted {total_rows} rows into PostgreSQL table: {table_name}")

        except Exception as e:
            print(f"❌ Failed to migrate {table_name}: {e}")