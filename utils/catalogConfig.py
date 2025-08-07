# utils/catalogConfig.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CatalogConfig").getOrCreate()

def create_database(catalog: str, database: str) -> None:
    """
    Create a database in the specified catalog and print a custom message.
    If the database exists and contains tables, it will not be recreated to avoid data loss.

    Args:
        catalog (str): The catalog name.
        database (str): The database name.
    """
    try:
        # Check if the database exists
        databases = spark.sql("SHOW DATABASES").collect()
        database_exists = any(db.databaseName == database for db in databases)

        if database_exists:
            # Check if the database contains tables
            tables = spark.sql(f"SHOW TABLES IN {catalog}.{database}").collect()
            if tables:
                print(f"Database {catalog}.{database} already exists and contains tables. No action taken to avoid data loss.")
                return
            else:
                print(f"Database {catalog}.{database} already exists but is empty. No action taken.")
                return

        # Print the action being taken
        print(f"Creating database {catalog}.{database}...")

        # Create the database
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

        print(f"Database {catalog}.{database} created successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")



def delete_database(catalog: str, database: str) -> None:
    """
    Delete a database in the specified catalog and print a custom message.

    Args:
        catalog (str): The catalog name.
        database (str): The database name.
    """
    try:
        # Print the action being taken
        print(f"Deleting database {catalog}.{database}...")

        # Drop the database
        spark.sql(f"DROP DATABASE IF EXISTS {catalog}.{database} CASCADE")

        # Check if the database was deleted
        databases = spark.sql("SHOW DATABASES").collect()
        database_exists = any(db.databaseName == database for db in databases)

        if not database_exists:
            print(f"Database {catalog}.{database} deleted successfully.")
        else:
            print(f"Failed to delete database {catalog}.{database}.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")



def inspect_catalog(catalog: str, database: str = None, table: str = None, show_columns: bool = True) -> None:
    """
    Inspect the catalog, database, or table and print the details.

    Args:
        catalog (str): The catalog name.
        database (str, optional): The database name. Defaults to None.
        table (str, optional): The table name. Defaults to None.
        show_columns (bool, optional): Whether to show columns under each table. Defaults to True.
    """
    try:
        if table:
            # Inspect a specific table
            print(f"Inspecting table {catalog}.{database}.{table}...")
            try:
                table_info = spark.sql(f"DESCRIBE TABLE {catalog}.{database}.{table}").collect()
                if table_info:
                    print(f"  - {table}")
                    for idx, row in enumerate(table_info, start=1):
                        print(f"    - Column {idx}: '{row.col_name}', data_type='{row.data_type}', comment={row.comment}")
                else:
                    print(f"Table {catalog}.{database}.{table} doesn't exist or has no columns.")
            except Exception:
                print(f"Table {catalog}.{database}.{table} doesn't exist.")
        elif database:
            # Inspect a specific database
            print(f"Inspecting database {catalog}.{database}...\n")
            try:
                tables = spark.sql(f"SHOW TABLES IN {catalog}.{database}").collect()
                table_count = len(tables)
                print(f"{table_count} tables in {catalog}.{database}\n")
                if tables:
                    print(f"Tables in {catalog}.{database}:")
                    for idx, table in enumerate(tables, start=1):
                        print(f"  {idx}. {table.tableName}")
                        if show_columns:
                            table_info = spark.sql(f"DESCRIBE TABLE {catalog}.{database}.{table.tableName}").collect()
                            if table_info:
                                for col_idx, row in enumerate(table_info, start=1):
                                    print(f"    - Column {col_idx}: '{row.col_name}', data_type='{row.data_type}', comment={row.comment}")
                            else:
                                print(f"    - Table {catalog}.{database}.{table.tableName} has no columns.")
                elif not tables:
                    print(f"The catalog {catalog} and database {database} exist, however the database doesn't contain any tables.")
            except Exception:
                print(f"Database {catalog}.{database} doesn't exist.")
        else:
            # Inspect the entire catalog
            print(f"Inspecting catalog {catalog}...\n")
            try:
                databases = spark.sql(f"SHOW DATABASES IN {catalog}").collect()
                database_count = len(databases)
                print(f"{database_count} databases in {catalog}\n")
                if databases:
                    print(f"Databases in {catalog}:")
                    for db in databases:
                        print(f"  - {db.databaseName}")
                        tables = spark.sql(f"SHOW TABLES IN {catalog}.{db.databaseName}").collect()
                        table_count = len(tables)
                        print(f"    {table_count} tables in {catalog}.{db.databaseName}")
                        if tables:
                            print(f"    Tables in {catalog}.{db.databaseName}:")
                            for idx, table in enumerate(tables, start=1):
                                print(f"      {idx}. {table.tableName}")
                                if show_columns:
                                    table_info = spark.sql(f"DESCRIBE TABLE {catalog}.{db.databaseName}.{table.tableName}").collect()
                                    if table_info:
                                        for col_idx, row in enumerate(table_info, start=1):
                                            print(f"        - Column {col_idx}: '{row.col_name}', data_type='{row.data_type}', comment={row.comment}")
                                    else:
                                        print(f"        - Table {catalog}.{db.databaseName}.{table.tableName} has no columns.")
                        else:
                            print(f"    The catalog {catalog} and database {db.databaseName} exist, however the database doesn't contain any tables.")
                elif not databases:
                    print(f"No databases found in {catalog}.")
            except Exception:
                print(f"Catalog {catalog} doesn't exist.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def coalesce_multiple_partitions_to_single_csv(source_volume: str, target_volume: str = None) -> None:
    """
    Merges multiple CSV part files from the source directory into a single CSV file.
    
    Args:
        source_volume: The directory containing the CSV part files.
        target_volume: The destination directory for the merged CSV. If not specified, 
                       the merged file will be saved in the source directory with a 
                       name prefixed by 'COMBINED_'.
    """
    # Check if the source directory exists
    if not os.path.exists(source_volume):
        print(f"[ERROR] Source directory {source_volume} does not exist.")
        return
    
    # List all CSV part files
    part_files = [f for f in os.listdir(source_volume) if f.startswith("part-") and f.endswith(".csv")]
    
    # Check if there are multiple part files
    if len(part_files) <= 1:
        print("[INFO] Source file is already a single CSV - exiting script as nothing needs to be completed.")
        return
    
    print(f"[INFO] File found at source with {len(part_files)} part files.")
    
    # Determine the destination file path
    if target_volume is None:
        target_volume = source_volume
        source_dir_name = os.path.basename(source_volume.rstrip('/'))
        dest_file = os.path.join(target_volume, f"COMBINED_{source_dir_name}.csv")
    else:
        dest_file = os.path.join(target_volume, f"COMBINED_{os.path.basename(source_volume.rstrip('/'))}.csv")
    
    print(f"[INFO] Starting to merge CSV files from {source_volume} into {dest_file}")
    
    # Open the destination file in write mode
    with open(dest_file, 'w') as outfile:
        for i, part_file in enumerate(part_files):
            part_file_path = os.path.join(source_volume, part_file)
            with open(part_file_path, 'r') as infile:
                # Write the header only for the first file
                if i == 0:
                    outfile.write(infile.read())
                    print(f"[INFO] Writing header and data from {part_file}")
                else:
                    # Skip the header for subsequent files
                    infile.readline()
                    outfile.write(infile.read())
                    print(f"[INFO] Writing data from {part_file}")
    
    print(f"[SUCCESS] Merged CSV saved to {dest_file}")
    print(f"[INFO] Completed merging {len(part_files)} part files into {dest_file}")