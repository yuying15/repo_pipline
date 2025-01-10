
# from dotenv import load_dotenv

# Load environment variables
# load_dotenv()

def sf_to_postgresql():
    import os
    from pyspark.sql import SparkSession
    print("Starting Snowflake to PostgreSQL ETL process...")

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Snowflake to PostgreSQL") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.3,org.postgresql:postgresql:42.2.23") \
        .getOrCreate()

    # Define Snowflake options
    snowflake_options = {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv('SNOWFLAKE_USER'),
        "sfPassword": os.getenv('SNOWFLAKE_PASSWORD'),
        "sfDatabase": os.getenv('SNOWFLAKE_DATABASE'),
        "sfSchema": os.getenv('SNOWFLAKE_SCHEMA'),
        "sfWarehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "sfRole": os.getenv('SNOWFLAKE_ROLE')
    }

    # Define PostgreSQL connection options
    postgresql_options = {
        "url": os.getenv('POSTGRESQL_URL'),  # Ensure this includes the port number
        "user": os.getenv('POSTGRESQL_USER'),
        "password": os.getenv('POSTGRESQL_PASSWORD'),
        "driver": os.getenv('POSTGRESQL_DRIVER'),
        "currentSchema": os.getenv('POSTGRESQL_SCHEMA')
    }

    # Function to load a table from Snowflake
    def load_table_from_snowflake(table_name: str):
        print(f"Loading table {table_name} from Snowflake...")
        df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", table_name) \
            .load()
        return df

    # Function to filter out columns with null values
    def filter_non_null_columns(df):
        print("Filtering out columns with null values...")
        non_null_columns = [col for col in df.columns if df.filter(df[col].isNull()).count() == 0]
        return df.select(non_null_columns)

    # Function to save a DataFrame to PostgreSQL
    def save_to_postgresql(df, table_name: str):
        print(f"Saving DataFrame to PostgreSQL table {table_name}...")
        try:
            df.write \
                .format("jdbc") \
                .options(**postgresql_options) \
                .option("dbtable", table_name) \
                .mode("overwrite") \
                .save()
            print(f"Data successfully saved to PostgreSQL table {table_name}.")
        except Exception as e:
            print(f"Error saving DataFrame to PostgreSQL: {e}")
            raise

    # Load tables from Snowflake
    order_detail_df = load_table_from_snowflake("order_detail")
    order_header_df = load_table_from_snowflake("order_header")
    truck_df = load_table_from_snowflake("truck")
    menu_df = load_table_from_snowflake("menu")
    franchise_df = load_table_from_snowflake("franchise")
    location_df = load_table_from_snowflake("location")
    customer_loyalty_df = load_table_from_snowflake("customer_loyalty")

    # Filter out columns with null values
    order_detail_df = filter_non_null_columns(order_detail_df)
    order_header_df = filter_non_null_columns(order_header_df)
    truck_df = filter_non_null_columns(truck_df)
    menu_df = filter_non_null_columns(menu_df)
    franchise_df = filter_non_null_columns(franchise_df)
    location_df = filter_non_null_columns(location_df)
    customer_loyalty_df = filter_non_null_columns(customer_loyalty_df)

    # Rename conflicting columns
    print("Renaming conflicting columns...")
    order_detail_df = order_detail_df.withColumnRenamed("discount_id", "order_detail_discount_id")
    order_header_df = order_header_df.withColumnRenamed("discount_id", "order_header_discount_id")
    truck_df = truck_df \
        .withColumnRenamed("city", "truck_city") \
        .withColumnRenamed("country", "truck_country") \
        .withColumnRenamed("region", "truck_region") \
        .withColumnRenamed("menu_type_id", "truck_menu_type_id") \
        .withColumnRenamed("iso_country_code", "truck_iso_country_code")
    menu_df = menu_df \
        .withColumnRenamed("menu_type_id", "menu_menu_type_id")
    franchise_df = franchise_df \
        .withColumnRenamed("city", "franchise_city") \
        .withColumnRenamed("country", "franchise_country") \
        .withColumnRenamed("region", "franchise_region") \
        .withColumnRenamed("first_name", "franchise_first_name") \
        .withColumnRenamed("last_name", "franchise_last_name") \
        .withColumnRenamed("e_mail", "franchise_email") \
        .withColumnRenamed("phone_number", "franchise_phone_number")
    location_df = location_df \
        .withColumnRenamed("city", "location_city") \
        .withColumnRenamed("country", "location_country") \
        .withColumnRenamed("region", "location_region") \
        .withColumnRenamed("iso_country_code", "location_iso_country_code")
    customer_loyalty_df = customer_loyalty_df \
        .withColumnRenamed("city", "customer_city") \
        .withColumnRenamed("country", "customer_country") \
        .withColumnRenamed("region", "customer_region") \
        .withColumnRenamed("first_name", "customer_first_name") \
        .withColumnRenamed("last_name", "customer_last_name") \
        .withColumnRenamed("e_mail", "customer_email") \
        .withColumnRenamed("phone_number", "customer_phone_number")

    # Perform the join operation
    print("Performing join operation...")
    joined_df = order_detail_df.join(order_header_df, "order_id", "left") \
        .join(truck_df, "truck_id", "left") \
        .join(menu_df, "menu_item_id", "left") \
        .join(franchise_df, "franchise_id", "left") \
        .join(location_df, "location_id", "left") \
        # .join(customer_loyalty_df, "customer_id", "left")

    # Limit the joined DataFrame to 20,000 rows
    print("Limiting the joined DataFrame to 20,000 rows...")
    limited_df = joined_df.limit(20000)

    # Save the limited DataFrame to PostgreSQL
    save_to_postgresql(limited_df, "joined_data")

    print("Data loaded from Snowflake, joined, and written to PostgreSQL successfully.")

    # Stop the Spark session
    spark.stop()

# Run the function
# sf_to_postgresql()