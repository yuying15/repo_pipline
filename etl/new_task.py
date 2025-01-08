from credentials import snowflake_credentials, postgresql_credentials

def new_task_function():

    print("This is a new task")
    # Import necessary libraries
    from pyspark.sql import SparkSession

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Snowflake to PostgreSQL") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.3,org.postgresql:postgresql:42.2.23") \
        .getOrCreate()

    # Define Snowflake options
    snowflake_options = {
        "sfURL": f"{snowflake_credentials['account']}.snowflakecomputing.com",
        "sfUser": snowflake_credentials['user'],
        "sfPassword": snowflake_credentials['password'],
        "sfDatabase": snowflake_credentials['database'],
        "sfSchema": snowflake_credentials['schema'],
        "sfWarehouse": snowflake_credentials['warehouse'],
        "sfRole": snowflake_credentials['role']
    }

    def load_and_join_tables(snowflake_options: dict):
        # Load data from Snowflake
        sales_df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "Sales_data") \
            .load()
        
        # Load and join Sales_Order_data
        sales_order_df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "Sales_Order_data") \
            .load()
        joined_df = sales_df.join(sales_order_df, on=['SALESORDERLINEKEY'], how='inner')
        
        # Load and join Sales_Territory_data
        sales_territory_df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "Sales_Territory_data") \
            .load()
        joined_df = joined_df.join(sales_territory_df, on=['SALESTERRITORYKEY'], how='inner')
        
        # Load and join Reseller_data
        reseller_df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "Reseller_data") \
            .load()
        joined_df = joined_df.join(reseller_df, on=['RESELLERKEY'], how='inner')
        
        # Load and join Date_data
        date_df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "Date_data") \
            .load()
        joined_df = joined_df.join(date_df, joined_df["ShipDateKey"] == date_df["DateKey"], how='inner')
        
        # Load and join Product_data
        product_df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "Product_data") \
            .load()
        joined_df = joined_df.join(product_df, on=['PRODUCTKEY'], how='inner')
        
        # Load and join Customer_data
        customer_df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "Customer_data") \
            .load()
        joined_df = joined_df.join(customer_df, on=['CUSTOMERKEY'], how='inner')
        
        return joined_df

    def load_from_snowflake_to_postgresql(snowflake_options: dict, pg_url: str, pg_properties: dict):
        joined_df = load_and_join_tables(snowflake_options)
        
        # Ensure there are no empty column names and no duplicate column names
        joined_df = joined_df.toDF(*[col.replace(' ', '_').replace('"', '').replace('-', '_') if col else f"col_{i}" for i, col in enumerate(joined_df.columns)])
        joined_df = joined_df.toDF(*[f"{col}_{i}" if joined_df.columns.count(col) > 1 else col for i, col in enumerate(joined_df.columns)])
        print(joined_df.columns)
        
        # Write joined data to PostgreSQL
        if joined_df:
            joined_df.write \
                .jdbc(url=pg_url, table="AdventureWorks", mode="overwrite", properties=pg_properties)
            print("Joined data written to PostgreSQL")
        else:
            print("No sheets could be joined due to missing common columns.")

    # Example usage
    load_from_snowflake_to_postgresql(snowflake_options, postgresql_credentials['url'], postgresql_credentials['properties'])

    print('Joined data loaded from Snowflake and written to PostgreSQL successfully.')

new_task_function()