from credentials import snowflake_credentials, postgresql_credentials 

def sf_to_postgresql():

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

    def load_table_from_snowflake(snowflake_options: dict, query: str):
        # Load data from Snowflake using a query
        df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("query", query) \
            .load()
        return df

    def load_from_snowflake_to_postgresql(snowflake_options: dict, pg_url: str, pg_properties: dict, query: str, target_table: str):
        df = load_table_from_snowflake(snowflake_options, query)
        
        # Write data to PostgreSQL
        if df:
            df.write \
                .jdbc(url=pg_url, table=target_table, mode="overwrite", properties=pg_properties)
            print(f"Data written to PostgreSQL table '{target_table}'")
        else:
            print("No data to write to PostgreSQL.")

    # Example usage
    query1 = '''SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
    FROM tb_101.raw_pos.order_detail od
    JOIN tb_101.raw_pos.order_header oh
    ON od.order_id = oh.order_id
    JOIN tb_101.raw_pos.truck t
    ON oh.truck_id = t.truck_id
    JOIN tb_101.raw_pos.menu m
    ON od.menu_item_id = m.menu_item_id
    JOIN tb_101.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
    JOIN tb_101.raw_pos.location l
    ON oh.location_id = l.location_id
    LEFT JOIN tb_101.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id  limit 100000;'''  # Replace with your actual query
    target_table1 = "orders"  # Replace with your actual target table name
    load_from_snowflake_to_postgresql(snowflake_options, postgresql_credentials['url'], postgresql_credentials['properties'], query1, target_table1)

    query2 = '''SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
    FROM tb_101.raw_customer.customer_loyalty cl
    JOIN tb_101.raw_pos.order_header oh
    ON cl.customer_id = oh.customer_id
    GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
    cl.last_name, cl.phone_number, cl.e_mail;'''  # Replace with your actual query
    target_table2 = "customer_loyalty_metrics"  # Replace with your actual target table name
    load_from_snowflake_to_postgresql(snowflake_options, postgresql_credentials['url'], postgresql_credentials['properties'], query2, target_table2)

    print('Data loaded from Snowflake and written to PostgreSQL successfully.')

# sf_to_postgresql()