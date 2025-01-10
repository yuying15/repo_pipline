import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
from tqdm import tqdm  # For progress bar

# Load environment variables from .env file
load_dotenv()

# Initialize Spark session with Hadoop AWS dependency
spark = SparkSession.builder \
    .appName("Load S3 Data and Perform Joins") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# Initialize boto3 S3 client
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

# S3 bucket and base path
bucket_name = "sfquickstarts"
base_path = f"s3a://{bucket_name}/frostbyte_tastybytes/"

schemas = {
    "truck": StructType([
        StructField("truck_id", IntegerType(), True),
        StructField("menu_type_id", IntegerType(), True),
        StructField("primary_city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("iso_region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("iso_country_code", StringType(), True),
        StructField("franchise_flag", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("ev_flag", IntegerType(), True),
        StructField("franchise_id", IntegerType(), True),
        StructField("truck_opening_date", DateType(), True)
    ]),
    "order_detail": StructType([
        StructField("order_detail_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("menu_item_id", IntegerType(), True),
        StructField("discount_id", StringType(), True),
        StructField("line_number", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_item_discount_amount", StringType(), True)
    ]),
    "order_header": StructType([
        StructField("order_id", IntegerType(), True),
        StructField("truck_id", IntegerType(), True),
        StructField("location_id", FloatType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("discount_id", StringType(), True),
        StructField("shift_id", IntegerType(), True),
        StructField("shift_start_time", StringType(), True),
        StructField("shift_end_time", StringType(), True),
        StructField("order_channel", StringType(), True),
        StructField("order_ts", TimestampType(), True),
        StructField("served_ts", StringType(), True),
        StructField("order_currency", StringType(), True),
        StructField("order_amount", DoubleType(), True),
        StructField("order_tax_amount", StringType(), True),
        StructField("order_discount_amount", StringType(), True),
        StructField("order_total", DoubleType(), True)
    ]),
    "menu": StructType([
        StructField("menu_id", IntegerType(), True),
        StructField("menu_type_id", IntegerType(), True),
        StructField("menu_type", StringType(), True),
        StructField("truck_brand_name", StringType(), True),
        StructField("menu_item_id", IntegerType(), True),
        StructField("menu_item_name", StringType(), True),
        StructField("item_category", StringType(), True),
        StructField("item_subcategory", StringType(), True),
        StructField("cost_of_goods_usd", DoubleType(), True),
        StructField("sale_price_usd", DoubleType(), True),
        StructField("menu_item_health_metrics_obj", StringType(), True)
    ]),
    "franchise": StructType([
        StructField("franchise_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("e_mail", StringType(), True),
        StructField("phone_number", StringType(), True)
    ]),
    "location": StructType([
        StructField("location_id", IntegerType(), True),
        StructField("placekey", StringType(), True),
        StructField("location", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("iso_country_code", StringType(), True),
        StructField("country", StringType(), True)
    ]),
    "customer_loyalty": StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("preferred_language", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("favourite_brand", StringType(), True),
        StructField("marital_status", StringType(), True),
        StructField("children_count", IntegerType(), True),
        StructField("sign_up_date", DateType(), True),
        StructField("birthday_date", DateType(), True),
        StructField("e_mail", StringType(), True),
        StructField("phone_number", StringType(), True)
    ])
}

# Define the required tables and their corresponding folders
required_tables = {
    "order_detail": "raw_pos/order_detail/",
    "order_header": "raw_pos/order_header/",
    "truck": "raw_pos/truck/",
    "menu": "raw_pos/menu/",
    "franchise": "raw_pos/franchise/",
    "location": "raw_pos/location/",
    "customer_loyalty": "raw_customer/customer_loyalty/"
}

# Log full paths for each table
for table_name, folder in required_tables.items():
    full_path = f"{base_path}{folder}"
    print(f"{table_name} full path: {full_path}")

# Load specific files for order_detail and order_header
order_detail_path = "s3a://sfquickstarts/frostbyte_tastybytes/raw_pos/order_detail/order_detail_1_1_2.csv.gz"
order_header_path = "s3a://sfquickstarts/frostbyte_tastybytes/raw_pos/order_header/order_header_1_1_2.csv.gz"

dataframes = {}

def read_file(spark, path, schema=None):
    if schema:
        return spark.read.option("header", "true").schema(schema).csv(path)
    else:
        return spark.read.option("header", "true").csv(path)

print(f"Loading specific file for order_detail: {order_detail_path}")
try:
    dataframes["order_detail"] = read_file(spark, order_detail_path, schema=schemas.get("order_detail"))
except Exception as e:
    print(f"Error loading order_detail: {e}")

print(f"Loading specific file for order_header: {order_header_path}")
try:
    dataframes["order_header"] = read_file(spark, order_header_path, schema=schemas.get("order_header"))
except Exception as e:
    print(f"Error loading order_header: {e}")

# Load data for other tables
for table_name, folder in tqdm(required_tables.items(), desc="Loading tables"):
    if table_name not in ["order_detail", "order_header"]:
        s3_path = f"{base_path}{folder}"
        print(f"\nLoading data for table: {table_name} from path: {s3_path}")

        schema = schemas.get(table_name)

        try:
            dataframes[table_name] = read_file(spark, s3_path, schema=schema)
            print(f"Loaded data for: {table_name}")
        except Exception as e:
            print(f"Error loading data for {table_name}: {e}")
            dataframes[table_name] = None  # Mark as missing

# Filter out missing tables
available_tables = {k: v for k, v in dataframes.items() if v is not None}

# Perform the required joins if all required tables are available
required_for_join = ["order_detail", "order_header", "truck", "menu", "franchise", "location"]
if all(key in available_tables for key in required_for_join):
    try:
        # Rename columns to avoid conflicts
        order_detail_df = available_tables["order_detail"].withColumnRenamed("discount_id", "order_detail_discount_id")
        order_header_df = available_tables["order_header"].withColumnRenamed("discount_id", "order_header_discount_id")
        truck_df = available_tables["truck"] \
            .withColumnRenamed("menu_type_id", "truck_menu_type_id") \
            .withColumnRenamed("country", "truck_country") \
            .withColumnRenamed("city", "truck_city") \
            .withColumnRenamed("region", "truck_region") \
            .withColumnRenamed("iso_country_code", "truck_iso_country_code")
        menu_df = available_tables["menu"].withColumnRenamed("menu_type_id", "menu_menu_type_id")
        franchise_df = available_tables["franchise"] \
            .withColumnRenamed("country", "franchise_country") \
            .withColumnRenamed("city", "franchise_city") \
            .withColumnRenamed("region", "franchise_region") \
            .withColumnRenamed("first_name", "franchise_first_name") \
            .withColumnRenamed("last_name", "franchise_last_name") \
            .withColumnRenamed("e_mail", "franchise_email") \
            .withColumnRenamed("phone_number", "franchise_phone_number")
        location_df = available_tables["location"] \
            .withColumnRenamed("country", "location_country") \
            .withColumnRenamed("city", "location_city") \
            .withColumnRenamed("region", "location_region") \
            .withColumnRenamed("iso_country_code", "location_iso_country_code")
        customer_loyalty_df = available_tables["customer_loyalty"] \
            .withColumnRenamed("country", "customer_country") \
            .withColumnRenamed("city", "customer_city") \
            .withColumnRenamed("region", "customer_region") \
            .withColumnRenamed("first_name", "customer_first_name") \
            .withColumnRenamed("last_name", "customer_last_name") \
            .withColumnRenamed("e_mail", "customer_email") \
            .withColumnRenamed("phone_number", "customer_phone_number")

        # Perform the join
        joined_df = order_detail_df.join(order_header_df, "order_id", "inner") \
            .join(truck_df, "truck_id", "inner") \
            .join(menu_df, "menu_item_id", "inner") \
            .join(franchise_df, "franchise_id", "inner") \
            .join(location_df, "location_id", "inner") \
            .join(customer_loyalty_df, "customer_id", "left")

        # Save the result to Snowflake
        def save_to_snowflake(df, sf_options, table_name):
            df.write \
                .format("snowflake") \
                .options(**sf_options) \
                .option("dbtable", table_name) \
                .mode("overwrite") \
                .save()
            print(f"Data saved to Snowflake table: {table_name}")

        # Build Snowflake URL dynamically using account name
        snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")  # e.g., "abc123"
        snowflake_url = f"{snowflake_account}.snowflakecomputing.com"

        # Snowflake connection options (from .env file)
        sf_options = {
            "sfURL": snowflake_url,  # Dynamically built URL
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
            "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE")
        }

        # Save the joined DataFrame to Snowflake
        save_to_snowflake(joined_df, sf_options, "orders_v")
    except Exception as e:
        print(f"Error during join or save operation: {e}")
else:
    print("Required tables for join are missing in the S3 bucket.")

# Stop Spark session
spark.stop()