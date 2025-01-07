# Import necessary libraries
import snowflake.connector

# Define Snowflake connection parameters
account = 'nohobhx-hl27063'
user = 'bdavic'
password = 'Hznmb2JKdpXw88x'
# warehouse = 'your_warehouse'  # Replace with your actual warehouse
database = 'BDA_VIC'
schema = 'public'
role = 'ACCOUNTADMIN'


# Define Snowflake options
snowflake_options = {
    "sfURL": f"{account}.snowflakecomputing.com",
    "sfUser": user,
    "sfPassword": password,
    "sfDatabase": database,
    "sfSchema": schema,
    # "sfWarehouse": warehouse,
    "sfRole": role
}


# Execute the SQL query using Snowflake connector
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    # warehouse=warehouse,
    database=database,
    schema=schema,
    role=role
)
# Define multiple SQL statements
sql_statements = """
---> set the Role
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;

---> create the Tasty Bytes Database
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

---> create the Raw POS (Point-of-Sale) Schema
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;

---> create the Raw Menu Table
CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.country

(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
);

---> confirm the empty Menu table exists
SELECT * FROM tasty_bytes_sample_data.raw_pos.country;

---> create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

---> copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.country
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/country/;

"""
# Execute the SQL statements
cursors = conn.execute_string(sql_statements)

# Process the results of the SELECT statement
for cursor in cursors:
    if cursor.rowcount == -1:  # Indicates a SELECT statement
        for row in cursor:
            print(row)

# Close the connection
conn.close()
