# Import necessary libraries
from etl.credentials import snowflake_credentials
import snowflake.connector

# Execute the SQL query using Snowflake connector
conn = snowflake.connector.connect(
    user=snowflake_credentials['user'],
    password=snowflake_credentials['password'],
    account=snowflake_credentials['account'],
    database=snowflake_credentials['database'],
    schema=snowflake_credentials['schema'],
    role=snowflake_credentials['role']
)

# Read the SQL file
with open('s3_sf_data.sql', 'r') as file:
    sql_statements = file.read()
print(sql_statements)
# Execute the SQL statements
cursors = conn.execute_string(sql_statements)

# Process the results of the SELECT statement
for cursor in cursors:
    if cursor.rowcount == -1:  # Indicates a SELECT statement
        for row in cursor:
            print(row)
print('done')
# Close the connection
conn.close()
