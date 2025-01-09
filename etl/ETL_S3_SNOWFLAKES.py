def execute_snowflake_sql():
    # Import necessary libraries
    import snowflake.connector
    import os
    from etl.s3_sf_data import query
    # Load environment variables
    

    # Execute the SQL query using Snowflake connector
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

    # Read the SQL file
    # with open('etl\s3_sf_data.sql', 'r') as file:
    #     sql_statements = file.read()
    sql_statements = query
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
# execute_snowflake_sql()
# Ensure to install the python-dotenv package
# pip install python-dotenv
