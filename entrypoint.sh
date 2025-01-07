#!/bin/bash

# Initialize the Airflow database
# airflow db init

# Start the Airflow scheduler in the background
airflow scheduler &

# Start the Airflow webserver
exec airflow webserver --port 8080
