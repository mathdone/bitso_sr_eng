#!/bin/bash
set -e

# Initialize the Airflow database
airflow db migrate

# Create an admin user if it doesn't exist
if ! airflow users list | grep -q "airflow"; then
    airflow users create \
        --username "$_AIRFLOW_WWW_USER_USERNAME" \
        --password "$_AIRFLOW_WWW_USER_PASSWORD" \
        --firstname "Airflow" \
        --lastname "Admin" \
        --role "Admin" \
        --email "admin@example.com"
fi

# Start the Airflow webserver
exec airflow webserver
