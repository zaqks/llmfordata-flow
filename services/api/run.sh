#!/bin/bash

# run airflow
airflow standalone &

# change pwd
while [ ! -f /app/airflow/simple_auth_manager_passwords.json.generated ]; do sleep 1; done; echo '{"admin":"admin"}' > /app/airflow/simple_auth_manager_passwords.json.generated


# run dj
uvicorn _main_app.asgi:application --host 0.0.0.0 --port 8000 --reload --app-dir ./dj