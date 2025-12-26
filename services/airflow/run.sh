#!/bin/bash

# run airflow
airflow standalone &

# change pwd
while [ ! -f /app/airflow/simple_auth_manager_passwords.json.generated ]; do sleep 1; done; echo '{"admin":"admin"}' > /app/airflow/simple_auth_manager_passwords.json.generated
