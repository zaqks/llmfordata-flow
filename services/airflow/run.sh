#!/bin/bash
# Generate password file in background (no wait needed)
(while [ ! -f /app/airflow/simple_auth_manager_passwords.json.generated ]; do sleep 1; done; echo '{"admin":"admin"}' > /app/airflow/simple_auth_manager_passwords.json.generated) &

# run airflow immediately
airflow standalone
