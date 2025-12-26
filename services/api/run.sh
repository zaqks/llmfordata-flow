#!/bin/bash

# run airflow
airflow standalone &

# run dj
uvicorn _main_app.asgi:application --host 0.0.0.0 --port 8000 --reload --app-dir ./dj