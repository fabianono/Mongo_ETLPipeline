#!/bin/bash

set -e

if [ ! -e "/opt/airflow/airflow.db" ]; then
    echo "creating users" &&
    airflow db init && \
    airflow users create \
        --username admin \
        --password admin \
        --firstname firstname \
        --lastname lastname \
        --role Admin \
        --email admin@admin.com
fi

$(command -v airflow) db migrate

exec airflow webserver