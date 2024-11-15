#!/bin/bash

set -e


python3 -m pip install -r /opt/airflow/requirements/docker_airflowscheduler_req.txt

exec airflow scheduler