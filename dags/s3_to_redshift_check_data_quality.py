# This DAG checks the data quality of the fact and dimension tables

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import SQLCheckOperator

from sql import quality_checks

SQLCheckOperator(task_id='data quality check', sql=self.data_quality_sql)
