# Data Pipelines with Airflow

Due to the steady increase of more and complexer data warehouse ETL jobs Sparkify decided to start using Airflow. This
tool allows them to automate and monitor their complex ETL jobs via a user friendly UI, and allows their Data Engineers
to write the pipelines in Python.

## The challenge

For this project 2 S3 buckets with .json files need to be staged in Redshift. These 2 staging tables are the building
blocks for 4 dimension and 1 fact table.

To handle this amount of complexity 2 DAGS were written:

- s3_to_redshift_create_tables.py
- s3_to_redshift_insert_tables.py

## s3_to_redshift_create_tables.py

Why 
- Graph

## s3_to_redshift_insert_tables.py

Why -> explain dq log statement, are the number of files and insert records what you expected?
- Graph

## Data Quality

Why -> explain the sensor -> explain how the SQLCheckOperator works
- Graph

## Airflow on your local machine

- WSL
- Postgres

## Contact

In case of suggestions or remarks please contact the Data Engineering department.
