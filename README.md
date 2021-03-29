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

<img src="https://user-images.githubusercontent.com/49920622/112884422-c86abb00-90cf-11eb-9266-cc613190eb2d.JPG">

## s3_to_redshift_create_tables.py

This DAG is solely responsible for creating the fact and dimension tables:

- public.songplays (fact)
- public.users
- public.artists
- public.songs
- public.time

The reason to put these statements in a separate DAG is because this DAG should only be run once. For additional
robustness any `DROP TABLE` statement is excluded from the SQL queries.  

<img src="https://user-images.githubusercontent.com/49920622/112884039-4ed2cd00-90cf-11eb-8deb-18ff06a16845.JPG">

## s3_to_redshift_insert_tables.py

This DAG is really the heart of this data warehouse ETL pipeline. It contains a lot of steps, some of them based on
custom operators:

- Create the staging tables on Redshift and COPY the .json files into these tables
- Insert the data into the songplays fact table and perform a data quality check
- Insert the data into the dimension tables and perform a data quality check 

<img src="https://user-images.githubusercontent.com/49920622/112883924-24810f80-90cf-11eb-9b32-9c797ce390c8.JPG">

## Data Quality

Staging: check the logs
Fact & Dimension: SQLCheckOperator

## Instructions

- Make sure you have Airflow up and running -> see Airflow on your local machine as an example
- Clone this project into the Airflow folder
- Make sure the DAGS and PLUGINS folder are set correctly in airflow config
- Activate both dags and trigger them manually

## Airflow on your local machine

- WSL
- Postgres

## Contact

In case of suggestions or remarks please contact the Data Engineering department.
