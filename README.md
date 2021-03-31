# Data Pipelines with Airflow

Due to the steady increase of more and complexer data warehouse ETL jobs, Sparkify decided to start using Airflow. This
tool allows them to automate and monitor their complex ETL jobs via a user friendly UI, and allows their Data Engineers
to write the pipelines in Python.

## The challenge

For this project 2 S3 buckets with .json files need to be staged in Redshift. These 2 staging tables are the building
blocks for 4 dimension and 1 fact table.

To handle this data warehouse ETL pipeline 2 DAGS were written:

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

This DAG contains a lot of steps, some of them based on custom operators:

- Create the staging tables on Redshift, COPY the .json files into these tables, and log the data quality
- Insert the data into the songplays fact table and execute a data quality check
- Insert the data into the dimension tables and execute a data quality check 

<img src="https://user-images.githubusercontent.com/49920622/112883924-24810f80-90cf-11eb-9b32-9c797ce390c8.JPG">

## Data Quality

There are 2 data quality checks build into the s3_to_redshift_insert_tables.py:

- a data quality log statement when staging the tables to RedShift
- an individual data quality check per table which needs to be passed in order for the DAG to complete

<img src="https://user-images.githubusercontent.com/49920622/113088590-36090b00-91e6-11eb-9191-ca003afbe21b.JPG">

The individual data quality checks are handled by the official Airflow SQLCheckOperator, and fed with tailored SQL
queries per table. More information on this operator can be found [here][sql_check_operator]. 

```sql
SELECT count(*) as n_rows
,    count(distinct songplay_id) = count(*) as n_unique_id_check
,    min(extract(year from start_time)) = 2018 & max(extract(year from start_time)) = 2018 as year_check
,    min(extract(month from start_time)) = 11 & max(extract(month from start_time)) = 11 as month_check

FROM
    public.songplays
;
```

<img src="https://user-images.githubusercontent.com/49920622/113088822-afa0f900-91e6-11eb-8d69-7feb89a9098c.JPG">

## Instructions

There are quite a few things you need to take care of before you can run this project:

- Make sure Airflow is up and running
- Copy / clone the `dags` and `plugins` folder into your Airflow folder
- Make sure your `dags` and `plugins` folder are correctly referred to in `airflow.cfg`
- Add valid AWS credentials and AWS Redshift cluster information to the `Admin > Connections` tab in the Airflow UI

Once you are ready, you should see the 2 DAGS in the UI. Unpause both DAGS and manually trigger `s3_to_redshift_create_tables.py` first.
When finished, manually trigger `s3_to_redshift_insert_tables.py`. After the first run the insert DAG should run every
hour.

This project was tested on a local instance of Airflow version 2.0.1 with the use of WSL and PostgreSQL. If you are
interested in replicating this approach please have a look [here][airflow_wsl_1], [here][airflow_local],
and [here][postgresql_wsl].

## Contact

In case of suggestions or remarks please contact the Data Engineering department.

[sql_check_operator]: https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/sql/index.html
[airflow_wsl_1]: https://towardsdatascience.com/run-apache-airflow-on-windows-10-without-docker-3c5754bb98b4
[airflow_local]: https://airflow.apache.org/docs/apache-airflow/stable/start/local.html
[postgresql_wsl]: https://docs.microsoft.com/en-us/windows/wsl/tutorials/wsl-database